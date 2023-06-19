
//******************************************************************************
// RCF - Remote Call Framework
//
// Copyright (c) 2005 - 2020, Delta V Software. All rights reserved.
// http://www.deltavsoft.com
//
// RCF is distributed under dual licenses - closed source or GPL.
// Consult your particular license for conditions of use.
//
// If you have not purchased a commercial license, you are using RCF 
// under GPL terms.
//
// Version: 3.2
// Contact: support <at> deltavsoft.com 
//
//******************************************************************************

#include <RCF/Marshal.hpp>

#include <algorithm>

#include <functional>

#include <RCF/AmiThreadPool.hpp>
#include <RCF/ClientProgress.hpp>
#include <RCF/Endpoint.hpp>
#include <RCF/Filter.hpp>
#include <RCF/Future.hpp>
#include <RCF/InitDeinit.hpp>
#include <RCF/OverlappedAmi.hpp>
#include <RCF/RcfServer.hpp>
#include <RCF/SerializationProtocol.hpp>
#include <RCF/ServerTransport.hpp>
#include <RCF/ThreadLocalData.hpp>
#include <RCF/Log.hpp>

namespace RCF {

    bool serializeOverride(SerializationProtocolOut &out, ByteBuffer & u)
    {
        int runtimeVersion = out.getRuntimeVersion();

        if (runtimeVersion <= 3)
        {
            // Legacy behavior - no metadata for ByteBuffer.
            int len = static_cast<int>(u.getLength());
            serialize(out, len);
            out.insert(u);
            return true;
        }
        else
        {
            return false;
        }
    }

    bool serializeOverride(SerializationProtocolOut &out, ByteBuffer * pu)
    {
        RCF_ASSERT(pu);
        return serializeOverride(out, *pu);
    }

    bool deserializeOverride(SerializationProtocolIn &in, ByteBuffer & u)
    {
        int runtimeVersion = in.getRuntimeVersion();

        if (runtimeVersion <= 3)
        {
            // Legacy behavior - no metadata for ByteBuffer.
            int len = 0;
            deserialize(in, len);
            in.extractSlice(u, len);
            return true;
        }
        else
        {
            return false;
        }
    }

    Mutex * gpCandidatesMutex = NULL;
    Candidates * gpCandidates = NULL;

    void initAmi()
    {
        gpCandidatesMutex = new Mutex; 
        gpCandidates = new Candidates;
    }

    void deinitAmi()
    {
        delete gpCandidatesMutex; 
        gpCandidatesMutex = NULL; 
        delete gpCandidates; 
        gpCandidates = NULL;
    }


    Mutex & gCandidatesMutex()
    {
        return *gpCandidatesMutex; 
    }

    Candidates & gCandidates()
    {
        return *gpCandidates;
    }

    void ClientStub::enrol(I_Future *pFuture)
    {
        mFutures.push_back(pFuture);
        pFuture->setClientStub(this);
    }

    void ClientStub::init( 
        int fnId, 
        RCF::RemoteCallMode rcs)
    {
        mRequest.init(
            getServerBindingName(),
            fnId,
            getSerializationProtocol(),
            (rcs == RCF::Oneway),
            false,
            getRuntimeVersion(),
            false,
            mPingBackIntervalMs,
            mArchiveVersion,
            mEnableSfPointerTracking,
            mEnableNativeWstringSerialization);

        ::RCF::CurrentClientStubSentry sentry(*this);

        mOut.reset(
            getSerializationProtocol(),
            32,
            mRequest.encodeRequestHeader(),
            mRuntimeVersion,
            mArchiveVersion,
            mEnableSfPointerTracking);

        bool asyncParameters = false;
        mpParameters->write(mOut);
        mFutures.clear();
        asyncParameters = mpParameters->enrolFutures(this);

        if (asyncParameters)
        {
            setAsync(true);
        }
    }

    void ClientStub::connect()
    {
        CurrentClientStubSentry sentry(*this);
        instantiateTransport();

        // Do we need to reconnect?

        bool shouldReconnect = false;

        if (!mConnected)
        {
            // Not connected.
            shouldReconnect = true;
        }
        else if (       mConnected 
                    &&  mAutoReconnect 
                    &&  mRcs == Twoway 
                    &&  !mTransport->isConnected())
        {
            // Auto reconnect.
            shouldReconnect = true;
        }

        if (shouldReconnect)
        {
            std::string endpoint;
            if (mEndpoint.get())
            {
                endpoint = mEndpoint->asString();
            }

            RCF_LOG_2()(this)(endpoint)(mConnectTimeoutMs) 
                << "RcfClient - connect to server.";

            if ( getTransportType() == Tt_Http || getTransportType() == Tt_Https )
            {
                if ( getHttpProxy().size() > 0 )
                {
                    RCF_LOG_2()(this) << "RcfClient - HTTP proxy: " << getHttpProxy() << ":" << getHttpProxyPort();
                }
                else
                {
                    RCF_LOG_2()(this) << "RcfClient - HTTP proxy: <None configured>";
                }
            }

            unsigned int connectTimeoutMs = mConnectTimeoutMs;
            if (connectTimeoutMs == 0)
            {
                connectTimeoutMs = MaxTimeoutMs;
            }
            mTransport->disconnect(connectTimeoutMs);

            mAsyncOpType = Connect;

            mTransport->connect(*this, connectTimeoutMs);
        }
        else
        {
            onConnectCompleted(true);
        }
    }

    void ClientStub::connectAsync(std::function<void()> onCompletion)
    {
        setAsync(true);
        instantiateTransport();
        mTransport->setAsync(mAsync);
        setAsyncCallback(onCompletion);
        connect();

        // In case the connect completes on this thread (as it would for a named pipe).
        getTlsAmiNotification().run();
    }

    void ClientStub::wait(
        std::function<void()> onCompletion,
        std::uint32_t timeoutMs)
    {
        setAsync(true);
        instantiateTransport();
        mTransport->setAsync(mAsync);
        setAsyncCallback(onCompletion);

        mAsyncOpType = Wait;
        mTransport->setTimer(timeoutMs, this);
    }

    void ClientStub::doBatching()
    {
        RCF_ASSERT(mRcs == Oneway);
        RCF_ASSERT(!mAsync);
        RCF_ASSERT(mBatchBufferPtr);

        // Accumulate in the batch buffer.
        std::size_t appendLen = lengthByteBuffers(mEncodedByteBuffers);
        std::size_t currentSize = mBatchBufferPtr->size();

        bool usingTempBuffer = false;

        // If this message will cause us to exceed the limit, then flush first.
        if (    mBatchMaxMessageLength 
            &&  currentSize + appendLen > mBatchMaxMessageLength)
        {
            mBatchBufferTemp.resize(appendLen);
            copyByteBuffers(mEncodedByteBuffers, & mBatchBufferTemp[0] );
            usingTempBuffer = true;

            unsigned int timeoutMs = generateTimeoutMs(mEndTimeMs);
            flushBatch(timeoutMs);
            currentSize = mBatchBufferPtr->size();
        }

        mBatchBufferPtr->resize( currentSize + appendLen);

        if (usingTempBuffer)
        {
            memcpy(
                & (*mBatchBufferPtr)[currentSize], 
                &mBatchBufferTemp[0], 
                mBatchBufferTemp.size());
        }
        else
        {
            copyByteBuffers(
                mEncodedByteBuffers, 
                & (*mBatchBufferPtr)[currentSize] );
        }

        ++mBatchMessageCount;
    }

    void ClientStub::onSetupTransportProtocolCompleted()
    {
        CurrentClientStubSentry sentry(*this);

        if (mAsync && hasAsyncException())
        {
            scheduleAmiNotification();
            getTlsAmiNotification().run();
            return;
        }

        try
        {
            mSetTransportProtocol = false;
            mConnected = true;

            // If we're only connecting, mEncodedByteBuffers will be empty.
            if (!mEncodedByteBuffers.empty())
            {
                beginSend();
            }
            else
            {
                if (mAsync)
                {
                    scheduleAmiNotification();
                }
            }   
        }
        catch(const RCF::Exception & e)
        {
            if (mAsync)
            {
                std::unique_ptr<Exception> ePtr(e.clone());
                setAsyncException(std::move(ePtr));
                scheduleAmiNotification();
                getTlsAmiNotification().run();
            }
            else
            {
                throw;
            }
        }
    }

    void ClientStub::beginSend()
    {       
        CurrentClientStubSentry sentry(*this);

        RCF_ASSERT( !mEncodedByteBuffers.empty() );

        unsigned int timeoutMs = generateTimeoutMs(mEndTimeMs);

        if (mAsync)
        {
            mAsyncOpType = Write;
        }

        // Add framing (4 byte length prefix).
        int messageSize = 
            static_cast<int>(RCF::lengthByteBuffers(mEncodedByteBuffers));

        ByteBuffer &byteBuffer = mEncodedByteBuffers.front();

        RCF_ASSERT(byteBuffer.getLeftMargin() >= 4);
        byteBuffer.expandIntoLeftMargin(4);
        memcpy(byteBuffer.getPtr(), &messageSize, 4);
        RCF::machineToNetworkOrder(byteBuffer.getPtr(), 4, 1);

        if (mBatchMode)
        {
            doBatching();
        }
        else
        {
            int err = getTransport().send(*this, mEncodedByteBuffers, timeoutMs);
            RCF_UNUSED_VARIABLE(err);
        }
    }

    void ClientStub::setupTransportProtocol()
    {
        std::vector<FilterPtr> filters;
        mTransport->getTransportFilters(filters);
        if (
            mTransportProtocol == Tp_Clear
            && !mEnableCompression
            &&  filters.size() > 0 )
        {
            // Custom filter sequence. Setup transport filters again for a new connection.
            std::vector<FilterPtr> filterVec;
            mTransport->getTransportFilters(filterVec);
            for ( std::size_t i = 0; i < filterVec.size(); ++i )
            {
                filterVec[i]->resetState();
            }
            mTransport->setTransportFilters(std::vector<FilterPtr>());
            if ( !filterVec.empty() )
            {
                requestTransportFilters(filterVec);
            }
            onSetupTransportProtocolCompleted();
        }
        else
        {
            if ( !mTransport->supportsTransportFilters() )
            {
                onSetupTransportProtocolCompleted();
            }
            else
            {
                mTransport->setTransportFilters(std::vector<FilterPtr>());
                if ( mTransportProtocol == Tp_Clear && !mEnableCompression )
                {
                    onSetupTransportProtocolCompleted();
                }
                else
                {
                    std::vector<FilterPtr> filterVec;
                    createFilterSequence(filterVec);

                    if ( mAsync )
                    {
                        requestTransportFiltersAsync(
                            filterVec,
                            std::bind(&ClientStub::onSetupTransportProtocolCompleted, this));
                    }
                    else
                    {
                        requestTransportFilters(filterVec);
                        onSetupTransportProtocolCompleted();
                    }
                }
            }
        }
    }

    void ClientStub::onConnectCompleted(bool alreadyConnected)
    {
        CurrentClientStubSentry sentry(*this);

        if (alreadyConnected && !mSetTransportProtocol)
        {
            if (mEncodedByteBuffers.size() > 0)
            {
                beginSend();
            }
        }
        else
        {
            if (mAsync)
            {
                mAsyncOpType = None;
            }

            if ( mEndpoint )
            {
                std::string proxyEndpointName = mEndpoint->getProxyEndpointName();
                if ( !alreadyConnected && proxyEndpointName.size() > 0 )
                {
                    // TODO: this currently runs synchronously, even if user is performing an async call.
                    setupProxiedConnection(proxyEndpointName);
                }
            }

            setupTransportProtocol();
        }
    }

    void ClientStub::beginCall()
    {
        CurrentClientStubSentry sentry(*this);

        unsigned int totalTimeoutMs = getRemoteCallTimeoutMs();
        unsigned int startTimeMs = getCurrentTimeMs();
        if (totalTimeoutMs)
        {
            mEndTimeMs = startTimeMs + totalTimeoutMs;
        }
        else
        {
            // Disable timeout.
            static const unsigned int maxTimeoutMs = (((unsigned int)-1)/10)*9;
            mEndTimeMs = startTimeMs + maxTimeoutMs;
        }

        ThreadLocalCached< std::vector<ByteBuffer> > tlcByteBuffers;
        std::vector<ByteBuffer> &byteBuffers = tlcByteBuffers.get();

        mOut.extractByteBuffers(byteBuffers);
        int protocol = mOut.getSerializationProtocol();
        RCF_UNUSED_VARIABLE(protocol);

        mEncodedByteBuffers.resize(0);

        mRequest.encodeRequest(
            byteBuffers,
            mEncodedByteBuffers,
            getMessageFilters());

        instantiateTransport();

        mTransport->setAsync(mAsync);

        // TODO: make sure timeouts behave as expected, esp. when connect() is 
        // doing round trip filter setup calls
        connect();
    }

    void ClientStub::onSendCompleted()
    {
        if (mRcs == RCF::Oneway)
        {
            mEncodedByteBuffers.resize(0);

            // TODO: Refactor. This code is identical to what happens when a 
            // receive completes, in a twoway call.

            if (mAsync)
            {
                mAsyncOpType = None;
                scheduleAmiNotification();
            }
        }
        else
        {
            mEncodedByteBuffers.resize(0);
            beginReceive();
        }
    }

    void ClientStub::beginReceive()
    {
        if (mPingBackIntervalMs && mRuntimeVersion >= 5)
        {
            mPingBackCheckIntervalMs = 3 * mPingBackIntervalMs;

            mNextPingBackCheckMs = 
                RCF::getCurrentTimeMs() + mPingBackCheckIntervalMs;

            // So we avoid the special value 0.
            mNextPingBackCheckMs |= 1;
        }

        if (mAsync)
        {
            mAsyncOpType = Read;
        }

        unsigned int timeoutMs = generateTimeoutMs(mEndTimeMs);

        mEncodedByteBuffer.clear();
        int err = getTransport().receive(*this, mEncodedByteBuffer, timeoutMs);
        RCF_UNUSED_VARIABLE(err);
    }

    void ClientStub::onException(const Exception & e)
    {
        if (mAsync)
        {
            onError(e);
        }
        else
        {
            e.throwSelf();
        }
    }

    void ClientStub::onReceiveCompleted()
    {
        if (mAsync)
        {
            mAsyncOpType = None;
        }

        ByteBuffer unfilteredByteBuffer;

        MethodInvocationResponse response;

        mRequest.decodeResponse(
            mEncodedByteBuffer,
            unfilteredByteBuffer,
            response,
            getMessageFilters());

        mEncodedByteBuffer.clear();

        mIn.reset(
            unfilteredByteBuffer,
            mOut.getSerializationProtocol(),
            mRuntimeVersion,
            mArchiveVersion,
            response.getEnableSfPointerTracking());

        RCF_LOG_3()(this)(response) << "RcfClient - received response.";

        if (response.isException())
        {
            std::unique_ptr<RemoteException> remoteExceptionUniquePtr(
                response.getExceptionPtr());

            if (!remoteExceptionUniquePtr.get())
            {
                int runtimeVersion = mRequest.mRuntimeVersion;
                if (runtimeVersion < 8)
                {
                    deserialize(mIn, remoteExceptionUniquePtr);
                }
                else
                {
                    RemoteException * pRe = NULL;
                    deserialize(mIn, pRe);
                    remoteExceptionUniquePtr.reset(pRe);
                }
            }

            onException(*remoteExceptionUniquePtr);
        }
        else if (response.isError())
        {
            int err = response.getError();
            if (err == RcfError_VersionMismatch_Id )
            {
                int serverRuntimeVersion = response.getArg0();
                int serverArchiveVersion = response.getArg1();

                int clientRuntimeVersion = getRuntimeVersion();
                int clientArchiveVersion = getArchiveVersion();

                // We get this response from the server, if either the
                // client runtime version or client archive version, is
                // greater than what the server supports.

                if (getAutoVersioning() && getTries() == 0)
                {
                    int adjustedRuntimeVersion = RCF_MIN(serverRuntimeVersion, clientRuntimeVersion);
                    setRuntimeVersion(adjustedRuntimeVersion);

                    
                    if (serverArchiveVersion)
                    {
                        int adjustedArchiveVersion = RCF_MIN(serverArchiveVersion, clientArchiveVersion);
                        setArchiveVersion(adjustedArchiveVersion);
                    }
                    setTries(1);

                    init(mRequest.getFnId(), mRcs);
                    beginCall();
                }
                else
                {
                    onException( VersioningException(
                        serverRuntimeVersion, 
                        serverArchiveVersion));
                }
            }
            else if (err == RcfError_PingBack_Id )
            {
                // A ping back message carries a parameter specifying
                // the ping back interval in ms. The client can use that
                // to make informed decisions about whether the connection
                // has died or not.

                mPingBackIntervalMs = response.getArg0();

                // Record a timestamp and go back to receiving.

                ++mPingBackCount;
                mPingBackTimeStamp = RCF::getCurrentTimeMs();

                applyRecursionLimiter(
                    mRecursionState,
                    &ClientStub::beginReceive,
                    *this);
            }
            else
            {
                onException(RemoteException( ErrorMsg(response.getError() )) );
            }
        }
        else
        {
            RCF::CurrentClientStubSentry sentry(*this);
            mpParameters->read(mIn);
            mIn.clearByteBuffer();

#if RCF_FEATURE_FILETRANSFER==1

            // Check for any file downloads.
            {
                if (!mDownloadStreams.empty())
                {
                    std::vector<FileDownload> downloadStreams;
                    downloadStreams.swap(mDownloadStreams);
                    for (std::size_t i=0; i<downloadStreams.size(); ++i)
                    {
                        downloadStreams[i].download(*this);
                    }
                }
            }

#endif

            if (mAsync)
            {
                scheduleAmiNotification();
            }
        }
    }

    void ClientStub::scheduleAmiNotification()
    {
        if (!mCurrentCallDesc.empty())
        {
            RCF_LOG_2() << "RcfClient - end remote call. " << mCurrentCallDesc;
        }

        if (mSignalledLockPtr)
        {
            RCF_ASSERT( !mSignalledLockPtr->owns_lock() );
            mSignalledLockPtr->lock();
        }
        else
        {
            mSignalledLockPtr.reset( new Lock(*mSignalledMutexPtr) );
        }

        mCallInProgress = false;
        mSignalled = true;
        mSignalledConditionPtr->notify_all();

        std::function<void()> cb;
        if (mAsyncCallback)
        {
            cb = mAsyncCallback;
            mAsyncCallback = std::function<void()>();
        }

        getTlsAmiNotification().set(cb, mSignalledLockPtr, mSignalledMutexPtr);
    }

    bool ClientStub::ready()
    {
        Lock lock(*mSignalledMutexPtr);
        return mSignalled;
    }

    void ClientStub::waitForReady(std::uint32_t timeoutMs)
    {
        Lock lock(*mSignalledMutexPtr);
        if (!mSignalled)
        {
            if (timeoutMs)
            {
                using namespace std::chrono_literals;
                mSignalledConditionPtr->wait_for(lock, timeoutMs*1ms);
            }
            else
            {
                mSignalledConditionPtr->wait(lock);
            }            
        }
    }

    void ClientStub::cancel()
    {
        if (mTransport.get())
        {
            mTransport->cancel();
        }
        getTlsAmiNotification().run();
    }

    void ClientStub::setSubRcfClientPtr(RcfClientPtr clientStubPtr)
    {
        Lock lock(mSubRcfClientMutex);
        mSubRcfClientPtr = clientStubPtr;
    }

    RcfClientPtr ClientStub::getSubRcfClientPtr()
    {
        Lock lock(mSubRcfClientMutex);
        return mSubRcfClientPtr;
    }

    void ClientStub::setEnableNativeWstringSerialization(bool enable)
    {
        mEnableNativeWstringSerialization = enable;
    }

    bool ClientStub::getEnableNativeWstringSerialization() const
    {
        return mEnableNativeWstringSerialization;
    }

    void ClientStub::call( 
        RCF::RemoteCallMode rcs)
    {

        if (    rcs == RCF::Oneway 
            &&  (getTransportType() == RCF::Tt_Http || getTransportType() == RCF::Tt_Https) )
        {
            // Oneway is not possible over HTTP/HTTPS.
            throw RCF::Exception(RcfError_OnewayHttp);
        }

        mRetry = false;
        mRcs = rcs;
        mPingBackTimeStamp = 0;
        mPingBackCount = 0;

        // Set the progress timer timeouts.
        mNextTimerCallbackMs = 0;

        if ( mProgressCallback && mProgressCallbackIntervalMs > 0 )
        {
            // First callback should happen immediately.
            mNextTimerCallbackMs = RCF::getCurrentTimeMs();

            // So we avoid the special value 0.
            mNextTimerCallbackMs |= 1;
        }

        // We don't set ping back timeouts until we are about to receive.
        mPingBackCheckIntervalMs = 0;
        mNextPingBackCheckMs = 0;

        mSignalled = false;
        
        beginCall();
    }

    void ClientStub::setAsync(bool async)
    {
        mAsync = async;

        if (mAsync && !mSignalledMutexPtr)
        {
            mSignalledMutexPtr.reset( new Mutex() );
            mSignalledConditionPtr.reset( new Condition() );
        }
    }

    bool ClientStub::getAsync()
    {
        return mAsync;
    }

    void ClientStub::setAsyncCallback(std::function<void()> callback)
    {
        mAsyncCallback = callback;
    }

    std::unique_ptr<Exception> ClientStub::getAsyncException()
    {
        Lock lock(*mSignalledMutexPtr);
        return std::move(mAsyncException);
    }

    void ClientStub::setAsyncException(std::unique_ptr<Exception> asyncException)
    {
        Lock lock(*mSignalledMutexPtr);
        mAsyncException = std::move(asyncException);
    }

    bool ClientStub::hasAsyncException()
    {
        Lock lock(*mSignalledMutexPtr);
        return mAsyncException.get() != NULL;
    }

    typedef std::shared_ptr< ClientTransportUniquePtr > ClientTransportUniquePtrPtr;

    void vc6_helper(
        std::function<void(RcfSessionPtr, ClientTransportUniquePtr)> func,
        RcfSessionPtr sessionPtr,
        ClientTransportUniquePtrPtr clientTransportUniquePtrPtr)
    {
        func(sessionPtr, std::move(*clientTransportUniquePtrPtr));
    }

    void convertRcfSessionToRcfClient(
        OnCallbackConnectionCreated func,
        RemoteCallMode rcs)
    {
        RcfSessionPtr sessionPtr = getCurrentRcfSessionPtr()->shared_from_this();
        RcfSession & rcfSession = *sessionPtr;

        ServerTransportEx & serverTransport =
            dynamic_cast<ServerTransportEx &>(
                rcfSession.getNetworkSession().getServerTransport());

        ClientTransportUniquePtrPtr clientTransportUniquePtrPtr(
            new ClientTransportUniquePtr(
                serverTransport.createClientTransport(rcfSession.shared_from_this())));

        rcfSession.addOnWriteCompletedCallback(
            std::bind(
                vc6_helper,
                func,
                sessionPtr,
                clientTransportUniquePtrPtr) );

        bool closeSession = (rcs == RCF::Twoway);

        rcfSession.setCloseSessionAfterWrite(closeSession);
    }

    RcfSessionPtr convertRcfClientToRcfSession(
        ClientStub & clientStub, 
        ServerTransport & serverTransport,
        bool keepClientConnection)
    {
        ServerTransportEx &serverTransportEx =
            dynamic_cast<RCF::ServerTransportEx &>(serverTransport);

        ClientTransportUniquePtr clientTransportUniquePtr(
            clientStub.releaseTransport());

        SessionPtr sessionPtr = serverTransportEx.createServerSession(
            clientTransportUniquePtr,
            RcfClientPtr(),
            keepClientConnection);

        clientStub.setTransport(std::move(clientTransportUniquePtr));

        return sessionPtr;
    }

    RcfSessionPtr convertRcfClientToRcfSession(
        ClientStub & clientStub, 
        RcfServer & server,
        bool keepClientConnection)
    {
        return convertRcfClientToRcfSession(
            clientStub, 
            server.getServerTransport(), 
            keepClientConnection);
    }

    void createCallbackConnectionImpl(
        ClientStub & clientStubOrig, 
        ServerTransport & callbackServer)
    {
        {
            I_RcfClient client("", clientStubOrig);
            ClientStub & stub = client.getClientStub();
            stub.setTransport( clientStubOrig.releaseTransport() );

            OobCreateCallbackConnection msg(clientStubOrig.getRuntimeVersion());
            ByteBuffer controlRequest;
            msg.encodeRequest(controlRequest);
            stub.setOutofBandRequest(controlRequest);

            stub.ping(RCF::Twoway);

            // Get OOB response.
            ByteBuffer controlResponse = stub.getOutOfBandResponse();
            stub.setOutofBandRequest(ByteBuffer());
            stub.setOutofBandResponse(ByteBuffer());
            msg.decodeResponse(controlResponse);

            int ret = msg.mResponseError; 
            RCF_VERIFY(ret == RcfError_Ok_Id, RemoteException(ErrorMsg(ret)));

            convertRcfClientToRcfSession(client.getClientStub(), callbackServer);
        }
    }

    void createCallbackConnectionImpl(
        ClientStub & clientStub, 
        RcfServer & callbackServer)
    {
        createCallbackConnectionImpl(clientStub, callbackServer.getServerTransport());
    }

    void setCurrentCallDesc(
        std::string& desc, 
        RCF::MethodInvocationRequest& request, 
        const char * szFunc, 
        const char * szArity)
    {
        desc.clear();
        if (LogManager::instance().isEnabled(LogNameRcf, LogLevel_2))
        {
            const std::string & target = request.getService();

            desc = target;
            desc += "::";
            desc += szFunc;
            desc += "().";

            char szFnid[10] = {0};

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable: 4995) // 'sprintf': name was marked as #pragma deprecated
#pragma warning(disable: 4996) // 'sprintf'/'localtime': This function or variable may be unsafe.
#endif

            sprintf(szFnid, "%d", request.getFnId());

#if defined(_MSC_VER)
#pragma warning(pop)
#endif

            desc += " Fnid: ";
            desc += szFnid;
            desc += ". Type: ";
            desc += szArity;
        }
    }

} // namespace RCF
