
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

#include <RCF/ClientStub.hpp>

#include <functional>

#include <RCF/AmiThreadPool.hpp>
#include <RCF/AsioServerTransport.hpp>
#include <RCF/ClientProgress.hpp>
#include <RCF/ClientTransport.hpp>
#include <RCF/Endpoint.hpp>
#include <RCF/FileIoThreadPool.hpp>
#include <RCF/Filter.hpp>
#include <RCF/Future.hpp>
#include <RCF/Globals.hpp>
#include <RCF/InitDeinit.hpp>
#include <RCF/IpClientTransport.hpp>
#include <RCF/Marshal.hpp>
#include <RCF/ObjectPool.hpp>
#include <RCF/SerializationProtocol.hpp>
#include <RCF/Version.hpp>
#include <RCF/Log.hpp>

#if RCF_FEATURE_SSPI
#include <RCF/SspiFilter.hpp>
#endif

namespace RCF {

    HttpCookie::HttpCookie()
    {
    }

    HttpCookie::HttpCookie(const std::string& name, const std::string& value) :
        mName(name),
        mValue(value)
    {
    }

    //****************************************
    // ClientStub

    void ClientStub::setAutoReconnect(bool autoReconnect)
    {
        mAutoReconnect = autoReconnect;
    }

    bool ClientStub::getAutoReconnect() const
    {
        return mAutoReconnect;
    }

    void ClientStub::setRemoteCallProgressCallback(
        RemoteCallProgressCallback progressCallback, 
        std::uint32_t progressCallbackIntervalMs)
    {
        mProgressCallback = progressCallback;
        mProgressCallbackIntervalMs = progressCallbackIntervalMs;
    }

    static const std::uint32_t DefaultBatchMaxMessageLimit = 1024*1024;

    ClientStub::ClientStub(const std::string &interfaceName)
    {
        init(interfaceName, "");
    }

    ClientStub::ClientStub(const std::string &interfaceName, const std::string &serverBindingName)
    {
        init(interfaceName, serverBindingName);
    }

    ClientStub::ClientStub(const ClientStub &rhs)
    {
        init(rhs.mInterfaceName, rhs.mServerBindingName);
        assign(rhs);
    }

    void ClientStub::init(const std::string & interfaceName, const std::string & serverBindingName)
    {
        mRemoteCallMode        = Twoway;
        mProtocol                       = DefaultSerializationProtocol;
        mServerBindingName              = serverBindingName;
        mInterfaceName                  = interfaceName;
        mRemoteCallTimeoutMs            = globals().getDefaultRemoteCallTimeoutMs();
        mConnectTimeoutMs               = globals().getDefaultConnectTimeoutMs();
        mAutoReconnect                  = true;
        mConnected                      = false;
        mTries                          = 0;
        mAutoVersioning                 = true;
        mRuntimeVersion                 = RCF::getRuntimeVersion();
        mArchiveVersion                 = RCF::getArchiveVersion();
        mEnableSfPointerTracking        = false;

        mAsync                          = false;
        mAsyncOpType                    = None;
        mEndTimeMs                      = 0;
        mRetry                          = false;
        mRcs                            = Twoway;
        mpParameters                    = NULL;
        mPingBackIntervalMs             = 0;
        mPingBackTimeStamp              = 0;
        mPingBackCount                  = 0;
        mNextTimerCallbackMs            = 0;
        mNextPingBackCheckMs            = 0;
        mPingBackCheckIntervalMs        = 0;

        mSignalled                      = false;

        mBatchMode                      = false;
        mBatchMaxMessageLength          = DefaultBatchMaxMessageLimit;
        mBatchCount                     = 0;
        mBatchMessageCount              = 0;
        mSetTransportProtocol           = false;

        mTransferWindowS                = 5;
        mCallInProgress                 = false;

        mHttpProxyPort                  = 0;
        mTransportProtocol              = Tp_Clear;
        mEnableCompression              = false;

        mSslImplementation              = RCF::globals().getDefaultSslImplementation();

    }

    void ClientStub::assign(const ClientStub & rhs)
    {
        if (&rhs != this)
        {
            mInterfaceName                  = rhs.mInterfaceName;
            mRemoteCallMode                 = rhs.mRemoteCallMode;
            mProtocol                       = rhs.mProtocol;
            mEndpointName                   = rhs.mEndpointName;
            mServerBindingName              = rhs.mServerBindingName;
            mRemoteCallTimeoutMs            = rhs.mRemoteCallTimeoutMs;
            mConnectTimeoutMs               = rhs.mConnectTimeoutMs;
            mAutoReconnect                  = rhs.mAutoReconnect;
            mConnected                      = false;
            mAutoVersioning                 = rhs.mAutoVersioning;
            mRuntimeVersion                 = rhs.mRuntimeVersion;
            mArchiveVersion                 = rhs.mArchiveVersion;
            mEnableSfPointerTracking        = rhs.mEnableSfPointerTracking;
            mEnableNativeWstringSerialization = rhs.mEnableNativeWstringSerialization;
            mPingBackIntervalMs             = rhs.mPingBackIntervalMs;
            mSignalled                      = false;

            setEndpoint(rhs.getEndpoint());

            mProgressCallback               = rhs.mProgressCallback;
            mProgressCallbackIntervalMs     = rhs.mProgressCallbackIntervalMs;

#if RCF_FEATURE_FILETRANSFER==1
            mFileProgressCb                 = rhs.mFileProgressCb;
            mTransferWindowS                = rhs.mTransferWindowS;
#endif

            mHttpProxy                      = rhs.mHttpProxy;
            mHttpProxyPort                  = rhs.mHttpProxyPort;
            mTransportProtocol              = rhs.mTransportProtocol;
            mUsername                       = rhs.mUsername;
            mPassword                       = rhs.mPassword;
            mKerberosSpn                    = rhs.mKerberosSpn;
            mEnableCompression              = rhs.mEnableCompression;

            mCertificatePtr                 = rhs.mCertificatePtr;
            mCaCertificatePtr               = rhs.mCaCertificatePtr;
            mCertificateValidationCb        = rhs.mCertificateValidationCb;
            mSchannelCertificateValidation  = rhs.mSchannelCertificateValidation;
            mOpenSslCipherSuite             = rhs.mOpenSslCipherSuite;

            mSslImplementation              = rhs.mSslImplementation;

            mHttpProxyPassword              = rhs.mHttpProxyPassword;
            mHttpProxyUsername              = rhs.mHttpProxyUsername;
            mHttpProxyRealm                 = rhs.mHttpProxyRealm;      
            mHttpCookies                    = rhs.mHttpCookies;
            mHttpMessageVerifierPtr         = rhs.mHttpMessageVerifierPtr;

#if RCF_FEATURE_SSPI==1
            mSchannelContextRequirements    = rhs.mSchannelContextRequirements;
            mSchannelEnabledProtocols       = rhs.mSchannelEnabledProtocols;
#endif

        }
    }

    ClientStub::~ClientStub()
    {
        disconnect();
        clearParameters();        
    }

    void ClientStub::clearParameters()
    {
        if (mpParameters)
        {
            CurrentClientStubSentry sentry(*this);
            mpParameters->~I_Parameters();
            mpParameters = NULL;
        }
    }

    ClientStub &ClientStub::operator=( const ClientStub &rhs )
    {
        assign(rhs);
        return *this;
    }

    const std::string & ClientStub::getServerBindingName() const
    {
        return mServerBindingName;
    }

    void ClientStub::setServerBindingName(const std::string & bindingName)
    {
        mServerBindingName = bindingName;
    }

    RemoteCallMode ClientStub::getRemoteCallMode() const
    {
        return mRemoteCallMode;
    }

    void ClientStub::setRemoteCallMode(
        RemoteCallMode defaultCallingSemantics)
    {
        mRemoteCallMode = defaultCallingSemantics;
    }

    void ClientStub::setSerializationProtocol(SerializationProtocol  protocol)
    {
        mProtocol = protocol;
    }

    SerializationProtocol ClientStub::getSerializationProtocol() const
    {
        return mProtocol;
    }

    void ClientStub::setEnableSfPointerTracking(bool enable)
    {
        mEnableSfPointerTracking = enable;
    }

    bool ClientStub::getEnableSfPointerTracking() const
    {
        return mEnableSfPointerTracking;
    }

    void ClientStub::setEndpoint(const Endpoint &endpoint)
    {
        mEndpoint = endpoint.clone();
    }

    void ClientStub::setEndpoint(EndpointPtr endpointPtr)
    {
        mEndpoint = endpointPtr;
    }

    EndpointPtr ClientStub::getEndpoint() const
    {
        return mEndpoint;
    }

    void ClientStub::setTransport(std::unique_ptr<ClientTransport> transport)
    {
        mTransport.reset( transport.release() );
        mConnected = mTransport.get() && mTransport->isConnected();
    }

    std::unique_ptr<ClientTransport> ClientStub::releaseTransport()
    {
        instantiateTransport();
        return std::move(mTransport);
    }

    ClientTransport& ClientStub::getTransport()
    {
        instantiateTransport();
        return *mTransport;
    }

    IpClientTransport &ClientStub::getIpTransport()
    {
        return dynamic_cast<IpClientTransport &>(getTransport());
    }

    void ClientStub::instantiateTransport()
    {
        CurrentClientStubSentry sentry(*this);
        if (!mTransport.get())
        {
            if ( !mEndpoint.get() )
            {
                Exception e(RcfError_NoEndpoint);
                RCF_THROW(e);
            }
            mTransport.reset( mEndpoint->createClientTransport().release() );
            if ( !mTransport.get() )
            {
                Exception e(RcfError_TransportCreation);
                RCF_THROW(e);
            }

            mConnected = mTransport.get() && mTransport->isConnected();
        }

        if (    mAsync 
            &&  !mTransport->isAssociatedWithIoService())
        {
            RcfServer * preferred = getAsyncDispatcher();
            AsioIoService * pIoService = NULL;

            if (preferred)
            {
                ServerTransport & transport = preferred->getServerTransport();
                AsioServerTransport & asioTransport = dynamic_cast<AsioServerTransport &>(transport);
                pIoService = & asioTransport.getIoService();
            }
            else
            {
                pIoService = & getAmiThreadPool().getIoService();
            }

            mTransport->associateWithIoService(*pIoService);
        }
    }

    void ClientStub::disconnect()
    {
        std::string endpointDesc;
        if (mEndpoint.get())
        {
            endpointDesc = mEndpoint->asString();
        }

        RcfClientPtr subRcfClientPtr = getSubRcfClientPtr();
        setSubRcfClientPtr( RcfClientPtr() );
        if (subRcfClientPtr)
        {
            subRcfClientPtr->getClientStub().disconnect();
            subRcfClientPtr.reset();
        }

        if ( mTransport.get() && endpointDesc.find("ProxyEndpoint") == 0 )
        {
            // For proxy endpoint connections, need to spin up a brand new transport.
            
            RCF_LOG_2()(this)(endpointDesc)
                << "RcfClient - disconnecting from server.";

            mTransport.reset();
            mConnected = false;
        }
        else if (mTransport.get())
        {
            RCF_LOG_2()(this)(endpointDesc)
                << "RcfClient - disconnecting from server.";

            mTransport->disconnect(mConnectTimeoutMs);
            mConnected = false;
        }

        if (mBatchBufferPtr)
        {
            mBatchBufferPtr->resize(0);
        }

        mAsyncCallback = std::function<void()>();
    }

    bool ClientStub::isConnected()
    {
        return mTransport.get() && mTransport->isConnected();
    }

    void ClientStub::setConnected(bool connected)
    {
        mConnected = connected;
    }

    void ClientStub::setMessageFilters()
    {
        setMessageFilters( std::vector<FilterPtr>());
    }

    void ClientStub::setMessageFilters(const std::vector<FilterPtr> &filters)
    {
        mMessageFilters.assign(filters.begin(), filters.end());
        RCF::connectFilters(mMessageFilters);
    }

    void ClientStub::setMessageFilters(FilterPtr filterPtr)
    {
        std::vector<FilterPtr> filters;
        filters.push_back(filterPtr);
        setMessageFilters(filters);
    }

    const std::vector<FilterPtr> &ClientStub::getMessageFilters()
    {
        return mMessageFilters;
    }

    bool ClientStub::isClientStub() const 
    { 
        return true;
    }

    void ClientStub::setRemoteCallTimeoutMs(unsigned int remoteCallTimeoutMs)
    {
        mRemoteCallTimeoutMs = remoteCallTimeoutMs;
    }

    unsigned int ClientStub::getRemoteCallTimeoutMs() const
    {
        return mRemoteCallTimeoutMs;
    }

    void ClientStub::setConnectTimeoutMs(unsigned int connectTimeoutMs)
    {
        mConnectTimeoutMs = connectTimeoutMs;
    }

    unsigned int ClientStub::getConnectTimeoutMs() const
    {
        return mConnectTimeoutMs;
    }

    void ClientStub::setAutoVersioning(bool autoVersioning)
    {
        mAutoVersioning = autoVersioning;
    }

    bool ClientStub::getAutoVersioning() const
    {
        return mAutoVersioning;
    }

    void ClientStub::setRuntimeVersion(std::uint32_t version)
    {
        mRuntimeVersion = version;
    }

    std::uint32_t ClientStub::getRuntimeVersion() const
    {
        return mRuntimeVersion;
    }

    void ClientStub::setArchiveVersion(std::uint32_t version)
    {
        mArchiveVersion = version;
    }

    std::uint32_t ClientStub::getArchiveVersion() const
    {
        return mArchiveVersion;
    }

    void ClientStub::setTries(std::size_t tries)
    {
        mTries = tries;
    }

    std::size_t ClientStub::getTries() const
    {
        return mTries;
    }

    CurrentClientStubSentry::CurrentClientStubSentry(ClientStub & clientStub)
    {
        mEnabled = true;
        pushTlsClientStub(&clientStub);
    }

    CurrentClientStubSentry::CurrentClientStubSentry(ClientStub * pClientStub)
    {
        mEnabled = false;
        if (pClientStub)
        {
            mEnabled = true;
            pushTlsClientStub(pClientStub);
        }
    }

    CurrentClientStubSentry::~CurrentClientStubSentry()
    {
        if (mEnabled)
        {
            popTlsClientStub();
        }
    }

    void ClientStub::onError(const std::exception &e)
    {
        const RemoteException *pRcfRE =
            dynamic_cast<const RemoteException *>(&e);

        const Exception *pRcfE =
            dynamic_cast<const Exception *>(&e);

        if ( pRcfE && pRcfE->getShouldRetry() )
        {
            // Disconnect automatically clears the async callback, so we need to
            // stash the callback and then restore it.
            std::function<void()> cb = mAsyncCallback;
            disconnect();
            setAsyncCallback(cb);

            call(mRcs);
            return;
        }

        if (mAsync)
        {
            scheduleAmiNotification();
        }

        mAsyncOpType = None;

        if (pRcfRE)
        {
            mEncodedByteBuffers.resize(0);
            if (shouldDisconnectOnRemoteError( pRcfRE->getErrorId() ))
            {
                disconnect();
            }

            mAsyncException.reset( pRcfRE->clone().release() );
        }
        else if (pRcfE)
        {
            mEncodedByteBuffers.resize(0);
            disconnect();
            mAsyncException.reset( pRcfE->clone().release() );
        }
        else
        {
            mEncodedByteBuffers.resize(0);
            disconnect();

            mAsyncException.reset(new Exception(e.what()));
        }
    }

    void ClientStub::onTimerExpired()
    {
        AsyncOpType opType = mAsyncOpType;
        mAsyncOpType = None;

        if (opType == Wait)
        {
            scheduleAmiNotification();
        }
        else
        {
            switch(opType)
            {
            case Connect:
                RCF_ASSERT(mEndpoint.get());
                
                onError(RCF::Exception(RcfError_ClientConnectTimeout,
                    mConnectTimeoutMs, 
                    mEndpoint->asString()));

                break;

            case Write:
                onError(RCF::Exception(RcfError_ClientWriteTimeout));
                break;

            case Read: 
                onError(RCF::Exception(RcfError_ClientReadTimeout));
                break;

            default:
                RCF_ASSERT_ALWAYS("");
            };
        }        
    }

    void ClientStub::setInterfaceName(const std::string & interfaceName)
    {
        mInterfaceName = interfaceName;
    }

    const std::string & ClientStub::getInterfaceName()
    {
        return mInterfaceName;
    }

    SerializationProtocolIn & ClientStub::getSpIn()
    {
        return mIn;
    }

    SerializationProtocolOut & ClientStub::getSpOut()
    {
        return mOut;
    }

    void ClientStub::setPingBackIntervalMs(int pingBackIntervalMs)
    {
        mPingBackIntervalMs = pingBackIntervalMs;
    }
    
    int ClientStub::getPingBackIntervalMs()
    {
        return mPingBackIntervalMs;
    }

    std::size_t ClientStub::getPingBackCount()
    {
        return mPingBackCount;
    }

    std::uint32_t ClientStub::getPingBackTimeStamp()
    {
        return mPingBackTimeStamp;
    }

    FutureConverter<Void> ClientStub::ping()
    {
        return ping( CallOptions() );
    }

    FutureConverter<Void> ClientStub::ping(const CallOptions & callOptions)
    {
        typedef Void V;

        CurrentClientStubSentry sentry(*this);

        setAsync(false);

        return RCF::FutureConverter<V>(
            ::RCF::AllocateClientParameters<
                V
                    ,
                V,V,V,V,V,V,V,V,V,V,V,V,V,V,V >()(
                    *this
                        ,
                    V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V(),V()).r.get(),
            *this,
            -1,
            callOptions.apply(*this),
            "ping",
            "V0");

    }   

    // Take the proposed timeout and cut it down to accommodate client progress 
    // callbacks and checking of ping back interval.

    std::uint32_t ClientStub::generatePollingTimeout(std::uint32_t timeoutMs)
    {
        std::uint32_t timeNowMs = RCF::getCurrentTimeMs();

        std::uint32_t timeToNextTimerCallbackMs = mNextTimerCallbackMs ?
            mNextTimerCallbackMs - timeNowMs:
            std::uint32_t(-1);

        std::uint32_t timeToNextPingBackCheckMs = mNextPingBackCheckMs ?
            mNextPingBackCheckMs - timeNowMs:
            std::uint32_t(-1);

        return 
            RCF_MIN( 
                RCF_MIN(timeToNextTimerCallbackMs, timeToNextPingBackCheckMs), 
                timeoutMs);
    }

    void ClientStub::onPollingTimeout(bool eventBased)
    {
        // Check whether we need to fire a client progress timer callback.

        bool timerExpired = mNextTimerCallbackMs && 0 == generateTimeoutMs(mNextTimerCallbackMs);

        if ( eventBased || timerExpired )
        {
            RemoteCallProgressInfo progressInfo;
            mTransport->getProgressInfo(progressInfo);

            if ( mProgressCallback )
            {
                RemoteCallAction action = RemoteCallAction::Rca_Continue;
                mProgressCallback(progressInfo, action);

                RCF_VERIFY(
                    action == RemoteCallAction::Rca_Continue,
                    Exception(RcfError_ClientCancel));
            }

            if ( timerExpired )
            {
                mNextTimerCallbackMs =
                    RCF::getCurrentTimeMs() + mProgressCallbackIntervalMs;

                mNextTimerCallbackMs |= 1;
            }
        }

        // Check that pingbacks have been received.
        if (mNextPingBackCheckMs && 0 == generateTimeoutMs(mNextPingBackCheckMs))
        {
            std::uint32_t timeNowMs = RCF::getCurrentTimeMs();

            std::uint32_t timeSinceLastPingBackMs = 
                timeNowMs - mPingBackTimeStamp;

            // Checking for subsequent pingbacks.
            RCF_VERIFY(
                timeSinceLastPingBackMs < mPingBackCheckIntervalMs,
                Exception(RcfError_PingBackTimeout, mPingBackCheckIntervalMs));

            // Setup polling for next pingback.
            mPingBackCheckIntervalMs = 3 * mPingBackIntervalMs;

            mNextPingBackCheckMs = 
                RCF::getCurrentTimeMs() + mPingBackCheckIntervalMs;

            mNextPingBackCheckMs |= 1;
        }

    }

    void ClientStub::onUiMessage()
    {
    }

    //**************************************************************************
    // Synchronous transport filter requests.

    ByteBuffer ClientStub::getOutOfBandRequest()
    {
        return mRequest.mOutOfBandRequest;
    }

    void ClientStub::setOutofBandRequest(ByteBuffer requestBuffer)
    {
        mRequest.mOutOfBandRequest = requestBuffer;
    }

    ByteBuffer ClientStub::getOutOfBandResponse()
    {
        return mRequest.mOutOfBandResponse;
    }

    void ClientStub::setOutofBandResponse(ByteBuffer responseBuffer)
    {
        mRequest.mOutOfBandResponse = responseBuffer;
    }

    void ClientStub::setupProxiedConnection(const std::string& proxyEndpointName)
    {
        ClientStub stub(*this);
        stub.setTransport(releaseTransport());

        RestoreClientTransportGuard guard(*this, stub);

        // Set OOB request.
        OobRequestProxyConnection msg(getRuntimeVersion(), proxyEndpointName);
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
        RCF_VERIFY(ret == RcfError_Ok_Id, RemoteException(msg.mResponseErrorString));
    }

    void ClientStub::requestTransportFilters(const std::vector<FilterPtr> &filters)
    {
        if ( getRuntimeVersion() <= 11 )
        {
            requestTransportFilters_Legacy(filters);
            return;
        }

        ClientStub stub(*this);
        stub.setTransport( releaseTransport());

        RestoreClientTransportGuard guard(*this, stub);

        // Set OOB request.
        OobRequestTransportFilters msg(getRuntimeVersion(), filters);
        ByteBuffer controlRequest;
        msg.encodeRequest(controlRequest);
        stub.setOutofBandRequest(controlRequest);

        stub.ping(RCF::Twoway);

        // Get OOB response.
        ByteBuffer controlResponse = stub.getOutOfBandResponse();
        stub.setOutofBandRequest(ByteBuffer());
        stub.setOutofBandResponse(ByteBuffer());
        msg.decodeResponse(controlResponse);

        if ( msg.mResponseError != RcfError_Ok_Id )
        {
            if ( msg.mResponseErrorString.size() > 0 )
            {
                RCF_THROW(RemoteException(msg.mResponseErrorString));
            }
            else
            {
                RCF_THROW(RemoteException(ErrorMsg(msg.mResponseError)));
            }
        }

        for (std::size_t i=0; i<filters.size(); ++i)
        {
            filters[i]->resetState();
        }

        stub.getTransport().setTransportFilters(filters);
    }

    void ClientStub::requestTransportFilters(FilterPtr filterPtr)
    {
        std::vector<FilterPtr> filters;
        if (filterPtr.get())
        {
            filters.push_back(filterPtr);
        }
        requestTransportFilters(filters);
    }

    void ClientStub::requestTransportFilters()
    {
        requestTransportFilters( std::vector<FilterPtr>());
    }

    void ClientStub::clearTransportFilters()
    {
        disconnect();
        if (mTransport.get())
        {
            mTransport->setTransportFilters( std::vector<FilterPtr>());
        }
    }

    //**************************************************************************
    // Asynchronous transport filter requests.

    void onRtfCompleted(
        RCF::Future<Void>                           fv, 
        RcfClientPtr                                rtfClientPtr,
        ClientStub &                                clientStubOrig,
        std::shared_ptr<std::vector<FilterPtr> >    filters, 
        std::function<void()>                       onCompletion)
    {
        ClientStubPtr rtfStubPtr = rtfClientPtr->getClientStubPtr();
        clientStubOrig.setTransport( rtfStubPtr->releaseTransport() );
        clientStubOrig.setSubRcfClientPtr( RcfClientPtr() );

        std::unique_ptr<Exception> ePtr = fv.getAsyncException();
        if ( ePtr.get() )
        {
            clientStubOrig.setAsyncException( std::move(ePtr) );
        }
        else
        {
            // Get OOB response.
            OobRequestTransportFilters msg(clientStubOrig.getRuntimeVersion());
            ByteBuffer controlResponse = rtfStubPtr->getOutOfBandResponse();
            rtfStubPtr->setOutofBandRequest(ByteBuffer());
            rtfStubPtr->setOutofBandResponse(ByteBuffer());
            msg.decodeResponse(controlResponse);

            if ( msg.mResponseErrorString.size() > 0 )
            {
                ePtr.reset(new RemoteException(msg.mResponseErrorString));
                clientStubOrig.setAsyncException(std::move(ePtr));
            }
            else if ( msg.mResponseError != RcfError_Ok_Id )
            {
                ePtr.reset( new RemoteException(ErrorMsg(msg.mResponseError)) );
                clientStubOrig.setAsyncException(std::move(ePtr));
            }
            else
            {
                for (std::size_t i=0; i<filters->size(); ++i)
                {
                    (*filters)[i]->resetState();
                }
                clientStubOrig.getTransport().setTransportFilters(*filters);
            }
        }
        onCompletion();
    }


    void ClientStub::requestTransportFiltersAsync(
        const std::vector<FilterPtr> &filters,
        std::function<void()> onCompletion)
    {       
        if ( getRuntimeVersion() <= 11 )
        {
            requestTransportFiltersAsync_Legacy(filters, onCompletion);
            return;
        }

        RcfClientPtr rtfClientPtr( new I_RcfClient("", *this) );
        rtfClientPtr->getClientStub().setTransport( releaseTransport());
        setSubRcfClientPtr(rtfClientPtr);

        setAsync(true);

        OobRequestTransportFilters msg(getRuntimeVersion(), filters);
        ByteBuffer controlRequest;
        msg.encodeRequest(controlRequest);
        rtfClientPtr->getClientStub().setOutofBandRequest(controlRequest);

        Future<Void> fv;

        std::shared_ptr<std::vector<FilterPtr> > filtersPtr(
            new std::vector<FilterPtr>(filters) );

        fv = rtfClientPtr->getClientStub().ping( RCF::AsyncTwoway( std::bind(
            &onRtfCompleted, 
            fv,
            rtfClientPtr,
            std::ref(*this),
            filtersPtr,
            onCompletion)));
    }

    void ClientStub::requestTransportFiltersAsync(
        FilterPtr filterPtr,
        std::function<void()> onCompletion)
    {
        std::vector<FilterPtr> filters;
        if (filterPtr.get())
        {
            filters.push_back(filterPtr);
        }
        requestTransportFiltersAsync(filters, onCompletion);
    }

    std::vector<char> & ClientStub::getRetValVec() 
    { 
        return mRetValVec; 
    }

    // Batching

    void ClientStub::enableBatching()
    {
        setRemoteCallMode(Oneway);

        mBatchMode = true;
        if (!mBatchBufferPtr)
        {
            mBatchBufferPtr.reset( new ReallocBuffer() );
        }
        mBatchBufferPtr->resize(0);
        mBatchCount = 0;
        mBatchMessageCount = 0;
    }

    void ClientStub::disableBatching(bool flush)
    {
        if (flush)
        {
            flushBatch();
        }
        mBatchMode = false;
        mBatchBufferPtr->resize(0);
        mBatchMessageCount = 0;
    }

    void ClientStub::flushBatch(unsigned int timeoutMs)
    {
        CurrentClientStubSentry sentry(*this);

        if (timeoutMs == 0)
        {
            timeoutMs = getRemoteCallTimeoutMs();
        }

        try
        {
            std::vector<ByteBuffer> buffers;
            buffers.push_back( ByteBuffer(mBatchBufferPtr) );
            int err = getTransport().send(*this, buffers, timeoutMs);
            RCF_UNUSED_VARIABLE(err);

            mBatchBufferPtr->resize(0);

            ++mBatchCount;
            mBatchMessageCount = 0;
        }
        catch(const RemoteException & e)
        {
            RCF_UNUSED_VARIABLE(e);
            mEncodedByteBuffers.resize(0);
            throw;
        }
        catch(...)
        {
            mEncodedByteBuffers.resize(0);
            disconnect();
            throw;
        }
    }

    void ClientStub::setMaxBatchMessageLength(std::uint32_t maxBatchMessageLength)
    {
        mBatchMaxMessageLength = maxBatchMessageLength;
    }

    std::uint32_t ClientStub::getMaxBatchMessageLength()
    {
        return mBatchMaxMessageLength;
    }

    std::uint32_t ClientStub::getBatchesSent()
    {
        return mBatchCount;
    }

    std::uint32_t ClientStub::getMessagesInCurrentBatch()
    {
        return mBatchMessageCount;
    }

    void ClientStub::setRequestUserData(const std::string & userData)
    {
        mRequest.mRequestUserData = ByteBuffer(userData);
    }

    std::string ClientStub::getRequestUserData()
    {
        if ( mRequest.mRequestUserData.isEmpty() )
        {
            return std::string();
        }

        return std::string(
            mRequest.mRequestUserData.getPtr(), 
            mRequest.mRequestUserData.getLength());
    }

    void ClientStub::setResponseUserData(const std::string & userData)
    {
        mRequest.mResponseUserData = ByteBuffer(userData);
    }

    std::string ClientStub::getResponseUserData()
    {
        if ( mRequest.mResponseUserData.isEmpty() )
        {
            return std::string();
        }

        return std::string(
            mRequest.mResponseUserData.getPtr(), 
            mRequest.mResponseUserData.getLength());
    }

    void ClientStub::setHttpProxy(const std::string & httpProxy)
    {
        if (httpProxy != mHttpProxy)
        {
            mHttpProxy = httpProxy;
            disconnect();
        }
    }

    std::string ClientStub::getHttpProxy() const
    {
        return mHttpProxy;
    }

    void ClientStub::setHttpProxyPort(int httpProxyPort)
    {
        if (httpProxyPort != mHttpProxyPort)
        {
            mHttpProxyPort = httpProxyPort;
            disconnect();
        }
    }

    int ClientStub::getHttpProxyPort() const
    {
        return mHttpProxyPort;
    }

    void ClientStub::setHttpProxyRealm(const tstring & proxyRealm)
    {
        mHttpProxyRealm = proxyRealm;
    }

    tstring ClientStub::getHttpProxyRealm() const
    {
        return mHttpProxyRealm;
    }

    void ClientStub::setHttpCookies(const std::vector<HttpCookie> & cookies)
    {
        for ( auto cookie : cookies )
        {
            mHttpCookies[cookie.mName] = cookie;
        }
    }

    std::vector<HttpCookie> ClientStub::getHttpCookies() const
    {
        std::vector<HttpCookie> cookies;
        for ( auto cookieEntry : mHttpCookies )
        {
            cookies.push_back(cookieEntry.second);
        }
        return cookies;
    }

    void ClientStub::clearHttpCookies()
    {
        mHttpCookies.clear();
    }

    std::map<std::string, HttpCookie> & ClientStub::getCookieMap()
    {
        return mHttpCookies;
    }

    void ClientStub::setHttpRedirectHandler(HttpRedirectHandler redirectHandler)
    {
        mHttpRedirectHandler = redirectHandler;
    }

    HttpRedirectHandler ClientStub::getHttpRedirectHandler() const
    {
        return mHttpRedirectHandler;
    }

    void ClientStub::setHttpUrlParameterString(const std::string & urlParameterString)
    {
        mHttpUrlParameterString = urlParameterString;
    }

    std::string ClientStub::getHttpUrlParameterString() const
    {
        return mHttpUrlParameterString;
    }

    void ClientStub::setHttpMessageVerifier(HttpMessageVerifierPtr verifierPtr)
    {
        mHttpMessageVerifierPtr = verifierPtr;
    }

    HttpMessageVerifierPtr  ClientStub::getHttpMessageVerifier() const
    {
        return mHttpMessageVerifierPtr;
    }

    void ClientStub::setTransportProtocol(TransportProtocol protocol)
    {
        if (mTransportProtocol != protocol)
        {
            mTransportProtocol = protocol;
            if ( mTransport.get() )
            {
                RcfSessionWeakPtr sessionWeakPtr = getTransport().getRcfSession();
                bool isRegularConnection = (sessionWeakPtr == RcfSessionWeakPtr());
                if ( isRegularConnection )
                {
                    disconnect();
                    clearTransportFilters();
                }
                else
                {
                    mSetTransportProtocol = true;
                }
            }
        }
    }

    TransportProtocol ClientStub::getTransportProtocol() const
    {
        return mTransportProtocol;
    }

    TransportType ClientStub::getTransportType()
    {
        instantiateTransport();
        return mTransport->getTransportType();
    }

    void ClientStub::setUserName(const tstring & username)
    {
        mUsername = username;
    }

    tstring ClientStub::getUserName() const
    {
        return mUsername;
    }

    void ClientStub::setPassword(const tstring & password)
    {
        mPassword = password;
    }

    tstring ClientStub::getPassword() const
    {
        return mPassword;
    }

    void ClientStub::setHttpProxyUserName(const tstring & username)
    {
        mHttpProxyUsername = username;
    }

    tstring ClientStub::getHttpProxyUserName() const
    {
        return mHttpProxyUsername;
    }

    void ClientStub::setHttpProxyPassword(const tstring & password)
    {
        mHttpProxyPassword = password;
    }

    tstring ClientStub::getHttpProxyPassword() const
    {
        return mHttpProxyPassword;
    }


    void ClientStub::setKerberosSpn(const tstring & kerberosSpn)
    {
        mKerberosSpn = kerberosSpn;
    }

    tstring ClientStub::getKerberosSpn() const
    {
        return mKerberosSpn;
    }

    void ClientStub::setEnableCompression(bool enableCompression)
    {
        if (mEnableCompression != enableCompression)
        {
            mEnableCompression = enableCompression;
            disconnect();
            clearTransportFilters();
        }
    }

    bool ClientStub::getEnableCompression() const
    {
        return mEnableCompression;
    }

    void ClientStub::setSspiMessageProtection(SspiMessageProtection sspiMessageProtection)
    {
        mSspiMessageProtection = sspiMessageProtection;
    }

    SspiMessageProtection ClientStub::getSspiMessageProtection() const
    {
        return mSspiMessageProtection;
    }

    void ClientStub::setCertificate(CertificatePtr certificatePtr)
    {
        mCertificatePtr = certificatePtr;
    }

    CertificatePtr ClientStub::getCertificate() const
    {
        return mCertificatePtr;
    }

    void ClientStub::setCaCertificate(CertificatePtr caCertificatePtr)
    {
        mCaCertificatePtr = caCertificatePtr;

        mCertificateValidationCb = CertificateValidationCallback();
        mSchannelCertificateValidation.clear();
    }

    CertificatePtr ClientStub::getCaCertificate() const
    {
        return mCaCertificatePtr;
    }

    void ClientStub::setCertificateValidationCallback(
        CertificateValidationCallback certificateValidationCb)
    {
        mCertificateValidationCb = certificateValidationCb;

        mCaCertificatePtr.reset();
        mSchannelCertificateValidation.clear();
    }

    const CertificateValidationCallback & ClientStub::getCertificateValidationCallback() const
    {
        return mCertificateValidationCb;
    }

    void ClientStub::setEnableSchannelCertificateValidation(const tstring & peerName)
    {
        mSchannelCertificateValidation = peerName;

        mCaCertificatePtr.reset();
        mCertificateValidationCb = CertificateValidationCallback();
    }

    tstring ClientStub::getEnableSchannelCertificateValidation() const
    {
        return mSchannelCertificateValidation;
    }

    void ClientStub::setOpenSslCipherSuite(const std::string & cipherSuite)
    {
        mOpenSslCipherSuite = cipherSuite;
    }

    std::string ClientStub::getOpenSslCipherSuite() const
    {
        return mOpenSslCipherSuite;
    }

    void ClientStub::setTlsSniName(const tstring & serverName)
    {
        mTlsSniName = serverName;
    }

    tstring ClientStub::getTlsSniName() const
    {
        return mTlsSniName;
    }

    void ClientStub::setSslImplementation(SslImplementation sslImplementation)
    {
        mSslImplementation = sslImplementation;
    }

    SslImplementation ClientStub::getSslImplementation() const
    {
        return mSslImplementation;
    }

#if RCF_FEATURE_SSPI==1

    void ClientStub::setSchannelEnabledProtocols(DWORD enabledProtocols)
    {
        mSchannelEnabledProtocols = enabledProtocols;
    }

    DWORD ClientStub::getSchannelEnabledProtocols() const
    {
        return mSchannelEnabledProtocols;
    }

    void ClientStub::setSchannelContextRequirements(ULONG contextRequirements)
    {
        mSchannelContextRequirements = contextRequirements;
    }

    ULONG ClientStub::getSchannelContextRequirements() const
    {
        return mSchannelContextRequirements;
    }

    PCtxtHandle ClientStub::getTransportSecurityContext() const
    {
        if ( mTransport )
        {
            std::vector<FilterPtr> wireFilters;
            mTransport->getWireFilters(wireFilters);
            for ( auto filterPtr : wireFilters )
            {
                SspiFilter * pSspiFilter = dynamic_cast<SspiFilter *>(filterPtr.get());
                if ( pSspiFilter )
                {
                    return pSspiFilter->getSecurityContext();
                }
            }
        }

        return NULL;
    }

    PCtxtHandle ClientStub::getTransportProtocolSecurityContext() const
    {
        if ( mTransport )
        {
            std::vector<FilterPtr> transportFilters;
            mTransport->getTransportFilters(transportFilters);
            for ( auto filterPtr : transportFilters )
            {
                SspiFilter * pSspiFilter = dynamic_cast<SspiFilter *>(filterPtr.get());
                if ( pSspiFilter )
                {
                    return pSspiFilter->getSecurityContext();
                }
            }
        }

        return NULL;
    }

#endif


#ifdef RCF_WINDOWS

    void ClientStub::setWindowsImpersonationToken(HANDLE hToken)
    {
        mWindowsImpersonationToken.reset( new HANDLE(hToken) );
    }

    HANDLE ClientStub::getWindowsImpersonationToken() const
    {
        if (mWindowsImpersonationToken.get())
        {
            return *mWindowsImpersonationToken;
        }
        return INVALID_HANDLE_VALUE;
    }

#endif

    RestoreClientTransportGuard::RestoreClientTransportGuard(
        ClientStub &client, 
        ClientStub &clientTemp) :
            mClient(client),
            mClientTemp(clientTemp)
    {}

    RestoreClientTransportGuard::~RestoreClientTransportGuard()
    {
        RCF_DTOR_BEGIN
            mClient.setTransport(mClientTemp.releaseTransport());
        RCF_DTOR_END
    }

    CallOptions::CallOptions() :
        mAsync(false),
        mRcsSpecified(false),
        mRcs(Twoway),
        mCallback()
    {
    }

    CallOptions::CallOptions(RemoteCallMode rcs) :
        mAsync(false),
        mRcsSpecified(true),
        mRcs(rcs),
        mCallback()
    {
    }

    CallOptions::CallOptions(RemoteCallMode rcs, const std::function<void()> & callback) :
        mAsync(true),
        mRcsSpecified(true),
        mRcs(rcs),
        mCallback(callback)
    {
    }

    CallOptions::CallOptions(std::function<void()> callback) :
        mAsync(true),
        mRcsSpecified(false),
        mRcs(Twoway),
        mCallback(callback)
    {
    }

    RemoteCallMode CallOptions::apply(ClientStub &clientStub) const
    {
        clientStub.setAsync(mAsync);
        clientStub.setAsyncCallback(mCallback);
        return mRcsSpecified ? mRcs : clientStub.getRemoteCallMode();
    }

    AsyncTwoway::AsyncTwoway(const std::function<void()> & callback) :
        CallOptions(RCF::Twoway, callback)
    {
    }

    AsyncOneway::AsyncOneway(const std::function<void()> & callback) :
        CallOptions(RCF::Oneway, callback)
    {
    }

} // namespace RCF
