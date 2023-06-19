
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

#include <RCF/RcfSession.hpp>

#include <RCF/AsioServerTransport.hpp>
#include <RCF/ClientTransport.hpp>
#include <RCF/HttpFrameFilter.hpp>
#include <RCF/Marshal.hpp>
#include <RCF/PerformanceData.hpp>
#include <RCF/RcfServer.hpp>
#include <RCF/SerializationProtocol.hpp>
#include <RCF/ServerTransport.hpp>
#include <RCF/ThreadLocalData.hpp>
#include <RCF/Uuid.hpp>
#include <RCF/Version.hpp>

#include <functional>

#if RCF_FEATURE_SSPI==1
#include <RCF/Schannel.hpp>
#include <RCF/Win32Certificate.hpp>
#endif

#if RCF_FEATURE_OPENSSL==1
#include <RCF/OpenSslEncryptionFilter.hpp>
#endif

#if RCF_FEATURE_SERVER==1
#include <RCF/PingBackService.hpp>
#include <RCF/SessionTimeoutService.hpp>
#endif

namespace RCF {

    RcfSession::RcfSession(RcfServer &server) :
        mStopCallInProgress(),
        mRcfServer(server),
        mRuntimeVersion(RCF::getRuntimeVersion()),
        mArchiveVersion(RCF::getArchiveVersion()),
        mEnableSfPointerTracking(false),
        mTransportFiltersLocked(),
        mFiltered(),
        mCloseSessionAfterWrite(),
        mPingTimestamp(),
        mPingIntervalMs(),
        mTouchTimestamp(0),
        mWritingPingBack(false),
        mpParameters(),
        mParmsVec(1+15), // return value + max 15 arguments
        mAutoSend(true),
        mpNetworkSession(NULL),
        mTransportProtocol(Tp_Clear),
        mEnableCompression(false),
        mTransportProtocolVerified(false),
        mIsCallbackSession(false),
        mConnectedAtTime(0),
        mRemoteCallCount(0)
    {

        time_t now = 0;
        now = time(NULL);
        setConnectedAtTime(now);
        touch();

        Lock lock(getPerformanceData().mMutex);
        ++getPerformanceData().mRcfSessions;
    }

    void RcfSession::runOnDestroyCallbacks()
    {
        if ( mOnDestroyCallback )
        {
            mOnDestroyCallback(*this);
            mOnDestroyCallback = OnDestroyCallback();
        }
    }

    RcfSession::~RcfSession()
    {
        RCF_DTOR_BEGIN

            {
                Lock lock(getPerformanceData().mMutex);
                --getPerformanceData().mRcfSessions;
            }

            // no locks here, relying on dtor thread safety of reference counted objects
            clearParameters();
            runOnDestroyCallbacks();
        RCF_DTOR_END
    }

    NetworkSession & RcfSession::getNetworkSession() const
    {
        return *mpNetworkSession;
    }

#if RCF_FEATURE_HTTP==1

    void RcfSession::getHttpFrameInfo(
        std::string&                                            requestLine,
        std::vector< std::pair<std::string, std::string> >&     headers)
    {
        TransportType tt = getTransportType();
        if ( tt == Tt_Http || tt == Tt_Https )
        {
            std::vector<RCF::FilterPtr> filters;
            RCF::AsioNetworkSession& networkSession = static_cast<RCF::AsioNetworkSession&>( getNetworkSession() );
            networkSession.getWireFilters(filters);
            if ( filters.size() >= 2 )
            {
                RCF::HttpFrameFilter & frameFilter = static_cast<RCF::HttpFrameFilter &>(*filters[1]);
                frameFilter.getHttpFrameInfo(requestLine, headers);
            }
        }
        else
        {
            Exception e(RcfError_NotHttpConnection);
            RCF_THROW(e);
        }
    }

#endif

    void RcfSession::setNetworkSession(NetworkSession & networkSession)
    {
        mpNetworkSession = &networkSession;
    }

    void RcfSession::setEnableNativeWstringSerialization(bool enable)
    {
        mEnableNativeWstringSerialization = enable;
    }

    bool RcfSession::getEnableNativeWstringSerialization() const
    {
        return mEnableNativeWstringSerialization;
    }

    void RcfSession::clearParameters()
    {
        if (mpParameters)
        {
            mpParameters->~I_Parameters();
            mpParameters = NULL;
        }
    }

    void RcfSession::setOnDestroyCallback(OnDestroyCallback onDestroyCallback)
    {
        Lock lock(mMutex);
        mOnDestroyCallback = onDestroyCallback;
    }

    void RcfSession::setEnableSfPointerTracking(bool enable)
    {
        mEnableSfPointerTracking = enable;
    }
    
    bool RcfSession::getEnableSfPointerTracking() const
    {
        return mEnableSfPointerTracking;
    }

    void RcfSession::addOnWriteCompletedCallback(
        const OnWriteCompletedCallback &onWriteCompletedCallback)
    {
        Lock lock(mMutex);
        mOnWriteCompletedCallbacks.push_back(onWriteCompletedCallback);
    }

    void RcfSession::extractOnWriteCompletedCallbacks(
        std::vector<OnWriteCompletedCallback> &onWriteCompletedCallbacks)
    {
        Lock lock(mMutex);
        onWriteCompletedCallbacks.clear();
        onWriteCompletedCallbacks.swap( mOnWriteCompletedCallbacks );
    }

    const RCF::RemoteAddress &RcfSession::getClientAddress()
    {
        return getNetworkSession().getRemoteAddress();
    }

    void RcfSession::disconnect()
    {
        NetworkSessionPtr networkSessionPtr = getNetworkSession().shared_from_this();
        networkSessionPtr->setEnableReconnect(false);
        networkSessionPtr->postClose();
    }

    RcfClientPtr RcfSession::getDefaultStubEntryPtr()
    {
        Lock lock(mMutex);
        return mDefaultStubEntryPtr;
    }

    void RcfSession::setDefaultStubEntryPtr(RcfClientPtr stubEntryPtr)
    {
        Lock lock(mMutex);
        mDefaultStubEntryPtr = stubEntryPtr;
    }

    void RcfSession::setCachedStubEntryPtr(RcfClientPtr stubEntryPtr)
    {
        mCachedStubEntryPtr = stubEntryPtr;
    }

    void RcfSession::getMessageFilters(std::vector<FilterPtr> &filters) const
    {
        filters = mFilters;
    }

    void RcfSession::getTransportFilters(std::vector<FilterPtr> &filters) const
    {
        getNetworkSession().getTransportFilters(filters);
    }

    std::uint32_t RcfSession::getRuntimeVersion()
    {
        return mRuntimeVersion;
    }

    void RcfSession::setRuntimeVersion(std::uint32_t version)
    {
        mRuntimeVersion = version;
    }

    std::uint32_t RcfSession::getArchiveVersion()
    {
        return mArchiveVersion;
    }

    void RcfSession::setArchiveVersion(std::uint32_t version)
    {
        mArchiveVersion = version;
    }

    void RcfSession::lockTransportFilters()
    {
        mTransportFiltersLocked = true;
    }

    void RcfSession::unlockTransportFilters()
    {
        mTransportFiltersLocked = false;
    }

    bool RcfSession::transportFiltersLocked()
    {
        return mTransportFiltersLocked;
    }

    SerializationProtocolIn & RcfSession::getSpIn()
    {
        return mIn;
    }

    SerializationProtocolOut & RcfSession::getSpOut()
    {
        return mOut;
    }

    bool RcfSession::getFiltered()
    {
        return mFiltered;
    }

    void RcfSession::setFiltered(bool filtered)
    {
        mFiltered = filtered;
    }

    std::vector<FilterPtr> & RcfSession::getFilters()
    {
        return mFilters;
    }

    RcfServer & RcfSession::getRcfServer()
    {
        return mRcfServer;
    }

    void RcfSession::setCloseSessionAfterWrite(bool close)
    {
        mCloseSessionAfterWrite = close;
    }

    std::uint32_t RcfSession::getPingBackIntervalMs()
    {
        return mRequest.getPingBackIntervalMs();
    }

    std::uint32_t RcfSession::getPingTimestamp()
    {
        Lock lock(mMutex);
        return mPingTimestamp;
    }

    void RcfSession::setPingTimestamp()
    {
        Lock lock(mMutex);
        mPingTimestamp = RCF::getCurrentTimeMs();
    }

    std::uint32_t RcfSession::getPingIntervalMs()
    {
        return mPingIntervalMs;
    }

    void RcfSession::setPingIntervalMs(std::uint32_t pingIntervalMs)
    {
        mPingIntervalMs = pingIntervalMs;
    }

    std::uint32_t RcfSession::getTouchTimestamp()
    {
        Lock lock(mMutex);
        return mTouchTimestamp;
    }

    void RcfSession::touch()
    {
        Lock lock(mMutex);
        mTouchTimestamp = RCF::getCurrentTimeMs();
    }

    void RcfSession::setCallInProgress(bool callInProgress)
    {
        Lock lock(mMutex);
        mCallInProgress = callInProgress;
    }

    bool RcfSession::getCallInProgress()
    {
        Lock lock(mMutex);
        return mCallInProgress;
    }

    void RcfSession::registerForPingBacks()
    {
        // Register for ping backs if appropriate.

        if (    mRequest.getPingBackIntervalMs() > 0 
            &&  !mRequest.getOneway())
        {
            PingBackServicePtr pbsPtr = mRcfServer.getPingBackServicePtr();
            if (pbsPtr)
            {

#if RCF_FEATURE_SERVER==1

                // Disable reconnecting for this session. After sending a 
                // pingback, a server I/O thread would get a write completion 
                // notification, and if it happened to be an error (unlikely 
                // but possible), we definitely would not want a reconnect, as 
                // the session would still in use.
                getNetworkSession().setEnableReconnect(false);

                PingBackTimerEntry pingBackTimerEntry = 
                    pbsPtr->registerSession(shared_from_this());

                Lock lock(mIoStateMutex);
                RCF_ASSERT( mPingBackTimerEntry.first == 0 );
                mPingBackTimerEntry = pingBackTimerEntry;

#endif

            }
            else

            {
                // TODO: something more efficient than throwing
                Exception e(RcfError_NoPingBackService);
                RCF_THROW(e);
            }
        }
    }

    void RcfSession::unregisterForPingBacks()
    {
        // Unregister for ping backs if appropriate.

        if (    mRequest.getPingBackIntervalMs() > 0 
            &&  !mRequest.getOneway())
        {
            PingBackServicePtr pbsPtr = mRcfServer.getPingBackServicePtr();
            if (pbsPtr)
            {

#if RCF_FEATURE_SERVER==1

                pbsPtr->unregisterSession(mPingBackTimerEntry);
                mPingBackTimerEntry = PingBackTimerEntry();

#endif

            }
        }
    }

    void RcfSession::sendPingBack()
    {
        //RCF_ASSERT( mIoStateMutex.locked() );

        mWritingPingBack = true;

        ThreadLocalCached< std::vector<ByteBuffer> > tlcByteBuffers;
        std::vector<ByteBuffer> &byteBuffers = tlcByteBuffers.get();

        byteBuffers.push_back(mPingBackByteBuffer);

        std::uint32_t pingBackIntervalMs = getPingBackIntervalMs();

        encodeServerError(
            mRcfServer,
            byteBuffers.front(),
            RcfError_PingBack_Id,
            pingBackIntervalMs,
            0);

        getNetworkSession().postWrite(byteBuffers);
    }

    bool RcfSession::getAutoSend()
    {
        return mAutoSend;
    }

    void RcfSession::setWeakThisPtr()
    {
        mWeakThisPtr = shared_from_this();
    }

    void RcfSession::setRequestUserData(const std::string & userData)
    {
        mRequest.mRequestUserData = ByteBuffer(userData);
    }

    std::string RcfSession::getRequestUserData()
    {
        if ( mRequest.mRequestUserData.isEmpty() )
        {
            return std::string();
        }

        return std::string(
            mRequest.mRequestUserData.getPtr(), 
            mRequest.mRequestUserData.getLength());
    }

    void RcfSession::setResponseUserData(const std::string & userData)
    {
        mRequest.mResponseUserData = ByteBuffer(userData);
    }

    std::string RcfSession::getResponseUserData()
    {
        if ( mRequest.mResponseUserData.isEmpty() )
        {
            return std::string();
        }

        return std::string(
            mRequest.mResponseUserData.getPtr(), 
            mRequest.mResponseUserData.getLength());    
    }

    bool RcfSession::isOneway()
    {
        return mRequest.mOneway;
    }

#if RCF_FEATURE_FILETRANSFER==1

    void RcfSession::cancelDownload()
    {
        Lock lock(mMutex);
        if (mDownloadInfoPtr)
        {
            mDownloadInfoPtr->mCancel = true;
        }
    }

    void RcfSession::addDownloadStream(
        std::uint32_t sessionLocalId, 
        FileStream fileStream)
    {
        Lock lock(mMutex);
        mSessionDownloads[sessionLocalId].mImplPtr = fileStream.mImplPtr;
    }

    std::string RcfSession::configureDownload(const Path& downloadPath, BandwidthQuotaPtr quotaPtr)
    {
        std::string downloadId = generateUuid();
        TransferInfo info;
        info.mPath = downloadPath;
        info.mBandwidthQuotaPtr = quotaPtr;
        mRcfServer.mFileTransferServicePtr->addFileTransfer(downloadId, info);
        return downloadId;
    }

    Path RcfSession::getUploadPath(const std::string& uploadId)
    {
        return mRcfServer.getUploadPath(uploadId);
    }

#else

    void RcfSession::cancelDownload()
    {

    }

#endif

    tstring RcfSession::getClientUserName()
    {
        return mClientUsername;
    }

    TransportProtocol RcfSession::getTransportProtocol()
    {
        return mTransportProtocol;
    }

    bool RcfSession::getEnableCompression()
    {
        return mEnableCompression;
    }

    CertificatePtr RcfSession::getClientCertificatePtr()
    {
        if (getTransportProtocol() == Tp_Ssl)
        {
            std::vector<FilterPtr> transportFilters;
            getTransportFilters(transportFilters);
            for (std::size_t i=0; i<transportFilters.size(); ++i)
            {

#if RCF_FEATURE_SSPI==1
                if (transportFilters[i]->getFilterId() == RcfFilter_SspiSchannel)
                {
                    SchannelServerFilter & schannelFilter = static_cast<SchannelServerFilter &>(*transportFilters[i]);
                    RCF::CertificatePtr peerCertPtr = schannelFilter.getPeerCertificate();
                    return peerCertPtr;
                }
#endif

#if RCF_FEATURE_OPENSSL==1
                if (transportFilters[i]->getFilterId() == RcfFilter_OpenSsl)
                {
                    OpenSslEncryptionFilter & opensslFilter = static_cast<OpenSslEncryptionFilter &>(*transportFilters[i]);
                    RCF::CertificatePtr peerCertPtr = opensslFilter.getPeerCertificate();
                    return peerCertPtr;
                }
#endif

            }
        }

        return CertificatePtr();
    }

    TransportType RcfSession::getTransportType()
    {
        return getNetworkSession().getServerTransport().getTransportType();
    }

#if RCF_FEATURE_SSPI==1

    PCtxtHandle RcfSession::getTransportSecurityContext() const
    {
        std::vector<FilterPtr> wireFilters;
        getNetworkSession().getWireFilters(wireFilters);
        for ( auto filterPtr : wireFilters )
        {
            SspiFilter * pSspiFilter = dynamic_cast<SspiFilter *>(filterPtr.get());
            if ( pSspiFilter )
            {
                return pSspiFilter->getSecurityContext();
            }
        }
        return NULL;
    }

    PCtxtHandle RcfSession::getTransportProtocolSecurityContext() const
    {
        std::vector<FilterPtr> transportFilters;
        getTransportFilters(transportFilters);
        for ( auto filterPtr : transportFilters )
        {
            SspiFilter * pSspiFilter = dynamic_cast<SspiFilter *>(filterPtr.get());
            if ( pSspiFilter )
            {
                return pSspiFilter->getSecurityContext();
            }
        }
        return NULL;
    }

#endif


    bool RcfSession::getIsCallbackSession() const
    {
        return mIsCallbackSession;
    }

    void RcfSession::setIsCallbackSession(bool isCallbackSession)
    {
        mIsCallbackSession = isCallbackSession;
    }

    RemoteCallInfo RcfSession::getRemoteCallRequest() const
    {
        return RemoteCallInfo(mRequest);
    }

    time_t RcfSession::getConnectedAtTime() const
    {
        return mConnectedAtTime;
    }

    void RcfSession::setConnectedAtTime(time_t connectedAtTime)
    {
        mConnectedAtTime = connectedAtTime;
    }

    std::size_t RcfSession::getConnectionDuration() const
    {
        if (mConnectedAtTime)
        {
            time_t now = time(NULL);
            time_t durationS = now - mConnectedAtTime;
            return static_cast<std::size_t>(durationS);
        }
        return 0;
    }

    std::size_t RcfSession::getRemoteCallCount() const
    {
        return mRemoteCallCount;
    }

    std::uint64_t RcfSession::getTotalBytesReceived() const
    {
        return mpNetworkSession->getTotalBytesReceived();
    }

    std::uint64_t RcfSession::getTotalBytesSent() const
    {
        return mpNetworkSession->getTotalBytesSent();
    }

    bool RcfSession::isConnected() const
    {
        return getNetworkSession().isConnected();
    }

} // namespace RCF
