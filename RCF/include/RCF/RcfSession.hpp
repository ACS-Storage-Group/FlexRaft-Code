
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

/// @file

#ifndef INCLUDE_RCF_RCFSESSION_HPP
#define INCLUDE_RCF_RCFSESSION_HPP

#include <vector>
#include <functional>
#include <memory>
#include <typeinfo>

#include <RCF/Any.hpp>
#include <RCF/Export.hpp>
#include <RCF/MethodInvocation.hpp>
#include <RCF/RcfFwd.hpp>
#include <RCF/SerializationProtocol.hpp>
#include <RCF/Tchar.hpp>

#if RCF_FEATURE_FILETRANSFER==1
#include <RCF/FileSystem.hpp>
#endif

#if RCF_FEATURE_SSPI==1
#include <wincred.h>
#endif

namespace RCF {


    struct TypeInfoCompare
    {
        bool operator()(
            const std::type_info* lhs,
            const std::type_info* rhs) const
        { 
            if (lhs->before(*rhs))
            {
                return true;
            }
            return false;
        }
    };

    /// Can only be called from within the server-side implementation of a remote call. Returns a reference to the RcfSession the remote call is executing within.
    RCF_EXPORT RcfSession &         getCurrentRcfSession();

    /// Represents a server side session, associated with a client connection.
    class RCF_EXPORT RcfSession : 
        public std::enable_shared_from_this<RcfSession>
    {
    public:

        RcfSession(RcfServer &server);
        ~RcfSession();

        typedef std::function<void(RcfSession&)> OnWriteCompletedCallback;
        typedef std::function<void(RcfSession&)> OnWriteInitiatedCallback;
        typedef std::function<void(RcfSession&)> OnDestroyCallback;

        typedef std::map<const std::type_info *, Any, TypeInfoCompare> SessionObjectMap;
        SessionObjectMap mSessionObjects;

    private:

        template<typename T>
        T * getSessionObjectImpl(bool createIfDoesntExist)
        {
            typedef std::shared_ptr<T> TPtr;

            const std::type_info & whichType = typeid(T);
            const std::type_info * pWhichType = &whichType;

            SessionObjectMap::iterator iter = mSessionObjects.find(pWhichType);
            if (iter != mSessionObjects.end())
            {
                Any & a = iter->second;
                TPtr & tPtr = a.get<TPtr>();
                RCF_ASSERT(tPtr.get());
                return tPtr.get();
            }
            else if (createIfDoesntExist)
            {
                TPtr tPtr( new T() );
                mSessionObjects[pWhichType] = tPtr;
                return tPtr.get();
            }
            else
            {
                return NULL;
            }
        }

    public:

        /// @name Session objects
        /// This section describes functions related to session objects. Session objects are objects created by
        /// application code, which are stored as part of the RcfSession. Their scope is limited to that of
        /// the RcfSession they are created within. 
        /// If you need to create server-side objects which outlive individual RCF sessions, use server objects 
        /// instead (see RcfServer).

        ///@{

        /// Deletes a session object.
        template<typename T>
        void deleteSessionObject()
        {
            typedef std::shared_ptr<T> TPtr;

            const std::type_info & whichType = typeid(T);
            const std::type_info * pWhichType = &whichType;

            SessionObjectMap::iterator iter = mSessionObjects.find(pWhichType);
            if (iter != mSessionObjects.end())
            {
                mSessionObjects.erase(iter);
            }
        }

        /// Creates a session object.
        template<typename T>
        T & createSessionObject()
        {
            deleteSessionObject<T>();
            T * pt = getSessionObjectImpl<T>(true);
            RCF_ASSERT(pt);
            if ( !pt )
            {
                RCF_THROW(Exception(RcfError_SessionObjectNotCreated, typeid(T).name()));
            }
            return *pt; 
        }

        /// Retrieves a session object, and optionally creates it.
        template<typename T>
        T & getSessionObject(bool createIfDoesntExist = false)
        {
            T * pt = getSessionObjectImpl<T>(createIfDoesntExist);
            if (!pt)
            {
                RCF_THROW(Exception(RcfError_SessionObjectDoesNotExist, typeid(T).name()));
            }
            return *pt; 
        }

        /// Queries for the existence of a session object.
        template<typename T>
        T * querySessionObject()
        {
            T * pt = getSessionObjectImpl<T>(false);
            return pt;
        }

        ///@}

        /// Returns a reference to the RcfServer that owns this RcfSession. 
        RcfServer &     getRcfServer();

        /// Disconnects this RcfSession.
        void            disconnect();

        RcfClientPtr    getDefaultStubEntryPtr();
        void            setDefaultStubEntryPtr(RcfClientPtr stubEntryPtr);
        void            setCachedStubEntryPtr(RcfClientPtr stubEntryPtr);

        /// @name Custom request/response user data
        /// The application data in a remote call is normally carried in the parameters of the remote call itself.
        /// RCF also allows you to add untyped custom data to the remote call request and response.
        ///@{
        
        /// Sets application-specific data associated with the RCF request.
        void            setRequestUserData(const std::string & userData);

        /// Gets application-specific data associated with the RCF request.
        std::string     getRequestUserData();

        /// Sets application-specific data associated with the RCF response.
        void            setResponseUserData(const std::string & userData);

        /// Gets application-specific data associated with the RCF response.
        std::string     getResponseUserData();

        ///@}

        /// @name Connection information
        /// This section describes functions relating to the current client connection.
        ///@{

        /// Gets the RCF runtime version that is in use on this RcfSession.
        std::uint32_t   getRuntimeVersion();

        /// Sets the RCF runtime version that is in use on this RcfSession.
        void            setRuntimeVersion(std::uint32_t version);

        /// Gets the archive version that is in use on this RcfSession.
        std::uint32_t   getArchiveVersion();

        /// Sets the archive version that is in use on this RcfSession.
        void            setArchiveVersion(std::uint32_t version);

        /// Gets a RemoteAddress object indicating the address of the client.
        const RemoteAddress &
                        getClientAddress();

        /// Gets a value indicating if the currently executing remote call is a one-way call.
        bool            isOneway();

        /// Gets the ping-back interval for this RcfSession.
        std::uint32_t   getPingBackIntervalMs();

        /// Gets the authenticated user name of the client, if authentication has taken place.
        tstring         getClientUserName();

        /// Gets the transport protocol in use on this RcfSession.
        TransportProtocol 
                        getTransportProtocol();

        /// Gets the underlying transport type of this RcfSession.
        TransportType   getTransportType();

#if RCF_FEATURE_SSPI==1

        /// For connections using Schannel-based HTTPS, retrieves the SSPI security context of the connection. 
        /// You can use this context to obtain further SSPI-specific information about the connection.
        PCtxtHandle     getTransportSecurityContext() const;

        /// For connections using SSPI-based transport protocols (NTLM, Kerberos, Negotiate, or Schannel), retrieves the SSPI security context of the connection. 
        /// You can use this context to obtain further SSPI-specific information about the connection.
        PCtxtHandle     getTransportProtocolSecurityContext() const;

#endif

        /// Gets a value indicating if transport level compression is enabled for this RcfSession.
        bool            getEnableCompression();

        /// Gets the certificate the client has presented, if any. Only applicable to SSL connections.
        CertificatePtr  getClientCertificatePtr();

        /// Returns information about the currently executing RCF request.
        RemoteCallInfo  getRemoteCallRequest() const;

        /// Returns the time at which the client connected to this RcfSession.
        time_t          getConnectedAtTime() const;

        /// Gets the connection duration of this RcfSession.
        std::size_t     getConnectionDuration() const;

        /// Gets the total remote call count of this RcfSession.
        std::size_t     getRemoteCallCount() const;

        /// Gets the total number of bytes received on this RcfSession.
        std::uint64_t   getTotalBytesReceived() const;

        /// Gets the total number of bytes sent on this RcfSession.
        std::uint64_t   getTotalBytesSent() const;

        /// Sets a value indicating if SF pointer tracking is enabled for this RcfSession.
        void            setEnableSfPointerTracking(bool enable);

        /// Gets a value indicating if SF pointer tracking is enabled for this RcfSession.
        bool            getEnableSfPointerTracking() const;


        ///@}

#if RCF_FEATURE_FILETRANSFER==1

        // RCF3 file API

        /// @name File transfers
        /// This section describes functions related to file transfers.
        ///@{

        /// Configures a file download from the given download path. Returns a download ID, which should be passed back to the client and used as a parameter in ClientStub::downloadFile().
        std::string         configureDownload(const Path& downloadPath, BandwidthQuotaPtr quotaPtr = nullptr);

        /// Sets whether file uploads are allowed for this RcfSession. By default file uploads are disabled.
        void                setAllowUploads(bool allowUploads);

        /// Gets whether file uploads are allowed for this RcfSession. By default file uploads are disabled.
        bool                getAllowUploads() const;

        /// Sets the bandwidth quota associated with uploads on this RcfSession. By default a RcfSession does not have a bandwidth bucket associated with it.
        void                setUploadBandwidthQuota(BandwidthQuotaPtr quotaPtr);

        /// Gets the bandwidth quota associated with uploads on this RcfSession. By default a RcfSession does not have a bandwidth bucket associated with it.
        BandwidthQuotaPtr   getUploadBandwidthQuota() const;

        /// Returns the path on disk, for the given upload.
        Path                getUploadPath(const std::string& uploadId);
        
        ///@}

#endif

        //*******************************
        // callback tables - synchronized

        // may well be called on a different thread than the one that executed the remote call
        void addOnWriteCompletedCallback(
            const OnWriteCompletedCallback &        onWriteCompletedCallback);

        void extractOnWriteCompletedCallbacks(
            std::vector<OnWriteCompletedCallback> & onWriteCompletedCallbacks);

        void setOnDestroyCallback(
            OnDestroyCallback                       onDestroyCallback);

        //*******************************

        void            setEnableNativeWstringSerialization(bool enable);
        bool            getEnableNativeWstringSerialization() const;

        void            getMessageFilters(std::vector<FilterPtr> &filters) const;
        void            getTransportFilters(std::vector<FilterPtr> &filters) const;

        void            lockTransportFilters();
        void            unlockTransportFilters();
        bool            transportFiltersLocked();

        SerializationProtocolIn &   getSpIn();
        SerializationProtocolOut &  getSpOut();

        bool                        getFiltered();
        void                        setFiltered(bool filtered);

        std::vector<FilterPtr> &    getFilters();

        void            setCloseSessionAfterWrite(bool close);



        std::uint32_t   getPingTimestamp();
        void            setPingTimestamp();

        std::uint32_t   getPingIntervalMs();
        void            setPingIntervalMs(std::uint32_t pingIntervalMs);

        std::uint32_t   getTouchTimestamp();
        bool            getCallInProgress();

        void            touch();
        void            setCallInProgress(bool callInProgress);

        void            sendPingBack();
        bool            getAutoSend();

        void            setWeakThisPtr();

        void            cancelDownload();

#if RCF_FEATURE_FILETRANSFER==1

        void            addDownloadStream(
                            std::uint32_t sessionLocalId, 
                            FileStream fileStream);

#endif

        Mutex                                   mStopCallInProgressMutex;
        bool                                    mStopCallInProgress;
        
    private:

        template<
            typename R, 
            typename A1, 
            typename A2, 
            typename A3, 
            typename A4, 
            typename A5, 
            typename A6, 
            typename A7, 
            typename A8, 
            typename A9, 
            typename A10, 
            typename A11, 
            typename A12, 
            typename A13, 
            typename A14, 
            typename A15>
        friend class AllocateServerParameters;

        template<
            typename R, 
            typename A1, 
            typename A2, 
            typename A3,
            typename A4,
            typename A5, 
            typename A6, 
            typename A7, 
            typename A8, 
            typename A9, 
            typename A10, 
            typename A11, 
            typename A12, 
            typename A13, 
            typename A14, 
            typename A15>
        friend class ServerParameters;

        friend class PingBackService;
        friend class FilterService;

        friend class StubAccess;

        RcfServer &                             mRcfServer;

        Mutex                                   mMutex;
        std::vector<OnWriteCompletedCallback>   mOnWriteCompletedCallbacks;
        std::vector<OnWriteInitiatedCallback>   mOnWriteInitiatedCallbacks;
        OnDestroyCallback                       mOnDestroyCallback;

        std::uint32_t                           mRuntimeVersion;
        std::uint32_t                           mArchiveVersion;

        bool                                    mEnableSfPointerTracking;
        
        bool                                    mTransportFiltersLocked;

        bool                                    mEnableNativeWstringSerialization = false;

        SerializationProtocolIn                 mIn;
        SerializationProtocolOut                mOut;

        // message filters
        std::vector<FilterPtr>                  mFilters;
        bool                                    mFiltered;

        MethodInvocationRequest                 mRequest;

        bool                                    mCallInProgress = false;
        bool                                    mCloseSessionAfterWrite;
        std::uint32_t                           mPingTimestamp;
        std::uint32_t                           mPingIntervalMs;
        std::uint32_t                           mTouchTimestamp;
        ByteBuffer                              mPingBackByteBuffer;
        PingBackTimerEntry                      mPingBackTimerEntry;

        Mutex                                   mIoStateMutex;
        bool                                    mWritingPingBack;
        std::vector<ByteBuffer>                 mQueuedSendBuffers;

        void clearParameters();

        void onReadCompleted();
        void onWriteCompleted();

        void processRequest();

        void processOob_RequestTransportFilters(OobMessage& msg);
        void processOob_CreateCallbackConnection(OobMessage& msg);
        void processOob_RequestSubscription(OobMessage& msg);
        void processOob_RequestProxyConnection(OobMessage& msg);
        void processOobMessages();
        
        void callServant();
        
        void sendResponse();
        void sendResponseException(const std::exception &e);
        void sendResponseUncaughtException();

        void encodeRemoteException(
            SerializationProtocolOut & out, 
            const RemoteException & e);

        void sendSessionResponse(); 

        void registerForPingBacks();
        void unregisterForPingBacks();

        void verifyTransportProtocol(RCF::TransportProtocol protocol);

        friend class RcfServer;
        friend class RemoteCallContextImpl;

        I_Parameters *                          mpParameters;
        std::vector<char>                       mParametersVec;

        // For individual parameters.
        std::vector< std::vector<char> >        mParmsVec;

        bool                                    mAutoSend;

        RcfSessionWeakPtr                       mWeakThisPtr;

    private:

        // UdpServerTransport needs to explicitly set mIoState to Reading,
        // since it doesn't use async I/O with callbacks to RcfServer.
        friend class UdpServerTransport;
        friend class UdpNetworkSession;
        friend class FileStreamImpl;

        friend class AsioNetworkSession;
        void runOnDestroyCallbacks();

#if RCF_FEATURE_FILETRANSFER==1

    private:

        friend class FileTransferService;

        FileDownloadInfoPtr                     mDownloadInfoPtr;
        FileUploadInfoPtr                       mUploadInfoPtr;

        typedef std::map<std::uint32_t, FileUploadInfoPtr> SessionUploads;
        typedef std::map<std::uint32_t, FileDownload> SessionDownloads;

        SessionUploads                          mSessionUploads;
        SessionDownloads                        mSessionDownloads;

#endif

    private:

        RcfClientPtr                            mDefaultStubEntryPtr;
        RcfClientPtr                            mCachedStubEntryPtr;

    public:
        NetworkSession & getNetworkSession() const;
        void setNetworkSession(NetworkSession & networkSession);

#if RCF_FEATURE_HTTP==1

        void getHttpFrameInfo(
            std::string&                                            requestLine, 
            std::vector< std::pair<std::string, std::string> >&     headers);

#endif

    private:
        friend class HttpSessionFilter;
        NetworkSession * mpNetworkSession;

    public:
        std::string mCurrentCallDesc;

    public:

        bool getIsCallbackSession() const;
        void setIsCallbackSession(bool isCallbackSession);

        bool isConnected() const;

    private:

        void setConnectedAtTime(time_t connectedAtTime);

        friend class SspiServerFilter;
        friend class Win32NamedPipeNetworkSession;

        tstring                 mClientUsername;
        TransportProtocol       mTransportProtocol;
        bool                    mEnableCompression;

        bool                    mTransportProtocolVerified;
        bool                    mIsCallbackSession;

        time_t                  mConnectedAtTime;

        std::size_t             mRemoteCallCount;        
    };       

} // namespace RCF

#endif // ! INCLUDE_RCF_RCFSESSION_HPP
