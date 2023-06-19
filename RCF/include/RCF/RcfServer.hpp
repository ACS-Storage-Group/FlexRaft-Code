
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

#ifndef INCLUDE_RCF_RCFSERVER_HPP
#define INCLUDE_RCF_RCFSERVER_HPP

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <RCF/Export.hpp>
#include <RCF/RcfClient.hpp>
#include <RCF/ServerTransport.hpp>
#include <RCF/ThreadLibrary.hpp>

#if RCF_FEATURE_FILETRANSFER==1
#include <RCF/FileTransferService.hpp>
#endif

#if RCF_FEATURE_PUBSUB==1
#include <RCF/PublishingService.hpp>
#include <RCF/SubscriptionService.hpp>
#endif

#include <RCF/ServerObjectService.hpp>
#include <RCF/ServerStub.hpp>

namespace RCF {

    /// Provides RCF server-side functionality. 
    
    /// An RcfServer instance listens on one or more server transport layers, and responds
    /// to remote calls from RCF clients. Remote calls from RCF clients are routed through 
    /// to servant bindings, configured with RcfServer::bind().
    class RCF_EXPORT RcfServer : Noncopyable
    {
    public:

        typedef std::function<void(RcfServer&)> StartCallback;

    public:

        /// @name Constructors and destructor

        ///@{

        /// Constructs an RcfServer instance with no initial server transport layers.
        RcfServer();
        
        /// Constructs an RcfServer instance with one initial server transport layer.
        RcfServer(const Endpoint &endpoint);

        /// Constructs an RcfServer instance with one initial server transport layer.
        RcfServer(ServerTransportPtr serverTransportPtr);

        /// Destroys an RcfServer instance.
        ~RcfServer();

        ///@}

        /// @name Transports
        /// This section describes functions relating to the transport layers of a RcfServer.

        ///@{

        /// Adds another server transport layer to the RcfServer.
        ServerTransport &       addEndpoint(const Endpoint & endpoint);

        /// Starts the RcfServer. After starting, clients are able to connect to the server and execute remote calls.
        void                    start();

        /// Stops the RcfServer. After stopping, clients are no longer able to connect to the server.
        void                    stop();

        /// Returns true if the RcfServer has been started.
        bool                    isStarted();

        /// Enumerates the proxy endpoints hosted on this RcfServer. See ProxyEndpoint.
        void                    enumerateProxyEndpoints(std::vector<std::string>& endpoints);

        /// Sets the supported transport protocols for this RcfServer.

        /// If one or more transport protocols are specified, then clients are required to use one of the specified transport 
        /// protocols to connect to the server.
        /// If no encryption protocols are specified, then clients may use any transport protocol to connect to the server.
        void                    setSupportedTransportProtocols(
                                    const std::vector<TransportProtocol> & protocols);

        /// Gets the supported encryption protocols for this RcfServer.
        const std::vector<TransportProtocol> &
                                getSupportedTransportProtocols() const;

        /// Sets the thread pool for this RcfServer to use when dispatching remote calls.
        /// The thread pool specified by this function is used by all the transports of the RcfServer. If you want a particular transport
        /// to use a different thread pool, use RCF::ServerTransport::setThreadPool().
        void                    setThreadPool(ThreadPoolPtr threadPoolPtr);

        /// Gets the thread pool for this RcfServer.
        ThreadPoolPtr           getThreadPool();


        ///@}

        // Waits for the RcfServer to stop. Can be called from any thread.
        void                    waitForStopEvent();

        // Waits for the RcfServer to start.  Can be called from any thread.
        void                    waitForStartEvent();

        /// Gets the first server transport of the RcfServer.
        ServerTransport &       getServerTransport();

        /// Gets the first server transport of the RcfServer, and casts it to an IpServerTransport.
        IpServerTransport &     getIpServerTransport();


        /// @name Servant binding
        /// This section describes functions related to servant binding.
        /// When clients make a remote call to a server, the call is routed to a corresponding servant object 
        /// on the server. Servant objects are bound to a particular RCF interface, and must have functions 
        /// that match the methods of the RCF interface.

        ///@{

        /// Creates a servant binding, exposing the servant object to remote calls through the RCF interface InterfaceT.

        /// The servant binding name is optional and normally only needed if you are creating several servant 
        /// bindings for a single RCF interface.
        template<typename InterfaceT, typename ImplementationT>
        ServerBindingPtr        bind(ImplementationT & servantObj, const std::string &name = "")
        { 
            std::reference_wrapper<ImplementationT> refWrapper(servantObj);

            RcfClientPtr rcfClientPtr = createServerStub( 
                (InterfaceT *) 0,
                (ImplementationT *) 0, refWrapper);

            return bindImpl( 
                name.empty() ? 
                    InterfaceT::getInterfaceName() :
                    name , 
                rcfClientPtr); 
        }

        /// Removes a servant binding from the RcfServer.
        template<typename InterfaceT>
        bool                    unbind(const std::string &name = "")
        {
            const std::string &name_ = (name == "") ?
                getInterfaceName((InterfaceT *) NULL) :
                name;

            WriteLock writeLock(mStubMapMutex);
            mStubMap.erase(name_);
            return true;
        }
        
        ///@}

        /// @name SSL settings
        /// This section describes functions relating to usage of SSL.

        ///@{

        /// Sets the server-side SSL certificate the RcfServer presents to SSL-based clients.
        void                    setCertificate(CertificatePtr certificatePtr);

        /// Gets the server-side SSL certificate the RcfServer presents to SSL-based clients.
        CertificatePtr          getCertificate();

        /// Sets the cipher suite to use when establishing an SSL connection. Only applicable to OpenSSL - based SSL.
        void                    setOpenSslCipherSuite(const std::string & cipherSuite);

        /// Gets the cipher suite to use when establishing an SSL connection. Only applicable to OpenSSL - based SSL.
        std::string             getOpenSslCipherSuite() const;

        /// Sets the certificate authority certificate to use when validating certificates from a HTTPS client. Not applicable to Schannel-based SSL.
        void                    setCaCertificate(CertificatePtr certificatePtr);

        /// Gets the certificate authority certificate to use when validating certificates from a HTTPS client. Not applicable to Schannel-based SSL.
        CertificatePtr          getCaCertificate();

        /// Sets the certificate validation callback, used to validate client certificates. 
        void                    setCertificateValidationCallback(CertificateValidationCallback certificateValidationCb);

        /// Gets the certificate validation callback, used to validate client certificates. 
        const CertificateValidationCallback & 
                                getCertificateValidationCallback() const;

        /// Sets the certificate name to match against, when validating a client certificate. Only applicable to Schannel-based SSL.
        void                    setEnableSchannelCertificateValidation(const tstring & peerName);

        /// Gets the certificate name to match against, when validating a client certificate. Only applicable to Schannel-based SSL.
        tstring                 getEnableSchannelCertificateValidation() const;

        /// Sets the SSL implementation in use by this RcfServer (OpenSSL or Schannel).
        void                    setSslImplementation(SslImplementation sslImplementation);

        /// Gets the SSL implementation in use by this RcfServer (OpenSSL or Schannel).
        SslImplementation       getSslImplementation() const;

#if RCF_FEATURE_SSPI==1
        void                    setSchannelEnabledProtocols(DWORD enabledProtocols);
        DWORD                   getSchannelEnabledProtocols() const;

        void                    setSchannelContextRequirements(ULONG contextRequirements);
        ULONG                   getSchannelContextRequirements() const;
#endif

        ///@}

        /// @name Timeout settings
        /// This section describes functions related to server timeout settings.

        ///@{

        /// Sets the idle connection timeout value, in ms.

        /// Client connections will be dropped if they remain idle long enough for the timeout setting to apply. A connection is idle if no remote calls are being made on it.
        void                    setConnectionIdleTimeoutMs(std::uint32_t idleConnectionTimeoutMs);

        /// Gets the idle connection timeout value, in ms.
        std::uint32_t           getConnectionIdleTimeoutMs();

        /// Sets the idle connection scan interval value, in ms.

        /// This setting controls how often the RcfServer scans for idle client connections.
        void                    setConnectionIdleScanIntervalMs(std::uint32_t idleConnectionScanIntervalMs);

        /// Gets the idle connection scan interval value, in ms.
        std::uint32_t           getConnectionIdleScanIntervalMs();

        /// Sets the HTTP session timeout value, in ms.

        /// This setting controls how long idle HTTP sessions are allowed to persist on the server, 
        /// before being destroyed.
        void                    setHttpSessionTimeoutMs(std::uint32_t httpSessionTimeoutMs);

        /// Gets the HTTP session timeout value, in ms.
        std::uint32_t           getHttpSessionTimeoutMs();

        /// Sets the value of the "Server" header in HTTP responses.
        void                    setHttpServerHeader(const std::string & httpServerHeader);

        /// Gets the value of the "Server" header in HTTP responses.
        std::string             getHttpServerHeader() const;

#if RCF_FEATURE_HTTP==1
        void                    setHttpMessageVerifier(HttpMessageVerifierPtr verifierPtr);
        HttpMessageVerifierPtr  getHttpMessageVerifier() const;
#endif

        ///@}

        /// @name Server objects
        /// This section describes functions related to server objects.
        /// Server objects are created by server-side application code. Server objects can be of arbitrary type,
        /// and are associated with a string identifier which must be unique within the server. Server objects
        /// have an associated timeout value, which determines how long a server object can be idle before
        /// the server cleans it up.

        ///@{

        /// Gets the server object harvesting interval, in s.
        std::uint32_t getServerObjectHarvestingIntervalS() const;

        /// Sets the server object harvesting interval, in s.
        void setServerObjectHarvestingIntervalS(std::uint32_t harvestingIntervalS);

        /// Queries for a server object under the given key. Returns an empty std::shared_ptr if no object is found.
        template<typename T>
        std::shared_ptr<T> queryServerObject(
            const std::string & objectKey)
        {
            return mServerObjectServicePtr->queryServerObject<T>(objectKey);
        }

        /// Queries or creates a server object under the given key. If creating the object, the timeout value is applicable and determines how long the object will be kept alive if left unused.
        template<typename T>
        std::shared_ptr<T> getServerObject(
            const std::string & objectKey,
            std::uint32_t timeoutMs)
        {
            return mServerObjectServicePtr->getServerObject<T>(objectKey, timeoutMs);
        }

        /// Deletes the server object under the given key.
        void deleteServerObject(const std::string & objectKey);

        ///@}

#if RCF_FEATURE_PUBSUB==1

        /// @name Publish/subscribe
        /// This section describes functions related to publish/subscribe functionality. 

        ///@{

        /// Creates a publisher instance for the given RCF interface.

        /// Use the Publisher::publish() function on the returned Publisher instance,
        /// to publish remote calls to all listening subscribers.
        template<typename Interface>
        std::shared_ptr< Publisher<Interface> > createPublisher()
        {
            PublisherParms parms;
            return mPublishingServicePtr->createPublisher<Interface>(parms);
        }

        /// Creates a publisher instance for the given RCF interface.

        /// Use the Publisher::publish() function on the returned Publisher instance,
        /// to publish remote calls to all listening subscribers.
        template<typename Interface>
        std::shared_ptr< Publisher<Interface> > createPublisher(
            const PublisherParms & parms)
        {
            return mPublishingServicePtr->createPublisher<Interface>(parms);
        }

        /// Creates a subscription to a remote RCF publisher.

        /// Remote calls from the publisher will be routed to the given servant object.
        template<typename Interface, typename T>
        std::shared_ptr< Subscription > createSubscription(
            T &                             servantObj, 
            const RCF::Endpoint &           publisherEp)
        {
            RCF_ASSERT(mStarted);
            SubscriptionParms parms;
            parms.setPublisherEndpoint(publisherEp);
            return mSubscriptionServicePtr->createSubscription<Interface>(servantObj, publisherEp);
        }

        /// Creates a subscription to a remote RCF publisher.

        /// Remote calls from the publisher will be routed to the given servant object.
        template<typename Interface, typename T>
        std::shared_ptr< Subscription > createSubscription(
            T &                             servantObj,
            const SubscriptionParms &       parms)
        {
            RCF_ASSERT(mStarted);
            return mSubscriptionServicePtr->createSubscription<Interface>(servantObj, parms);
        }

        ///@}

#endif

        /// Sets whether proxy endpoints are enabled on this RcfServer.
        void                setEnableProxyEndpoints(bool enable);

        /// Gets whether proxy endpoints are enabled on this RcfServer.
        bool                getEnableProxyEndpoints() const;

#if RCF_FEATURE_FILETRANSFER==1

        /// @name File transfers
        /// This section describes functions related to file transfers.

        ///@{

        /// Sets the total bandwidth limit for all uploads to this RcfServer. By default the bandwidth limit is zero (unlimited).
        void                setUploadBandwidthLimit(std::uint32_t uploadQuotaBps);

        /// Gets the total bandwidth limit for all uploads to this RcfServer. By default the bandwidth limit is zero (unlimited).
        std::uint32_t       getUploadBandwidthLimit() const;

        /// Sets the total bandwidth limit for all downloads from this RcfServer. By default the bandwidth limit is zero (unlimited).
        void                setDownloadBandwidthLimit(std::uint32_t downloadQuotaBps);

        /// Gets the total bandwidth limit for all downloads from this RcfServer. By default the bandwidth limit is zero (unlimited).
        std::uint32_t       getDownloadBandwidthLimit() const;

        /// Sets the path under which client uploads are saved. Must be set before any files can be uploaded.
        void                setUploadDirectory(const Path & uploadDir);

        /// Gets the path under which client uploads are saved. Must be set before any files can be uploaded.
        Path                getUploadDirectory() const;

        /// Sets the download progress callback for this RcfServer. The callback is called for every chunk that is transferred when a client is downloading a file.
        void                setDownloadProgressCallback(DownloadProgressCallback downloadProgressCb);

        /// Sets the upload progress callback for this RcfServer. The callback is called for every chunk that is transferred when a client is uploading a file.
        void                setUploadProgressCallback(UploadProgressCallback uploadProgressCb);

        /// Sets a custom upload bandwidth quota callback. The callback is called when a upload begins, and allows the application to assign a bandwidth quota to that upload.
        void                setUploadBandwidthQuotaCallback(UploadBandwidthQuotaCallback uploadQuotaCb);

        /// Sets a custom download bandwidth quota callback. The callback is called when a download begins, and allows the application to assign a bandwidth quota to that download.
        void                setDownloadBandwidthQuotaCallback(DownloadBandwidthQuotaCallback downloadQuotaCb);

        ///@}

#endif


        
        // For internal use.
        std::uint32_t           getRuntimeVersion();
        void                    setRuntimeVersion(std::uint32_t version);

        /// Gets the archive version number used by this RcfServer.
        std::uint32_t           getArchiveVersion();

        /// Sets the archive version number used by this RcfServer.
        void                    setArchiveVersion(std::uint32_t version);

        void setOnCallbackConnectionCreated(OnCallbackConnectionCreated onCallbackConnectionCreated);
        OnCallbackConnectionCreated getOnCallbackConnectionCreated();


    private:

        void init();

    public:


        I_Service &             
                getServerTransportService();

        ServerTransportPtr      
                getServerTransportPtr();

    private:

        bool    addService(
                    ServicePtr servicePtr);

        bool    removeService(
                    ServicePtr servicePtr);

    public:

        PingBackServicePtr 
                getPingBackServicePtr();

        FileTransferServicePtr
                getFileTransferServicePtr();

        SessionTimeoutServicePtr 
                getSessionTimeoutServicePtr();

        PublishingServicePtr
                getPublishingServicePtr();

        SubscriptionServicePtr
                getSubscriptionServicePtr();

        FilterServicePtr getFilterServicePtr();

        bool    addServerTransport(
                    ServerTransportPtr serverTransportPtr);

        bool    removeServerTransport(
                    ServerTransportPtr serverTransportPtr);

        ServerTransport * 
                findTransportCompatibleWith(ClientTransport & clientTransport);

        ServerTransport * queryForTransport(RCF::TransportType transportType);
        
        void    setStartCallback(const StartCallback &startCallback);       
        
    private:
        void    invokeStartCallback();

    private:
        ServerBindingPtr bindImpl(
                    const std::string &name, 
                    RcfClientPtr rcfClientPtr);

        //*************************************
        // async io transport interface

    public:
        SessionPtr  createSession();
        void        onReadCompleted(SessionPtr sessionPtr);
        void        onWriteCompleted(SessionPtr sessionPtr);


        //*************************************
        // transports, queues and threads

    public:


        template<typename Iter>
        void enumerateSessions(const Iter & iter)
        {
            for (std::size_t i=0; i<mServerTransports.size(); ++i)
            {
                mServerTransports[i]->enumerateSessions(iter);
            }
        }

        //*************************************
        // stub management

    private:
        ReadWriteMutex                                  mStubMapMutex;
        typedef std::map<std::string, RcfClientPtr>     StubMap;
        StubMap                                         mStubMap;


        typedef std::function<void(const JsonRpcRequest &, JsonRpcResponse &)> JsonRpcMethod;
        typedef std::map<std::string, JsonRpcMethod>    JsonRpcMethods;
        JsonRpcMethods                                  mJsonRpcMethods;

        friend class RcfSession;



        //*************************************
        // service management

    private:
        std::vector<ServerTransportPtr>                 mServerTransports;
        std::vector<ServicePtr>                         mServices;
        FilterServicePtr                                mFilterServicePtr;
        PingBackServicePtr                              mPingBackServicePtr;
        FileTransferServicePtr                          mFileTransferServicePtr;
        SessionTimeoutServicePtr                        mSessionTimeoutServicePtr;
        PublishingServicePtr                            mPublishingServicePtr;
        SubscriptionServicePtr                          mSubscriptionServicePtr;
        CallbackConnectionServicePtr                    mCallbackConnectionServicePtr;
        ProxyEndpointServicePtr                         mProxyEndpointServicePtr;
        ServerObjectServicePtr                          mServerObjectServicePtr;

        void startService(ServicePtr servicePtr) const;
        void stopService(ServicePtr servicePtr) const;
        void resolveServiceThreadPools(ServicePtr servicePtr) const;

        friend class AsioNetworkSession;
        FilterPtr createFilter(int filterId);

        // start/stop functionality
        StartCallback                                   mStartCallback;
        Condition                                       mStartEvent;
        Condition                                       mStopEvent;

        Mutex                                           mStartStopMutex;
        bool                                            mStarted;      

    private:
        ThreadPoolPtr                                   mThreadPoolPtr;


        // TODO: get rid of this hack
    private:
        friend class MethodInvocationRequest;

    private:
        std::uint32_t                                 mRuntimeVersion;
        std::uint32_t                                 mArchiveVersion;


    public:

#if RCF_FEATURE_FILETRANSFER==1


        Path getUploadPath(const std::string & uploadId);

    private:

        DownloadProgressCallback            mOnFileDownloadProgress;
        UploadProgressCallback              mOnFileUploadProgress;

        Path                                mFileUploadDirectory;

        std::uint32_t                       mFileUploadQuota;
        UploadBandwidthQuotaCallback        mFileUploadQuotaCb;

        std::uint32_t                       mFileDownloadQuota;
        DownloadBandwidthQuotaCallback      mFileDownloadQuotaCb;

        friend class FileTransferService;

#endif

    private:

        mutable ReadWriteMutex          mPropertiesMutex;

        std::vector<TransportProtocol>  mSupportedProtocols;
        CertificatePtr                  mCertificatePtr;
        std::string                     mOpenSslCipherSuite;

        CertificatePtr                  mCaCertificatePtr;
        CertificateValidationCallback   mCertificateValidationCb;
        tstring                         mSchannelCertificateValidation;

        SslImplementation               mSslImplementation;

        std::uint32_t                   mSessionTimeoutMs;
        std::uint32_t                   mSessionHarvestingIntervalMs;

        std::uint32_t                   mHttpSessionTimeoutMs;

        std::string                     mHttpServerHeader;

        OnCallbackConnectionCreated     mOnCallbackConnectionCreated;

#if RCF_FEATURE_SSPI==1
        DWORD                           mSchannelEnabledProtocols = 0;
        ULONG                           mSchannelContextRequirements = 0;
#endif

    public:


    private:

        std::uint32_t                           mServerObjectHarvestingIntervalS;

        friend class HttpSessionFilter;

        HttpSessionPtr  attachHttpSession(const std::string & httpSessionId, bool allowCreate, ExceptionPtr & ePtr);
        void            detachHttpSession(HttpSessionPtr httpSessionPtr);
        
        friend class ServerObjectService;
        void            harvestHttpSessions();

#if RCF_FEATURE_HTTP==1
        Mutex                                   mHttpSessionMapMutex;
        std::map<std::string, HttpSessionPtr>   mHttpSessionMap;

        HttpMessageVerifierPtr  mHttpMessageVerifierPtr;

#endif

    public:


    private:
        friend class ProxyEndpoint;
        ClientTransportUniquePtr makeProxyEndpointConnection(const std::string& proxyEndpointName);

        bool                                    mEnableProxyEndpoints = false;

    };

} // namespace RCF

#endif // ! INCLUDE_RCF_RCFSERVER_HPP

