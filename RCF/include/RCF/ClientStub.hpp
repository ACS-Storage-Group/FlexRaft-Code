
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

/// \file

#ifndef INCLUDE_RCF_CLIENTSTUB_HPP
#define INCLUDE_RCF_CLIENTSTUB_HPP

#include <string>
#include <vector>
#include <memory>

#include <RCF/ClientProgress.hpp>
#include <RCF/ClientTransport.hpp>
#include <RCF/Enums.hpp>
#include <RCF/Export.hpp>
#include <RCF/RcfClient.hpp>
#include <RCF/MethodInvocation.hpp>
#include <RCF/SerializationProtocol_Base.hpp>
#include <RCF/RcfFwd.hpp>
#include <RCF/RecursionLimiter.hpp>
#include <RCF/SerializationProtocol.hpp>
#include <RCF/Tchar.hpp>
#include <RCF/ThreadLibrary.hpp>

#if RCF_FEATURE_FILETRANSFER==1
#include <RCF/FileDownload.hpp>
#include <RCF/FileUpload.hpp>
#endif

#ifdef RCF_WINDOWS
#include <windows.h>
#endif

#ifdef RCF_WINDOWS
#include <wincred.h>
#endif

namespace RCF {

    /// Represents an HTTP cookie, with a name and value.

    /// HTTP cookies are not used by RCF itself, but may be returned from HTTP reverse proxies 
    /// in order to implement functionality such as session affinity.
    class RCF_EXPORT HttpCookie
    {
    public:
        HttpCookie();
        HttpCookie(const std::string& name, const std::string& value);
        std::string mName;
        std::string mValue;
    };

    class RCF_EXPORT CurrentClientStubSentry
    {
    public:
        CurrentClientStubSentry(ClientStub & clientStub);
        CurrentClientStubSentry(ClientStub * pClientStub);
        ~CurrentClientStubSentry();

    private:
        bool mEnabled;
    };

    /// Controls the client side of a RCF connection. 
    
    /// ClientStub is normally accessed through the RcfClient<>::getClientStub() function.
    class RCF_EXPORT ClientStub : 
        public ClientTransportCallback, 
        public std::enable_shared_from_this<ClientStub>
    {

    public:
        ClientStub(const std::string & interfaceName);
        ClientStub(const std::string & interfaceName, const std::string &serverBindingName);
        ClientStub(const ClientStub & rhs);
        ~ClientStub();

        ClientStub &operator=(const ClientStub & rhs);

    private:
        void                    init(const std::string & interfaceName, const std::string & serverBindingName);
        void                    assign(const ClientStub & rhs);

        // Used internally only.
        friend class I_RcfClient;
        void                    setInterfaceName(const std::string & interfaceName);


    public:

        /// @name Transport layer
        /// This section describes functions relating to the transport layer of a ClientStub. The transport layer 
        /// encapsulates the physical network connection used by a ClientStub instance to communicate with the server.
        
        ///@{

        /// Sets the network endpoint at which the RCF server is located.
        void                    setServerEndpoint(const Endpoint &endpoint);

        /// Sets the network endpoint at which the RCF server is located.
        void                    setServerEndpoint(EndpointPtr endpointPtr);

        /// Gets the network endpoint at which the RCF server is located.
        EndpointPtr             getServerEndpoint() const;

        /// Replaces the client transport for this ClientStub.
        void                    setTransport(ClientTransportUniquePtr transport);

        /// Returns a reference to the client transport of this ClientStub.
        ClientTransport&        getTransport();

        /// Returns a reference to the client transport of this ClientStub, as an IpClientTransport reference. Throws if the client transport is not IP-based.
        IpClientTransport &     getIpTransport();

        /// Releases the client transport from this ClientStub.
        ClientTransportUniquePtr  releaseTransport();

        /// Gets the client transport type.
        TransportType           getTransportType();

        /// Establishes a network connection to the server.
        void                    connect();

        /// Disconnects from the server.
        void                    disconnect();

        /// Gets a boolean value indicating if the ClientStub has been connected.
        bool                    isConnected();

        /// Makes a two-way ping call to the RCF server.
        FutureConverter<Void>        ping();

        /// Sets whether automatic network re-connection is enabled.
        
        /// If this setting is enabled, when making a call, the RCF client will automatically reconnect 
        /// if it discovers there has been a network disconnection. In some situations this may not be 
        /// desirable, as your application may have custom logic, for example some type of login call, that 
        /// needs to run when a connection is established.
        void                    setAutoReconnect(bool autoReconnect);

        /// Gets whether automatic network re-connection is enabled.
        bool                    getAutoReconnect() const;

        /// Sets the connect timeout.

        /// The connect timeout only applies to the establishment of a physical network connection to the
        /// server. To set timeouts related to the execution of a remote call, see setRemoteCallTimeoutMs().
        void                    setConnectTimeoutMs(unsigned int connectTimeoutMs);

        /// Gets the connect timeout.
        unsigned int            getConnectTimeoutMs() const;

        ///@}

        // Instantiates a client transport for this ClientStub, based on the endpoint type.
        void                    instantiateTransport();

        // Sets the network endpoint at which the RCF server is located.
        void                    setEndpoint(const Endpoint &endpoint);

        // Sets the network endpoint at which the RCF server is located.
        void                    setEndpoint(EndpointPtr endpointPtr);

        // Gets the network endpoint at which the RCF server is located.
        EndpointPtr             getEndpoint() const;


        /// @name Transport protocol settings
        /// This section describes functions relating to the transport protocol of the client. Transport protocols
        /// are used to encrypt communication over the transport layer.

        ///@{

        /// Sets the transport protocol.
        void                    setTransportProtocol(TransportProtocol protocol);

        /// Gets the transport protocol.
        TransportProtocol       getTransportProtocol() const;

        /// Sets the user name to use for authenticating to the server.

        /// Only applicable to transport protocols that need a user name.
        void                    setUserName(const tstring & username);

        /// Gets the user name to use when authenticating to the server.
        tstring                 getUserName() const;

        /// Sets the password to use when authenticating to the server.

        /// Only applicable to transport protocols that need a password.
        void                    setPassword(const tstring & password);

        /// Gets the password to use when authenticating to the server.
        tstring                 getPassword() const;

        /// Sets the Kerberos SPN to use when authenticating to the server.

        // Only applicable to the Kerberos transport protocol.
        void                    setKerberosSpn(const tstring & kerberosSpn);

        /// Gets the Kerberos SPN to use when authenticating to the server.
        tstring                 getKerberosSpn() const;

        /// Sets whether compression is enabled for this connection. 
        
        /// Compression requires zlib and RCF_FEATURE_ZLIB=1 to be defined.
        void                    setEnableCompression(bool enableCompression);

        /// Gets whether compression is enabled for this connection. 
        bool                    getEnableCompression() const;

        /// Sets the protection level for messages sent using the Kerberos, NTLM and Negotiate transport protocols.
        void                    setSspiMessageProtection(SspiMessageProtection sspiMessageProtection);

        /// Gets the protection level for messages sent using the Kerberos, NTLM and Negotiate transport protocols.
        SspiMessageProtection   getSspiMessageProtection() const;

        ///@}


        /// @name Remote call settings
        /// This section describes functions relating to performing remote calls on a server.

        ///@{

        /// Gets the RCF interface name of the RCF client.
        const std::string &     getInterfaceName();

        /// Gets the remote call mode.
        RemoteCallMode          getRemoteCallMode() const;

        /// Sets the remote call mode.
        void                    setRemoteCallMode(RemoteCallMode defaultCallingSemantics);

        /// Sets the remote call serialization protocol.
        void                    setSerializationProtocol(SerializationProtocol protocol);

        /// Gets the remote call serialization protocol.
        SerializationProtocol   getSerializationProtocol() const;

        /// Sets pointer tracking mode when using SF serialization.
        void                    setEnableSfPointerTracking(bool enable);

        /// Gets pointer tracking mode when using SF serialization.
        bool                    getEnableSfPointerTracking() const;

        /// Sets the auto-versioning property. 
        
        /// If auto-versioning is enabled, the RCF client will automatically adjust the RCF runtime version 
        // and the archive version back to that of the RCF server, if the server reports a version mismatch.
        /// Auto-versioning is enabled by default.
        void                    setAutoVersioning(bool autoVersioning);

        /// Gets the auto-versioning property.
        bool                    getAutoVersioning() const;

        /// Sets the RCF runtime version of this ClientStub. 
        
        /// The RCF runtime version controls the behavior of RCF client and server runtimes.
        /// Adjustments to the RCF runtime version are normally made automatically (see setAutoVersioning()), 
        /// and allow interoperability between newer and older RCF builds. However, in some cases it may 
        /// be desirable to set the runtime version explicitly.
        void                    setRuntimeVersion(std::uint32_t version);

        /// Gets the RCF runtime version of this ClientStub.
        std::uint32_t           getRuntimeVersion() const;

        /// Sets the archive version of this ClientStub. 

        /// The archive version is used to control the serialization format changes that you make to your
        /// application data structures over time.
        void                    setArchiveVersion(std::uint32_t version);

        /// Gets the archive version in use by this ClientStub.
        std::uint32_t           getArchiveVersion() const;

        /// Sets the ping-back interval. 
        
        /// The ping-back interval controls how often the RCF server will ping the RCF client, while a remote call is executing on the server.
        void                    setPingBackIntervalMs(int pingBackIntervalMs);

        /// Gets the ping-back interval.
        int                     getPingBackIntervalMs();

        /// Sets the remote call timeout.

        /// This setting determines how long a RCF client will wait for a remote call to finish executing on the server.
        void                    setRemoteCallTimeoutMs(unsigned int remoteCallTimeoutMs);

        /// Gets the remote call timeout.
        unsigned int            getRemoteCallTimeoutMs() const;

        /// Sets the remote progress callback. 
        /// The remote call progress callback will be called at the specified time interval while a remote call is in progress.
        void                    setRemoteCallProgressCallback(
                                    RemoteCallProgressCallback  progressCallback,
                                    std::uint32_t               callbackIntervalMs);


        ///@}

        // Gets the servant binding name to use on the RCF server.
        const std::string &     getServerBindingName() const;

        // Sets the servant binding name to use on the RCF server.
        void                    setServerBindingName(const std::string &bindingName);


#if RCF_FEATURE_FILETRANSFER==1

        /// @name File transfers
        /// Small files can be sent or received as part of regular remote call. For large (or potentially large)
        /// files, RCF provides specialized upload and download functionality.

        ///@{

        /// Downloads a file from a RcfServer. The download must already have been configured by the server, using RcfSession::configureDownload(). downloadPath can be either a directory or a file. If it is a directory, the file name will be set by the server.
        void                    downloadFile(
                                    const std::string&      downloadId, 
                                    const Path&             downloadPath, 
                                    FileTransferOptions *   pOptions = NULL);

        /// Uploads a file to a RcfServer. The RcfServer must previously have enabled uploads and configured an upload directory. The upload ID must be empty, or a valid UUID. If it is empty, the server will generate a UUID and return it.
        void                    uploadFile(
                                    std::string&            uploadId,
                                    const Path&             uploadPath, 
                                    FileTransferOptions *   pOptions = NULL);

        ///@}

#endif


        /// @name Custom request/response user data
        /// The application data in a remote call is normally carried in the parameters of the remote call itself.
        /// RCF also allows you to add untyped custom data to the remote call request and response.
        ///@{

        /// Sets application-specific data associated with the RCF request.
        void                    setRequestUserData(const std::string & userData);

        /// Gets application-specific data associated with the RCF request.
        std::string             getRequestUserData();

        /// Sets application-specific data associated with the RCF response.
        void                    setResponseUserData(const std::string & userData);

        /// Gets application-specific data associated with the RCF response.
        std::string             getResponseUserData();

        ///@}

        // Gets out-of-band data associated with the next request.
        ByteBuffer              getOutOfBandRequest();

        // Sets out-of-band data associated with the next request.
        void                    setOutofBandRequest(ByteBuffer requestBuffer);

        // Gets out-of-band data associated with the previous response.
        ByteBuffer              getOutOfBandResponse();

        // Sets out-of-band data associated with the previous response.
        void                    setOutofBandResponse(ByteBuffer responseBuffer);


        /// @name HTTP settings
        /// RCF allows you to tunnel remote calls through a HTTP or HTTPS connection. This section describes functions relating to HTTP and HTTPS based connections.

        ///@{

        /// Sets the HTTP proxy to use for HTTP/HTTPS connections.
        void                    setHttpProxy(const std::string & httpProxy);

        /// Gets the HTTP proxy to use for HTTP/HTTPS connections.
        std::string             getHttpProxy() const;

        /// Sets the HTTP proxy port to use for HTTP/HTTPS connections.
        void                    setHttpProxyPort(int httpProxyPort);

        /// Gets the HTTP proxy port to use for HTTP/HTTPS connections.
        int                     getHttpProxyPort() const;

        /// Sets the user name to use when authenticating to an HTTP proxy.
        void                    setHttpProxyUserName(const tstring & proxyUsername);

        /// Gets the user name to use when authenticating to an HTTP proxy.
        tstring                 getHttpProxyUserName() const;

        /// Sets the password to use when authenticating to an HTTP proxy.
        void                    setHttpProxyPassword(const tstring & proxyPassword);

        /// Gets the password to use when authenticating to an HTTP proxy.
        tstring                 getHttpProxyPassword() const;

        /// Sets the realm value to use when authenticating to an HTTP proxy.
        void                    setHttpProxyRealm(const tstring & proxyRealm);

        /// Gets the realm value to use when authenticating to an HTTP proxy.
        tstring                 getHttpProxyRealm() const;

        /// Sets a list of cookies to include in HTTP requests to the server. 

        /// Cookies can be used to implement session affinity when connecting to a server farm through HTTP routing infrastructure.
        /// When remote calls are passed through server-side HTTP routing infrastructure, cookies may be returned, indicating session affinity information.
        /// By retrieving these cookies and applying them to a several RcfClient<> instances, multiple clients can be made to connect to the same back-end server.
        void                    setHttpCookies(const std::vector<HttpCookie> & cookies);

        /// Gets a list of cookies that have been returned as part of HTTP responses.
        std::vector<HttpCookie> getHttpCookies() const;

        /// Clears the list of cookies to include in HTTP requests to the server.
        void                    clearHttpCookies();

        /// Sets the redirect handler, to be called when the client receives a HTTP redirect response.
        void                    setHttpRedirectHandler(HttpRedirectHandler redirectHandler);

        /// Gets the redirect handler.
        HttpRedirectHandler     getHttpRedirectHandler() const;

        /// Sets the URL parameter string to append to the server URL when making a HTTP request.
        void                    setHttpUrlParameterString(const std::string & urlParameterString);

        /// Gets the URL parameter string to append to the server URL when making a HTTP request.
        std::string             getHttpUrlParameterString() const;

        HttpRedirectHandler     mHttpRedirectHandler;

        /// Sets the HTTP message verification mechanism to use for HTTP connections.
        void                    setHttpMessageVerifier(HttpMessageVerifierPtr verifierPtr);

        /// Gets the HTTP message verification mechanism to use for HTTP connections.
        HttpMessageVerifierPtr  getHttpMessageVerifier() const;

        HttpMessageVerifierPtr  mHttpMessageVerifierPtr;
        
        ///@}       

        /// @name SSL settings
        /// This section describes functions relating to usage of SSL, either as part of an HTTPS connection,
        /// or when using a SSL transport protocol.
        ///@{

        /// Sets the certificate to use when authenticating to a SSL server.
        void                    setCertificate(CertificatePtr certificatePtr);

        /// Gets the certificate to use when authenticating to a SSL server.
        CertificatePtr          getCertificate() const;

        /// Sets the certificate authority certificate to use when validating certificates from a SSL server. Not applicable to Schannel-based SSL.
        void                    setCaCertificate(CertificatePtr certificatePtr);

        /// Gets the certificate authority certificate to use when validating certificates from a SSL server. Not applicable to Schannel-based SSL.
        CertificatePtr          getCaCertificate() const;

        /// Sets the cipher suite to use when establishing an SSL connection. Only applicable to OpenSSL-based SSL.
        void                    setOpenSslCipherSuite(const std::string & cipherSuite);

        /// Gets the cipher suite to use when establishing an SSL connection. Only applicable to OpenSSL-based SSL.
        std::string             getOpenSslCipherSuite() const;

        /// Sets the certificate name to match against, when validating a server certificate. Only applicable to Schannel-based SSL.
        void                    setEnableSchannelCertificateValidation(const tstring & peerName);

        /// Gets the certificate name to match against, when validating a server certificate. Only applicable to Schannel-based SSL.
        tstring                 getEnableSchannelCertificateValidation() const;

        /// Sets the certificate validation callback.
        void                                setCertificateValidationCallback(CertificateValidationCallback certificateValidationCb);

        /// Gets the certificate validation callback.
        const CertificateValidationCallback &     getCertificateValidationCallback() const;

        // Sets the TLS SNI server name value. Only applicable to HTTPS connections.
        void                    setTlsSniName(const tstring & serverName);

        // Gets the TLS SNI server name value.  Only applicable to HTTPS connections.
        tstring                 getTlsSniName() const;

        /// Sets the SSL implementation in use by this ClientStub (OpenSSL or Schannel).
        void                    setSslImplementation(SslImplementation sslImplementation);

        /// Gets the SSL implementation in use by this ClientStub (OpenSSL or Schannel).
        SslImplementation       getSslImplementation() const;

#if RCF_FEATURE_SSPI==1
        
        void                    setSchannelEnabledProtocols(DWORD enabledProtocols);
        DWORD                   getSchannelEnabledProtocols() const;

        void                    setSchannelContextRequirements(ULONG contextRequirements);
        ULONG                   getSchannelContextRequirements() const;

        /// For connections using Schannel-based HTTPS, retrieves the SSPI security context of the connection. 
        /// You can use this context to obtain further SSPI-specific information about the connection.
        PCtxtHandle             getTransportSecurityContext() const;

        /// For connections using SSPI-based transport protocols (NTLM, Kerberos, Negotiate, or Schannel), retrieves the SSPI security context of the connection. 
        /// You can use this context to obtain further SSPI-specific information about the connection.
        PCtxtHandle             getTransportProtocolSecurityContext() const;
#endif

        ///@}

#ifdef RCF_WINDOWS
        /// Sets the Windows impersonation token to use, while authenticating to the server. Impersonation tokens are only applicable to NTLM or Kerberos transport protocols.
        void                setWindowsImpersonationToken(HANDLE hToken);

        /// Gets the Windows impersonation token to use, while authenticating to the server. Impersonation tokens are only applicable to NTLM or Kerberos transport protocols.
        HANDLE              getWindowsImpersonationToken() const;
#endif


        /// @name Message batching
        /// This section describes functions relating to message batching. Message batching is a variant of 
        /// one-way messaging, where multiple one-way messages are accumulated until manually flushed, or 
        /// the maximum batch length is reached.

        ///@{

        /// Enables batch mode.
        void                enableBatching();

        /// Disables batch mode, optionally flushing the last batch.
        void                disableBatching(bool flush = true);

        /// Flushes the current batch of messages.
        void                flushBatch(unsigned int timeoutMs = 0);

        /// Sets the maximum message length of a batch of messages.
        void                setMaxBatchMessageLength(std::uint32_t maxBatchMessageLength);

        /// Gets the maximum message length of a batch of messages.
        std::uint32_t       getMaxBatchMessageLength();

        /// Gets the number of batches sent since batch mode was enabled.
        std::uint32_t       getBatchesSent();

        /// Gets the number of messages accumulated in the current batch.
        std::uint32_t       getMessagesInCurrentBatch();

        ///@}

        /// @name Asynchronous calls
        /// This section describes functions related to asynchronous remote calls. To make an asynchronous
        /// remote call, at least one parameter or return type of a remote call must be a Future instance.

        ///@{

        /// Establishes a network connection to the server asynchronously.
        void                connectAsync(std::function<void()> onCompletion);

        /// Returns true if an asynchronous call is ready.
        bool                ready();

        /// Waits until an asynchronous call is ready.
        void                waitForReady(std::uint32_t timeoutMs = 0);

        /// Cancels an asynchronous call.
        void                cancel();

        /// Tests if an asynchronous call has resulted in an exception.
        bool                        hasAsyncException();

        /// Retrieves an asynchronous exception.
        std::unique_ptr<Exception>  getAsyncException();

        ///@}


        void                    wait(std::function<void()> onCompletion, std::uint32_t timeoutMs);
        
        void                    setTries(std::size_t tries);
        std::size_t             getTries() const;

#if RCF_FEATURE_FILETRANSFER==1

        // Legacy file transfer API.

        void                    setFileProgressCallback(FileProgressCallback fileProgressCb);
        void                    setFileProgressCallback() { setFileProgressCallback( FileProgressCallback() ); }
        void                    runFileProgressNotifications(FileTransferProgress& progress);

        void                    uploadFiles(
                                    const std::string & whichFile, 
                                    std::string & uploadId,
                                    std::uint32_t chunkSize,
                                    std::uint32_t transferRateBps,
                                    std::uint32_t sessionLocalId);

        void                    uploadFiles(
                                    const FileManifest & whichFile, 
                                    std::string & uploadId,
                                    std::uint32_t chunkSize,
                                    std::uint32_t transferRateBps,
                                    std::uint32_t sessionLocalId);

        void                    downloadFiles(
                                    const Path& downloadToPath,
                                    FileManifest & manifest,
                                    std::uint32_t chunkSize, 
                                    std::uint32_t transferRateBps,
                                    std::uint32_t sessionLocalId,
                                    const std::string & downloadId,
                                    std::uint64_t startPos = 0,
                                    std::uint64_t endPos = -1);

        std::uint32_t           addUploadStream(FileUpload fileStream);
        void                    processUploadStreams();

        std::uint32_t           addDownloadStream(FileDownload fileStream);

        // For testing.
        void                    setTransferWindowS(std::uint32_t transferWindowS);
        std::uint32_t           getTransferWindowS();
#endif


        FutureConverter<Void>        ping(const CallOptions & callOptions);

        FutureConverter<Void>        doControlMessage(
                                    const CallOptions &     callOptions, 
                                    ByteBuffer              controlRequest);

        //**********************************************************************

    public:

        std::size_t                     getPingBackCount();
        std::uint32_t                   getPingBackTimeStamp();
        
        void                            clearParameters();

        SerializationProtocolIn &       getSpIn();
        SerializationProtocolOut &      getSpOut();

        void                            setAsync(bool async);
        bool                            getAsync();

        void                            setAsyncException(std::unique_ptr<Exception>);

        std::uint32_t                   generatePollingTimeout(std::uint32_t timeoutMs);
        void                            onPollingTimeout(bool eventBased = false);
        void                            onUiMessage();

        void                            setSubRcfClientPtr(RcfClientPtr clientStubPtr);
        RcfClientPtr                    getSubRcfClientPtr();

        void                            setEnableNativeWstringSerialization(bool enable);
        bool                            getEnableNativeWstringSerialization() const;

        friend class CallOptions;

    private:

        friend class FutureConverterBase;
        friend class FutureImplBase;

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
        friend class AllocateClientParameters;

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
        friend class ClientParameters;

        template<typename T>
        friend class Future;

        void                enrol(
                                I_Future *pFuture);

        void                init( 
                                int fnId, 
                                RCF::RemoteCallMode rcs);

        void                beginCall();

        void                beginSend();

        void                beginReceive();

        void                call( 
                                RCF::RemoteCallMode rcs);

        void                onConnectCompleted(
                                bool alreadyConnected = false);

        void                setupTransportProtocol();
        void                onSetupTransportProtocolCompleted();

        void                doBatching();

        void                onSendCompleted();

        void                onReceiveCompleted();

        void                onTimerExpired();

        void                onError(
                                const std::exception &e);
   
        void                setAsyncCallback(
                                std::function<void()> callback);        

        void                onException(
                                const Exception & e);

        void                scheduleAmiNotification();

        void                createFilterSequence(std::vector<FilterPtr> & filters);

        void                setConnected(bool connected);

        virtual bool        isClientStub() const;

        

    public:
        // Old-style setting of transport and message filters. Leaving as public for now.

        // Synchronous transport filter requests.
        void                    requestTransportFilters(const std::vector<FilterPtr> &filters);
        void                    requestTransportFilters(FilterPtr filterPtr);
        void                    requestTransportFilters();

        void                    clearTransportFilters();

        // Asynchronous transport filter requests.
        void                    requestTransportFiltersAsync(
                                    const std::vector<FilterPtr> &  filters,
                                    std::function<void()>           onCompletion);

        void                    requestTransportFiltersAsync(
                                    FilterPtr                       filterPtr,
                                    std::function<void()>           onCompletion);

        void                    setMessageFilters(const std::vector<FilterPtr> &filters);
        void                    setMessageFilters();
        void                    setMessageFilters(FilterPtr filterPtr);

        const std::vector<FilterPtr> &getMessageFilters();

        std::vector<char> &     getRetValVec();


        void                    setupProxiedConnection(const std::string& proxyEndpointName);

    private:
        std::string                 mProxyEndpointName;

    private:

        RemoteCallMode              mRemoteCallMode;
        SerializationProtocol       mProtocol;
        std::string                 mEndpointName;
        std::string                 mServerBindingName;
        std::string                 mInterfaceName;

        unsigned int                mRemoteCallTimeoutMs;
        unsigned int                mConnectTimeoutMs;
        bool                        mAutoReconnect;
        bool                        mConnected;
        std::size_t                 mTries;

        EndpointPtr                 mEndpoint;
        ClientTransportUniquePtr    mTransport;

        std::vector<FilterPtr>      mMessageFilters;

        RemoteCallProgressCallback  mProgressCallback;
        unsigned int                mProgressCallbackIntervalMs = 0;


        bool                        mAutoVersioning;
        std::uint32_t               mRuntimeVersion;
        std::uint32_t               mArchiveVersion;

        bool                        mEnableSfPointerTracking;
        bool                        mEnableNativeWstringSerialization = false;

        std::vector<I_Future *>     mFutures;

        MethodInvocationRequest     mRequest;
        SerializationProtocolIn     mIn;
        SerializationProtocolOut    mOut;

        bool                        mAsync;
        AsyncOpType                 mAsyncOpType;
        std::function<void()>       mAsyncCallback;
        std::unique_ptr<Exception>  mAsyncException;
        unsigned int                mEndTimeMs;
        bool                        mRetry;
        RemoteCallMode              mRcs;
        ByteBuffer                  mEncodedByteBuffer;
        std::vector<ByteBuffer>     mEncodedByteBuffers;

        std::vector<char>           mRetValVec;
        std::vector<char>           mParametersVec;
        I_Parameters *              mpParameters;

        std::uint32_t               mPingBackIntervalMs;
        std::uint32_t               mPingBackTimeStamp;
        std::size_t                 mPingBackCount;

        std::uint32_t               mNextTimerCallbackMs;
        std::uint32_t               mNextPingBackCheckMs;
        std::uint32_t               mPingBackCheckIntervalMs;

        MutexPtr                    mSignalledMutexPtr;
        ConditionPtr                mSignalledConditionPtr;
        LockPtr                     mSignalledLockPtr;

        bool                        mSignalled;

        Mutex                       mSubRcfClientMutex;
        RcfClientPtr                mSubRcfClientPtr;

        bool                        mBatchMode;
        ReallocBufferPtr            mBatchBufferPtr;
        ReallocBuffer               mBatchBufferTemp;
        std::uint32_t               mBatchMaxMessageLength;
        std::uint32_t               mBatchCount;
        std::uint32_t               mBatchMessageCount;

        // Set to true if we need to set the transport protocol without disconnecting.
        bool                        mSetTransportProtocol;


#if RCF_FEATURE_FILETRANSFER==1
        FileProgressCallback              mFileProgressCb;

        std::vector<FileUpload>     mUploadStreams;
        std::vector<FileDownload>   mDownloadStreams;       
#endif

        std::uint32_t               mTransferWindowS;    

        RecursionState<int, int>    mRecursionState;

    public:

        std::string                 mCurrentCallMethodName;
        std::string                 mCurrentCallDesc;
        bool                        mCallInProgress;

#ifdef RCF_WINDOWS
    private:
        std::unique_ptr<HANDLE>                 mWindowsImpersonationToken;
#endif

    private:
        std::string                             mHttpProxy;
        int                                     mHttpProxyPort;
        tstring                                 mHttpProxyRealm;
        std::map<std::string, HttpCookie>       mHttpCookies;
        TransportProtocol                       mTransportProtocol;
        tstring                                 mUsername;
        tstring                                 mPassword;
        tstring                                 mHttpProxyUsername;
        tstring                                 mHttpProxyPassword;
        std::string                             mHttpUrlParameterString;
        tstring                                 mKerberosSpn;
        bool                                    mEnableCompression;
        SspiMessageProtection                   mSspiMessageProtection = Smp_Encryption;

        CertificatePtr                          mCertificatePtr;
        CertificatePtr                          mCaCertificatePtr;
        CertificateValidationCallback           mCertificateValidationCb;
        tstring                                 mSchannelCertificateValidation;

        std::string                             mOpenSslCipherSuite;

        tstring                                 mTlsSniName;

        SslImplementation                       mSslImplementation;

#if RCF_FEATURE_SSPI==1
        DWORD                                   mSchannelEnabledProtocols = 0;
        ULONG                                   mSchannelContextRequirements = 0;
#endif


        friend class HttpFrameFilter;
        std::map<std::string, HttpCookie> &     getCookieMap();

    private:
        void                    requestTransportFilters_Legacy(const std::vector<FilterPtr> &filters);

        void                    requestTransportFiltersAsync_Legacy(
                                    const std::vector<FilterPtr> &filters,
                                    std::function<void()> onCompletion);
    };

    class RCF_EXPORT CallOptions
    {
    public:
        CallOptions();
        CallOptions(RemoteCallMode rcs);
        CallOptions(RemoteCallMode rcs, const std::function<void()> & callback);
        CallOptions(std::function<void()> callback);
        RemoteCallMode apply(ClientStub &clientStub) const;

    private:
        bool                        mAsync;
        bool                        mRcsSpecified;
        RemoteCallMode              mRcs;
        std::function<void()>       mCallback;
    };

    class RCF_EXPORT AsyncTwoway : public CallOptions
    {
    public:
        AsyncTwoway(const std::function<void()> & callback);
    };

    class RCF_EXPORT AsyncOneway : public CallOptions
    {
    public:
        AsyncOneway(const std::function<void()> & callback);
    };

    class RestoreClientTransportGuard
    {
    public:

        RestoreClientTransportGuard(ClientStub &client, ClientStub &clientTemp);
        ~RestoreClientTransportGuard();

    private:
        ClientStub &mClient;
        ClientStub &mClientTemp;
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_CLIENTSTUB_HPP
