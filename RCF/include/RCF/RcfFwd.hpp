
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

#ifndef INCLUDE_RCF_RCFFWD_HPP
#define INCLUDE_RCF_RCFFWD_HPP

#include <functional>
#include <memory>
#include <vector>

#include <RCF/Enums.hpp>

namespace RCF
{

    class RcfServer;
    class RcfSession;
    class ClientTransport;
    class ServerTransport;
    class Endpoint;
    class I_RcfClient;
    typedef std::shared_ptr<I_RcfClient>        RcfClientPtr;
    typedef std::shared_ptr<RcfSession>         RcfSessionPtr;
    typedef std::weak_ptr<RcfSession>           RcfSessionWeakPtr;

    /// Unique pointer wrapper for RCF::ClientTransport.
    typedef std::unique_ptr<ClientTransport>    ClientTransportUniquePtr;

    /// Unique pointer wrapper for RCF::ServerTransport.
    typedef std::shared_ptr<ServerTransport>    ServerTransportPtr;

    class Exception;
    typedef std::shared_ptr<Exception> ExceptionPtr;

    template<typename T> class RcfClient;
    class I_RequestSubscription;
    class I_Null;

    class                                           PublisherBase;
    typedef std::shared_ptr<PublisherBase>          PublisherPtr;
    typedef std::weak_ptr<PublisherBase>            PublisherWeakPtr;

    class                                           PublishingService;

    class SubscriptionService;
    class SubscriptionParms;
    class Subscription;
    typedef std::shared_ptr<Subscription>           SubscriptionPtr;
    typedef std::weak_ptr<Subscription>             SubscriptionWeakPtr;

    typedef std::shared_ptr<Subscription>           SubscriptionPtr;
    typedef std::weak_ptr<Subscription>             SubscriptionWeakPtr;

    /// Describes a user-provided callback function to be called on the publisher side, whenever a subscriber connects to a publisher.
    typedef std::function<bool(RcfSession &, const std::string &)>          OnSubscriberConnect;

    /// Describes a user-provided callback function to be called on the publisher side, whenever a subscriber is disconnected from a publisher.
    typedef std::function<void(RcfSession &, const std::string &)>          OnSubscriberDisconnect;

    /// Describes a user-provided callback function to be called on the subscriber side, whenever a subscriber is disconnected from a publisher.
    typedef std::function<void(RcfSession &)>                               OnSubscriptionDisconnect;

    /// Describes a user-provided callback function to be called on the subscriber side, when an subscription is established asynchronously.
    typedef std::function<void(SubscriptionPtr, ExceptionPtr)>              OnAsyncSubscribeCompleted;

    enum RemoteCallAction;
    class RemoteCallProgressInfo;

    /// Describes a user-provided callback function to be called periodically on the client side, while a remote call is in progress.
    typedef std::function<void(const RemoteCallProgressInfo&, RemoteCallAction&)>  RemoteCallProgressCallback;

    template<typename T>
    class Future;

    template<typename T>
    class FutureConverter;

    struct Void;
    class CallOptions;
    class ConnectionResetGuard;
    class I_Parameters;
    class Certificate;
    class ClientStub;
    class ClientProgress;
    class I_Future;
    class IpClientTransport;
    class FileTransferProgress;
    class OpenSslEncryptionFilter;
    class SspiFilter;
    class OverlappedAmi;
    class FileManifest;
    class Filter;

    /// Reference counted wrapper for RCF::Certificate.
    typedef std::shared_ptr<Certificate>                        CertificatePtr;

    /// Describes user-provided callback functions for validating a certificate.
    typedef std::function<bool(Certificate *)>                  CertificateValidationCallback;
    
    /// Reference counted wrapper for RCF::Endpoint.
    typedef std::shared_ptr<Endpoint>                           EndpointPtr;

    typedef std::shared_ptr<Filter>                             FilterPtr;
    typedef std::shared_ptr<ClientStub>                         ClientStubPtr;
    typedef std::shared_ptr<ClientProgress>                     ClientProgressPtr;
    typedef std::shared_ptr<OverlappedAmi>                      OverlappedAmiPtr;

    class FileDownloadInfo;
    class FileUploadInfo;

    class BandwidthQuota;
    typedef std::shared_ptr<BandwidthQuota> BandwidthQuotaPtr;

    /// Describes user-provided callback functions for client-side monitoring of a file transfer (download or upload).
    typedef std::function<void(const FileTransferProgress &, RemoteCallAction&)>       FileProgressCallback;

    /// Describes user-provided callback functions for server-side monitoring of a file download.
    typedef std::function<void(RcfSession&, FileDownloadInfo &)>    DownloadProgressCallback;

    /// Describes user-provided callback functions for server-side monitoring of a file upload.
    typedef std::function<void(RcfSession&, FileUploadInfo &)>      UploadProgressCallback;

    /// Describes user-provided callback functions for assigning custom bandwidth quotas to a RcfSession.
    typedef std::function<BandwidthQuotaPtr(RcfSession &)>          BandwidthQuotaCallback;

    /// Describes user-provided callback functions for assigning custom bandwidth quotas to a RcfSession.
    typedef BandwidthQuotaCallback                                  UploadBandwidthQuotaCallback;

    /// Describes user-provided callback functions for assigning custom bandwidth quotas to a RcfSession.
    typedef BandwidthQuotaCallback                                  DownloadBandwidthQuotaCallback;


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
    class AllocateClientParameters;

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
    class ClientParameters;

    class I_Service;
    class IpServerTransport;
    class PingBackService;
    class FileTransferService;
    class FilterService;
    class SessionTimeoutService;
    class PublishingService;
    class CallbackConnectionService;
    class ServerObjectService;
    class ProxyEndpointService;
    
    typedef std::shared_ptr<I_Service>                      ServicePtr;
    typedef std::shared_ptr<PingBackService>                PingBackServicePtr;
    typedef std::shared_ptr<FileTransferService>            FileTransferServicePtr;
    typedef std::shared_ptr<FilterService>                  FilterServicePtr;
    typedef std::shared_ptr<SessionTimeoutService>          SessionTimeoutServicePtr;
    typedef std::shared_ptr<PublishingService>              PublishingServicePtr;
    typedef std::shared_ptr<SubscriptionService>            SubscriptionServicePtr;
    typedef std::shared_ptr<CallbackConnectionService>      CallbackConnectionServicePtr;
    typedef std::shared_ptr<ServerObjectService>            ServerObjectServicePtr;
    typedef std::shared_ptr<ProxyEndpointService>           ProxyEndpointServicePtr;

    template<typename Interface>
    class Publisher;

    class BandwidthQuota;

    /// Reference counted wrapper for RCF::BandwidthQuota.
    typedef std::shared_ptr<BandwidthQuota> BandwidthQuotaPtr;

    class FileDownloadInfo;
    class FileUploadInfo;

    class JsonRpcRequest;
    class JsonRpcResponse;

    class HttpSession;
    typedef std::shared_ptr<HttpSession> HttpSessionPtr;

    class HttpCookie;

    typedef std::function<void(RcfSessionPtr, ClientTransportUniquePtr)> OnCallbackConnectionCreated;

    typedef RcfSessionPtr                       SessionPtr;

    enum TransportProtocol;
    enum SslImplementation;
    enum TransportType;
    enum CertificateImplementationType;

    class PublisherParms;

    class I_CreateCallbackConnection;

    typedef std::shared_ptr<ClientTransport> ClientTransportPtr;

    typedef std::shared_ptr< ClientTransportUniquePtr > ClientTransportUniquePtrPtr;

    typedef std::unique_ptr<ServerTransport> ServerTransportUniquePtr;


    class ServerBinding;

    typedef std::shared_ptr<ServerBinding> ServerBindingPtr;

    class MethodInvocationRequest;

    class NetworkSession;
    class ByteBuffer;

    class Win32Certificate;

    /// Reference counted wrapper for RCF::Win32Certificate.
    typedef std::shared_ptr<Win32Certificate> Win32CertificatePtr;

    class X509Certificate;
    typedef std::shared_ptr<X509Certificate> X509CertificatePtr;

    class UdpServerTransport;
    class UdpNetworkSession;

    class FileStreamImpl;

    typedef std::shared_ptr<FileUploadInfo>   FileUploadInfoPtr;
    typedef std::shared_ptr<FileDownloadInfo> FileDownloadInfoPtr;

    typedef std::pair<std::uint32_t, RcfSessionWeakPtr>   PingBackTimerEntry;

    class RemoteAddress;
    class RemoteCallInfo;
    class FileStream;
    class FileUpload;
    class FileDownload;

    class AsioNetworkSession;

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
    class AllocateServerParameters;

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
    class ServerParameters;

    class ServerBinding;

    /// Reference counted wrapper for RCF::ServerBinding.
    typedef std::shared_ptr<ServerBinding> ServerBindingPtr;

    /// Describes a user-provided function for determining whether a client connections should be able to access a server binding.
    typedef std::function<bool(int)> AccessControlCallback;

    // List of HTTP headers in a HTTP message.
    typedef std::vector< std::pair< std::string, std::string > > HttpHeaderList;

    /// Describes a user-provided function for handling HTTP redirect responses when connecting to a HTTP/HTTPS server.
    typedef std::function<void(const std::string & statusLine, const HttpHeaderList& headerList)> HttpRedirectHandler;

    class HttpMessageVerifier;

    /// Reference counted wrapper for RCF::HttpMessageVerifier.
    typedef std::shared_ptr<HttpMessageVerifier> HttpMessageVerifierPtr;

} // namespace RCF

#endif // ! INCLUDE_RCF_RCFFWD_HPP
