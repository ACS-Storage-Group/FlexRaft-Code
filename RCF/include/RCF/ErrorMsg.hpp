
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

#ifndef INCLUDE_RCF_ERRORMSG_HPP
#define INCLUDE_RCF_ERRORMSG_HPP

#include <string>

#include <RCF/Export.hpp>

namespace RCF
{
    class RCF_EXPORT ErrorMsg
    {
    public:
        explicit ErrorMsg(int errorId);
        int getErrorId() const;
        std::string getLiteral() const;
    private:
        int mErrorId = 0;
    };

    /*[[[cog
    import cog
    import os
    import sys

    scriptDir = os.path.dirname(cog.inFile)
    pyDir = os.path.abspath( os.path.join(scriptDir, '..\\..\\..\\scripts'))
    sys.path.insert(0, pyDir)

    import ErrorMsg

    errorMessages = ErrorMsg.loadErrorMessages()
    
    cog.outl()
    
    for (errorId, errorNbr, errorMsg, maxArg) in errorMessages:
        cog.outl('#define {} ErrorMsg({}) // {}'.format(errorId.ljust(40), str(errorNbr).rjust(3), errorMsg))
    
    cog.outl()

    for (errorId, errorNbr, errorMsg, maxArg) in errorMessages:
        cog.outl('static const int {} = {};'.format( (errorId+'_Id').ljust(40), str(errorNbr).rjust(3) ))
    
    cog.outl()

    ]]]*/

    #define RcfError_Ok                              ErrorMsg(  0) // No error.
    #define RcfError_ServerMessageLength             ErrorMsg(  2) // Server-side message length error.
    #define RcfError_ClientMessageLength             ErrorMsg(  3) // Client-side message length error.
    #define RcfError_Serialization                   ErrorMsg(  4) // Data serialization error. Type name: %1%. Exception type: %2%. Exception message: '%3%'.
    #define RcfError_Deserialization                 ErrorMsg(  5) // Data deserialization error. Type name: %1%. Exception type: %2%. Exception message: '%3%'.
    #define RcfError_AppException                    ErrorMsg(  6) // Server-side application exception. Exception type: %1%. Exception message: '%2%'.
    #define RcfError_UnknownEndpoint                 ErrorMsg(  8) // Unknown endpoint.
    #define RcfError_EndpointPassword                ErrorMsg(  9) // Incorrect endpoint password.
    #define RcfError_EndpointDown                    ErrorMsg( 10) // Endpoint unavailable.
    #define RcfError_EndpointRetry                   ErrorMsg( 11) // Endpoint temporarily unavailable (try again).
    #define RcfError_ClientConnectTimeout            ErrorMsg( 16) // Network connection to %2% timed out after %1% ms (server not started?).
    #define RcfError_PeerDisconnect                  ErrorMsg( 17) // Unexpected peer disconnection.
    #define RcfError_ClientCancel                    ErrorMsg( 18) // Remote call canceled by client.
    #define RcfError_StubAssignment                  ErrorMsg( 19) // Incompatible stub assignment.
    #define RcfError_PayloadFilterMismatch           ErrorMsg( 20) // Message filter mismatch.
    #define RcfError_OpenSslFilterInit               ErrorMsg( 21) // Failed to initialize OpenSSL filter. OpenSSL error: %1%
    #define RcfError_OpenSslLoadCert                 ErrorMsg( 22) // Failed to load OpenSSL certificate file. File: %1%. OpenSSL error: %2%
    #define RcfError_UnknownPublisher                ErrorMsg( 23) // Unknown publishing topic.
    #define RcfError_UnknownFilter                   ErrorMsg( 24) // Unknown filter type.
    #define RcfError_NoServerStub                    ErrorMsg( 25) // Server-side binding not found. Binding: %1%.
    #define RcfError_Sspi                            ErrorMsg( 26) // SSPI error. Call to SSPI function %1% failed(). %2%
    #define RcfError_SspiAuthFail                    ErrorMsg( 27) // SSPI authentication failed.
    #define RcfError_SspiInit                        ErrorMsg( 28) // Failed to initialize SSPI filter. %1% %2%
    #define RcfError_UnknownSubscriber               ErrorMsg( 29) // Unknown subscriber.
    #define RcfError_ClientReadTimeout               ErrorMsg( 30) // Remote call timeout exceeded. No response from peer.
    #define RcfError_ClientReadFail                  ErrorMsg( 31) // Network connection error while waiting for response. %1%
    #define RcfError_ClientWriteTimeout              ErrorMsg( 32) // Remote call timeout exceeded. Unable to send request.
    #define RcfError_ClientWriteFail                 ErrorMsg( 33) // Network connection error while sending request. %1%
    #define RcfError_ClientConnectFail               ErrorMsg( 34) // Unable to establish network connection. %1%
    #define RcfError_Filter                          ErrorMsg( 35) // Filter error.
    #define RcfError_Socket                          ErrorMsg( 36) // Socket error. Call to OS function %1% failed. %2%
    #define RcfError_FnId                            ErrorMsg( 37) // Invalid function id. Function id: %1%
    #define RcfError_UnknownInterface                ErrorMsg( 38) // Unknown object interface. Interface: %1%.
    #define RcfError_NoEndpoint                      ErrorMsg( 39) // No endpoint.
    #define RcfError_TransportCreation               ErrorMsg( 40) // Failed to create transport.
    #define RcfError_FilterCount                     ErrorMsg( 41) // Invalid number of filters. Requested: %1%. Max allowed: %2%.
    #define RcfError_FilterMessage                   ErrorMsg( 42) // Failed to filter message.
    #define RcfError_UnfilterMessage                 ErrorMsg( 43) // Failed to unfilter message.
    #define RcfError_SspiCredentials                 ErrorMsg( 44) // SSPI credentials failure. Call to SSPI function %1% failed().
    #define RcfError_SspiEncrypt                     ErrorMsg( 45) // SSPI encryption failure. Call to SSPI function %1% failed(). %2%
    #define RcfError_SspiDecrypt                     ErrorMsg( 46) // SSPI decryption failure. Call to SSPI function %1% failed(). %2%
    #define RcfError_SspiImpersonation               ErrorMsg( 47) // SSPI impersonation failure. Call to SSPI function %1% failed(). %2%
    #define RcfError_NotConnected                    ErrorMsg( 48) // Send operation attempted without connecting.
    #define RcfError_SocketClose                     ErrorMsg( 49) // Failed to close socket. %1%
    #define RcfError_ZlibDeflate                     ErrorMsg( 50) // Zlib compression error. %1%
    #define RcfError_ZlibInflate                     ErrorMsg( 51) // Zlib decompression error. %1%
    #define RcfError_Zlib                            ErrorMsg( 52) // Call to Zlib function '%1%' failed. %2%
    #define RcfError_UnknownSerializationProtocol    ErrorMsg( 53) // Unknown serialization protocol. Protocol: %1%.
    #define RcfError_InvalidErrorMessage             ErrorMsg( 54) // Invalid error message from server.
    #define RcfError_SfNoCtor                        ErrorMsg( 55) // Construction not supported for this type.
    #define RcfError_SfRefMismatch                   ErrorMsg( 56) // Can't deserialize a reference into a non-reference object.
    #define RcfError_SfDataFormat                    ErrorMsg( 57) // Input data format error.
    #define RcfError_SfReadFailure                   ErrorMsg( 58) // Failed to read data from underlying stream.
    #define RcfError_SfWriteFailure                  ErrorMsg( 59) // Failed to write data to underlying stream.
    #define RcfError_SfBaseDerivedRegistration       ErrorMsg( 60) // Base/derived pair not registered. Base: %1%. Derived: %2%.
    #define RcfError_SfTypeRegistration              ErrorMsg( 61) // Type not registered. Type: %1%.
    #define RcfError_NonStdException                 ErrorMsg( 62) // Non std::exception-derived exception was thrown.
    #define RcfError_SocketBind                      ErrorMsg( 63) // Failed to bind socket to port (port already in use?). Network interface: %1%. Port: %2%. Error: %3%
    #define RcfError_Decoding                        ErrorMsg( 64) // Decoding error.
    #define RcfError_Encoding                        ErrorMsg( 65) // Encoding error.
    #define RcfError_TokenRequestFailed              ErrorMsg( 66) // No tokens available.
    #define RcfError_ObjectFactoryNotFound           ErrorMsg( 67) // Object factory not found.
    #define RcfError_PortInUse                       ErrorMsg( 68) // Port already in use. Network interface: %1%. Port: %2%.
    #define RcfError_DynamicObjectNotFound           ErrorMsg( 69) // Server-side object for given token not found. Token id: %1%.
    #define RcfError_VersionMismatch                 ErrorMsg( 70) // Version mismatch.
    #define RcfError_SslCertVerification             ErrorMsg( 72) // SSL certificate verification failure. OpenSSL error: %1%
    #define RcfError_FiltersLocked                   ErrorMsg( 74) // Filters locked.
    #define RcfError_Pipe                            ErrorMsg( 75) // Pipe error. %1%
    #define RcfError_AnySerializerNotFound           ErrorMsg( 76) // Boost.Any serialization error: serializer not registered for the type '%1%'. Use SF::registerAny() to register a serializer.
    #define RcfError_ConnectionLimitExceeded         ErrorMsg( 77) // The server has reached its incoming connection limit.
    #define RcfError_DeserializationNullPointer      ErrorMsg( 78) // Null pointer deserialization error.
    #define RcfError_PipeNameTooLong                 ErrorMsg( 79) // Pipe name too long. Pipe name: %1%. Max length: %2%.
    #define RcfError_PingBack                        ErrorMsg( 80) // Received ping back message from peer.
    #define RcfError_NoPingBackService               ErrorMsg( 81) // A ping back service is not available on the server.
    #define RcfError_NoDownload                      ErrorMsg( 82) // The specified download does not exist.
    #define RcfError_FileOffset                      ErrorMsg( 83) // The specified file offset is invalid. Expected offset: %1%. Actual offset: %2%.
    #define RcfError_NoUpload                        ErrorMsg( 84) // The specified upload does not exist.            
    #define RcfError_FileOpen                        ErrorMsg( 85) // Failed to open file. Path: %1%. %2%
    #define RcfError_FileRead                        ErrorMsg( 86) // Failed to read from file. Path: %1%. %2%.
    #define RcfError_FileWrite                       ErrorMsg( 87) // Failed to write to file. Path: %1%. %2%.
    #define RcfError_UploadFailed                    ErrorMsg( 88) // Upload failed to complete.
    #define RcfError_UploadInProgress                ErrorMsg( 89) // Upload still in progress.
    #define RcfError_ConcurrentUpload                ErrorMsg( 90) // Cannot upload on several connections simultaneously.
    #define RcfError_UploadFileSize                  ErrorMsg( 91) // File upload exceeding size limit.
    #define RcfError_AccessDenied                    ErrorMsg( 92) // Access denied.
    #define RcfError_PingBackTimeout                 ErrorMsg( 93) // Failed to receive pingbacks from server. Expected pingback interval (ms): %1%.
    #define RcfError_AllThreadsBusy                  ErrorMsg( 94) // All server threads are busy.
    #define RcfError_UnsupportedRuntimeVersion       ErrorMsg( 95) // Unsupported RCF runtime version. Requested version: %1%. Max supported version: %2%.
    #define RcfError_FdSetSize                       ErrorMsg( 97) // FD_SETSIZE limit exceeded. Maximum: %1%.
    #define RcfError_DnsLookup                       ErrorMsg( 98) // DNS lookup of network address failed, for name '%1%'. %2%
    #define RcfError_SspiHandshakeExtraData          ErrorMsg( 99) // SSPI handshake protocol error (extra data).
    #define RcfError_ProtobufWrite                   ErrorMsg(101) // Failed to serialize Protocol Buffer object. Type: %1%.
    #define RcfError_ProtobufRead                    ErrorMsg(102) // Failed to deserialize Protocol Buffer object. Type: %1%.
    #define RcfError_ExtractSlice                    ErrorMsg(103) // Failed to read from marshaling buffer. Position: %1%. Length: %2%. Marshaling buffer size: %3%.
    #define RcfError_ServerStubExpired               ErrorMsg(104) // Server stub no longer available
    #define RcfError_VariantDeserialization          ErrorMsg(105) // Failed to deserialize variant object. Variant index: %1%. Variant size: %2%.
    #define RcfError_SspiAuthFailServer              ErrorMsg(106) // Server-side SSPI authentication failed.
    #define RcfError_SspiAuthFailClient              ErrorMsg(107) // Client-side SSPI authentication failed. %1%
    #define RcfError_Win32ApiError                   ErrorMsg(108) // The Win32 function %1% failed. %2%
    #define RcfError_SspiLengthField                 ErrorMsg(109) // SSPI invalid format. Packet length exceeds total message length. Packet length: %1%. Message length: %2%.
    #define RcfError_DownloadFailed                  ErrorMsg(110) // Download failed to complete. %1%
    #define RcfError_FileSeek                        ErrorMsg(111) // Failed to seek in file. File: %1%. Seek offset: %2%. %3%
    #define RcfError_DownloadCancelled               ErrorMsg(112) // Download canceled by server.
    #define RcfError_ParseSockAddr                   ErrorMsg(113) // Unable to parse socket address structure. %1%
    #define RcfError_GetSockName                     ErrorMsg(114) // Unable to retrieve local address information from socket. %1%
    #define RcfError_ProtobufWriteSize               ErrorMsg(115) // Unable to determine serialized size of Protocol Buffers object. ByteSize() returned 0. Type: %1%.
    #define RcfError_ProtobufWriteInit               ErrorMsg(116) // Failed to serialize Protocol Buffers object. Not all required fields were initialized. Type: %1%.
    #define RcfError_ArraySizeMismatch               ErrorMsg(117) // Array size mismatch while deserializing. Array size: %1%. Array size from archive: %2%.
    #define RcfError_WcharSizeMismatch               ErrorMsg(118) // Unicode character size mismatch. Native wchar_t size: %1%. wchar_t size from archive: %2%.
    #define RcfError_AnyTypeNotRegistered            ErrorMsg(119) // Boost.Any serialization error: a type name has not been registered for the typeid '%1%'. Use SF::registerType() to register a type name.
    #define RcfError_CryptoApiError                  ErrorMsg(120) // CryptoAPI error. Call to CryptoAPI function %1% failed. %2%
    #define RcfError_ServerStubAccessDenied          ErrorMsg(121) // Access to server binding was denied.
    #define RcfError_ApiError                        ErrorMsg(122) // Call to %1% failed. %2%
    #define RcfError_HttpProxyPort                   ErrorMsg(123) // HTTP proxy port was not specified.
    #define RcfError_OpenSslError                    ErrorMsg(124) // OpenSSL error: %1%
    #define RcfError_ProtocolNotSupported            ErrorMsg(125) // The requested transport protocol is not supported by this server.
    #define RcfError_ClearCommunicationNotAllowed    ErrorMsg(126) // Server requires one of the following transport protocols to be used: %1%.
    #define RcfError_ThreadingError                  ErrorMsg(127) // Threading error. Call to %1% failed.
    #define RcfError_RcfNotInitialized               ErrorMsg(128) // RCF has not been initialized. Use the RCF::RcfInitDenit class, or call RCF::init() directly.
    #define RcfError_InvalidHttpMessage              ErrorMsg(129) // Invalid HTTP message.
    #define RcfError_HttpRequestContentLength        ErrorMsg(130) // Unexpected HTTP request. Content-Length header was not present.
    #define RcfError_HttpResponseContentLength       ErrorMsg(131) // Unexpected HTTP response. Content-Length header was not present. HTTP status: %1%. HTTP response: %2%
    #define RcfError_InvalidOpenSslCertificate       ErrorMsg(132) // Invalid certificate format. OpenSSL-based SSL implementation requires certificate to be in PEM format and loaded with the RCF::PemCertificate class.
    #define RcfError_InvalidSchannelCertificate      ErrorMsg(133) // Invalid certificate format. Schannel-based SSL implementation requires certificate to be loaded with the RCF::PfxCertificate or RCF::StoreCertificate classes.
    #define RcfError_HttpConnectFailed               ErrorMsg(134) // Failed to connect via HTTPS proxy. HTTP CONNECT request to proxy failed. HTTP status: %1%. HTTP response: %2%
    #define RcfError_SspiImpersonateNoSspi           ErrorMsg(135) // Unable to impersonate client. Impersonation requires one of the following transport protocols: NTLM, Kerberos, Negotiate.
    #define RcfError_TransportProtocolNotSupported   ErrorMsg(136) // The requested transport protocol is not supported by this RCF build. Transport protocol: %1%
    #define RcfError_SslNotSupported                 ErrorMsg(137) // SSL transport protocol is not supported by this RCF build.
    #define RcfError_SessionObjectDoesNotExist       ErrorMsg(138) // Session object does not exist. Session object type: %1%
    #define RcfError_UploadAlreadyCompleted          ErrorMsg(139) // The specified upload has already been completed.
    #define RcfError_FileIndex                       ErrorMsg(140) // The specified file index is invalid. Expected index: %1%. Actual index: %2%.
    #define RcfError_ConcurrentCalls                 ErrorMsg(141) // Error: multiple concurrent calls attempted on the same RcfClient<> object. To make concurrent calls, use multiple RcfClient<> objects instead.
    #define RcfError_ParseJsonRpcRequest             ErrorMsg(142) // Unable to parse JSON-RPC request. json_spirit::read_stream() returned false.
    #define RcfError_DllLoad                         ErrorMsg(143) // Unable to load library. Library name: %1%. Error: %2%
    #define RcfError_DllFuncLoad                     ErrorMsg(144) // Unable to load function from dynamic library. Library name: %1%. Function name: %2%. Error: %3%
    #define RcfError_UnixDllLoad                     ErrorMsg(145) // Unable to load library. Library name: %1%. Error: %2%
    #define RcfError_UnixDllFuncLoad                 ErrorMsg(146) // Unable to load function from dynamic library. Library name: %1%. Function name: %2%. Error: %3%
    #define RcfError_PingBackInterval                ErrorMsg(147) // Invalid ping back interval. Ping back interval must be at least %2% ms. Requested ping back interval was %1% ms.
    #define RcfError_FileOpenWrite                   ErrorMsg(148) // Unable to open file for writing. File path: %1%
    #define RcfError_CustomCertValidation            ErrorMsg(149) // Certificate could not be validated. Error: %1%
    #define RcfError_SupportedOnWindowsOnly          ErrorMsg(150) // %1% is only supported on Windows platforms.
    #define RcfError_NotSupportedOnWindows           ErrorMsg(151) // %1% is not supported on Windows platforms.
    #define RcfError_NotSupportedInThisBuild         ErrorMsg(152) // %1% is not supported in this RCF build.
    #define RcfError_NoLongerSupported               ErrorMsg(153) // %1% is no longer supported in this version of RCF.
    #define RcfError_SslCertVerificationCustom       ErrorMsg(154) // SSL certificate verification failure.
    #define RcfError_ServerCallbacksNotSupported     ErrorMsg(155) // The server has not been configured to accept callback connections.
    #define RcfError_ServerUnsupportedFeature        ErrorMsg(156) // The server does not support this feature. Feature: %1%.
    #define RcfError_SyncPublishError                ErrorMsg(157) // Synchronous error while sending to subscriber. Error: %1%.
    #define RcfError_DeserializeVectorBool           ErrorMsg(158) // Bit count mismatch on deserialization of vector<bool>. Bit count: %1%. Buffer size: %2%.
    #define RcfError_HttpTunnelError                 ErrorMsg(159) // HTTP tunnel error. %1%
    #define RcfError_HttpSessionTimeout              ErrorMsg(160) // HTTP session was not found on server.
    #define RcfError_HttpRequestSessionIndex         ErrorMsg(161) // HTTP session index mismatch on request. Expected index: %1% . Actual index: %2% .
    #define RcfError_HttpResponseStatus              ErrorMsg(162) // HTTP response error. HTTP response status: %1% . HTTP response: %2%
    #define RcfError_HttpResponseSessionIndex        ErrorMsg(163) // HTTP session index mismatch on response. Expected index: %1% . Actual index: %2% .
    #define RcfError_HttpResponseSessionId           ErrorMsg(164) // HTTP session ID mismatch on response. Expected ID: %1% . Actual ID: %2% .
    #define RcfError_NotHttpResponse                 ErrorMsg(165) // The server responded with a non-HTTP response. The first bytes of the response were: '%1%'.
    #define RcfError_NotHttpPostRequest              ErrorMsg(166) // This HTTP port only supports HTTP POST requests.
    #define RcfError_NotHttpRequest                  ErrorMsg(167) // The server received a non-HTTP request.
    #define RcfError_NotSslHandshake                 ErrorMsg(168) // Protocol mismatch. Expected SSL handshake.
    #define RcfError_ClientStubParms                 ErrorMsg(169) // Unable to allocate client stub parameter structure.
    #define RcfError_ServerStubParms                 ErrorMsg(170) // Unable to allocate server stub parameter structure
    #define RcfError_SessionObjectNotCreated         ErrorMsg(171) // Could not create session object.Session object type: %1%
    #define RcfError_MessageHeaderEncoding           ErrorMsg(172) // Message header encoding error. Maximum length: %1% . Actual length: %2%
    #define RcfError_OnewayHttp                      ErrorMsg(173) // Oneway calls are not supported over the HTTP and HTTPS transports.
    #define RcfError_ProxyAuthRetry                  ErrorMsg(174) // Proxy authentication is required. Reconnecting to proxy.
    #define RcfError_ProxyCredentialsNeeded          ErrorMsg(175) // Proxy authentication is required. Please supply valid credentials.
    #define RcfError_ProxyCredentialsInvalid         ErrorMsg(176) // Unable to authenticate to proxy. Invalid credentials.
    #define RcfError_UploadDirectory                 ErrorMsg(177) // RCF upload directory has not been configured on the server.
    #define RcfError_NotHttpConnection               ErrorMsg(178) // This connection does not use HTTP or HTTPS.
    #define RcfError_ProxyEndpointsNotEnabled        ErrorMsg(179) // Proxy endpoints are not enabled on this server.
    #define RcfError_ProxyEndpointDown               ErrorMsg(180) // Proxy endpoint '%1%' is currently not available.
    #define RcfError_BSerFileTransferNotSupported    ErrorMsg(181) // File transfers not supported when using Boost.Serialization as serialization protocol.
    #define RcfError_CouldNotFindUpload              ErrorMsg(182) // No file upload found for upload ID '%1%'.
    #define RcfError_ClientUninitialized             ErrorMsg(183) // RCF client has not been initialized.
    #define RcfError_OutgoingMessageLen              ErrorMsg(184) // Outgoing message exceeded the maximum allowed length. Message length: %1%. Maximum allowed length: %2%.
    #define RcfError_NonSSLResponse                  ErrorMsg(185) // The server responded with a non-SSL response. The first bytes of the response were: '%1%'.
    #define RcfError_NoProxyConnection               ErrorMsg(186) // Unable to establish connection to proxy endpoint '%1%'.
    #define RcfError_ProxyServerMultiThreaded        ErrorMsg(187) // Proxy server must be multi-threaded, in order to service proxy requests.
    #define RcfError_HttpRedirect                    ErrorMsg(188) // The server returned a HTTP redirect response. The HTTP response was '%1%'.
    #define RcfError_SetFileModTime                  ErrorMsg(189) // Unable to set last modified time for file '%1%'. %2%
    #define RcfError_GetFileModTime                  ErrorMsg(190) // Unable to retrieve last modified time for file '%1%'. %2%
    #define RcfError_HttpMessageVerification         ErrorMsg(191) // HTTP message verification failed. 
    #define RcfError_HttpMessageVerificationAdmin    ErrorMsg(192) // HTTP message verification failed. %1%
    #define RcfError_HttpSessionNotAvailable         ErrorMsg(193) // HTTP session not available.
    #define RcfError_HttpInvalidMessage              ErrorMsg(194) // Invalid HTTP message.

    static const int RcfError_Ok_Id                           =   0;
    static const int RcfError_ServerMessageLength_Id          =   2;
    static const int RcfError_ClientMessageLength_Id          =   3;
    static const int RcfError_Serialization_Id                =   4;
    static const int RcfError_Deserialization_Id              =   5;
    static const int RcfError_AppException_Id                 =   6;
    static const int RcfError_UnknownEndpoint_Id              =   8;
    static const int RcfError_EndpointPassword_Id             =   9;
    static const int RcfError_EndpointDown_Id                 =  10;
    static const int RcfError_EndpointRetry_Id                =  11;
    static const int RcfError_ClientConnectTimeout_Id         =  16;
    static const int RcfError_PeerDisconnect_Id               =  17;
    static const int RcfError_ClientCancel_Id                 =  18;
    static const int RcfError_StubAssignment_Id               =  19;
    static const int RcfError_PayloadFilterMismatch_Id        =  20;
    static const int RcfError_OpenSslFilterInit_Id            =  21;
    static const int RcfError_OpenSslLoadCert_Id              =  22;
    static const int RcfError_UnknownPublisher_Id             =  23;
    static const int RcfError_UnknownFilter_Id                =  24;
    static const int RcfError_NoServerStub_Id                 =  25;
    static const int RcfError_Sspi_Id                         =  26;
    static const int RcfError_SspiAuthFail_Id                 =  27;
    static const int RcfError_SspiInit_Id                     =  28;
    static const int RcfError_UnknownSubscriber_Id            =  29;
    static const int RcfError_ClientReadTimeout_Id            =  30;
    static const int RcfError_ClientReadFail_Id               =  31;
    static const int RcfError_ClientWriteTimeout_Id           =  32;
    static const int RcfError_ClientWriteFail_Id              =  33;
    static const int RcfError_ClientConnectFail_Id            =  34;
    static const int RcfError_Filter_Id                       =  35;
    static const int RcfError_Socket_Id                       =  36;
    static const int RcfError_FnId_Id                         =  37;
    static const int RcfError_UnknownInterface_Id             =  38;
    static const int RcfError_NoEndpoint_Id                   =  39;
    static const int RcfError_TransportCreation_Id            =  40;
    static const int RcfError_FilterCount_Id                  =  41;
    static const int RcfError_FilterMessage_Id                =  42;
    static const int RcfError_UnfilterMessage_Id              =  43;
    static const int RcfError_SspiCredentials_Id              =  44;
    static const int RcfError_SspiEncrypt_Id                  =  45;
    static const int RcfError_SspiDecrypt_Id                  =  46;
    static const int RcfError_SspiImpersonation_Id            =  47;
    static const int RcfError_NotConnected_Id                 =  48;
    static const int RcfError_SocketClose_Id                  =  49;
    static const int RcfError_ZlibDeflate_Id                  =  50;
    static const int RcfError_ZlibInflate_Id                  =  51;
    static const int RcfError_Zlib_Id                         =  52;
    static const int RcfError_UnknownSerializationProtocol_Id =  53;
    static const int RcfError_InvalidErrorMessage_Id          =  54;
    static const int RcfError_SfNoCtor_Id                     =  55;
    static const int RcfError_SfRefMismatch_Id                =  56;
    static const int RcfError_SfDataFormat_Id                 =  57;
    static const int RcfError_SfReadFailure_Id                =  58;
    static const int RcfError_SfWriteFailure_Id               =  59;
    static const int RcfError_SfBaseDerivedRegistration_Id    =  60;
    static const int RcfError_SfTypeRegistration_Id           =  61;
    static const int RcfError_NonStdException_Id              =  62;
    static const int RcfError_SocketBind_Id                   =  63;
    static const int RcfError_Decoding_Id                     =  64;
    static const int RcfError_Encoding_Id                     =  65;
    static const int RcfError_TokenRequestFailed_Id           =  66;
    static const int RcfError_ObjectFactoryNotFound_Id        =  67;
    static const int RcfError_PortInUse_Id                    =  68;
    static const int RcfError_DynamicObjectNotFound_Id        =  69;
    static const int RcfError_VersionMismatch_Id              =  70;
    static const int RcfError_SslCertVerification_Id          =  72;
    static const int RcfError_FiltersLocked_Id                =  74;
    static const int RcfError_Pipe_Id                         =  75;
    static const int RcfError_AnySerializerNotFound_Id        =  76;
    static const int RcfError_ConnectionLimitExceeded_Id      =  77;
    static const int RcfError_DeserializationNullPointer_Id   =  78;
    static const int RcfError_PipeNameTooLong_Id              =  79;
    static const int RcfError_PingBack_Id                     =  80;
    static const int RcfError_NoPingBackService_Id            =  81;
    static const int RcfError_NoDownload_Id                   =  82;
    static const int RcfError_FileOffset_Id                   =  83;
    static const int RcfError_NoUpload_Id                     =  84;
    static const int RcfError_FileOpen_Id                     =  85;
    static const int RcfError_FileRead_Id                     =  86;
    static const int RcfError_FileWrite_Id                    =  87;
    static const int RcfError_UploadFailed_Id                 =  88;
    static const int RcfError_UploadInProgress_Id             =  89;
    static const int RcfError_ConcurrentUpload_Id             =  90;
    static const int RcfError_UploadFileSize_Id               =  91;
    static const int RcfError_AccessDenied_Id                 =  92;
    static const int RcfError_PingBackTimeout_Id              =  93;
    static const int RcfError_AllThreadsBusy_Id               =  94;
    static const int RcfError_UnsupportedRuntimeVersion_Id    =  95;
    static const int RcfError_FdSetSize_Id                    =  97;
    static const int RcfError_DnsLookup_Id                    =  98;
    static const int RcfError_SspiHandshakeExtraData_Id       =  99;
    static const int RcfError_ProtobufWrite_Id                = 101;
    static const int RcfError_ProtobufRead_Id                 = 102;
    static const int RcfError_ExtractSlice_Id                 = 103;
    static const int RcfError_ServerStubExpired_Id            = 104;
    static const int RcfError_VariantDeserialization_Id       = 105;
    static const int RcfError_SspiAuthFailServer_Id           = 106;
    static const int RcfError_SspiAuthFailClient_Id           = 107;
    static const int RcfError_Win32ApiError_Id                = 108;
    static const int RcfError_SspiLengthField_Id              = 109;
    static const int RcfError_DownloadFailed_Id               = 110;
    static const int RcfError_FileSeek_Id                     = 111;
    static const int RcfError_DownloadCancelled_Id            = 112;
    static const int RcfError_ParseSockAddr_Id                = 113;
    static const int RcfError_GetSockName_Id                  = 114;
    static const int RcfError_ProtobufWriteSize_Id            = 115;
    static const int RcfError_ProtobufWriteInit_Id            = 116;
    static const int RcfError_ArraySizeMismatch_Id            = 117;
    static const int RcfError_WcharSizeMismatch_Id            = 118;
    static const int RcfError_AnyTypeNotRegistered_Id         = 119;
    static const int RcfError_CryptoApiError_Id               = 120;
    static const int RcfError_ServerStubAccessDenied_Id       = 121;
    static const int RcfError_ApiError_Id                     = 122;
    static const int RcfError_HttpProxyPort_Id                = 123;
    static const int RcfError_OpenSslError_Id                 = 124;
    static const int RcfError_ProtocolNotSupported_Id         = 125;
    static const int RcfError_ClearCommunicationNotAllowed_Id = 126;
    static const int RcfError_ThreadingError_Id               = 127;
    static const int RcfError_RcfNotInitialized_Id            = 128;
    static const int RcfError_InvalidHttpMessage_Id           = 129;
    static const int RcfError_HttpRequestContentLength_Id     = 130;
    static const int RcfError_HttpResponseContentLength_Id    = 131;
    static const int RcfError_InvalidOpenSslCertificate_Id    = 132;
    static const int RcfError_InvalidSchannelCertificate_Id   = 133;
    static const int RcfError_HttpConnectFailed_Id            = 134;
    static const int RcfError_SspiImpersonateNoSspi_Id        = 135;
    static const int RcfError_TransportProtocolNotSupported_Id = 136;
    static const int RcfError_SslNotSupported_Id              = 137;
    static const int RcfError_SessionObjectDoesNotExist_Id    = 138;
    static const int RcfError_UploadAlreadyCompleted_Id       = 139;
    static const int RcfError_FileIndex_Id                    = 140;
    static const int RcfError_ConcurrentCalls_Id              = 141;
    static const int RcfError_ParseJsonRpcRequest_Id          = 142;
    static const int RcfError_DllLoad_Id                      = 143;
    static const int RcfError_DllFuncLoad_Id                  = 144;
    static const int RcfError_UnixDllLoad_Id                  = 145;
    static const int RcfError_UnixDllFuncLoad_Id              = 146;
    static const int RcfError_PingBackInterval_Id             = 147;
    static const int RcfError_FileOpenWrite_Id                = 148;
    static const int RcfError_CustomCertValidation_Id         = 149;
    static const int RcfError_SupportedOnWindowsOnly_Id       = 150;
    static const int RcfError_NotSupportedOnWindows_Id        = 151;
    static const int RcfError_NotSupportedInThisBuild_Id      = 152;
    static const int RcfError_NoLongerSupported_Id            = 153;
    static const int RcfError_SslCertVerificationCustom_Id    = 154;
    static const int RcfError_ServerCallbacksNotSupported_Id  = 155;
    static const int RcfError_ServerUnsupportedFeature_Id     = 156;
    static const int RcfError_SyncPublishError_Id             = 157;
    static const int RcfError_DeserializeVectorBool_Id        = 158;
    static const int RcfError_HttpTunnelError_Id              = 159;
    static const int RcfError_HttpSessionTimeout_Id           = 160;
    static const int RcfError_HttpRequestSessionIndex_Id      = 161;
    static const int RcfError_HttpResponseStatus_Id           = 162;
    static const int RcfError_HttpResponseSessionIndex_Id     = 163;
    static const int RcfError_HttpResponseSessionId_Id        = 164;
    static const int RcfError_NotHttpResponse_Id              = 165;
    static const int RcfError_NotHttpPostRequest_Id           = 166;
    static const int RcfError_NotHttpRequest_Id               = 167;
    static const int RcfError_NotSslHandshake_Id              = 168;
    static const int RcfError_ClientStubParms_Id              = 169;
    static const int RcfError_ServerStubParms_Id              = 170;
    static const int RcfError_SessionObjectNotCreated_Id      = 171;
    static const int RcfError_MessageHeaderEncoding_Id        = 172;
    static const int RcfError_OnewayHttp_Id                   = 173;
    static const int RcfError_ProxyAuthRetry_Id               = 174;
    static const int RcfError_ProxyCredentialsNeeded_Id       = 175;
    static const int RcfError_ProxyCredentialsInvalid_Id      = 176;
    static const int RcfError_UploadDirectory_Id              = 177;
    static const int RcfError_NotHttpConnection_Id            = 178;
    static const int RcfError_ProxyEndpointsNotEnabled_Id     = 179;
    static const int RcfError_ProxyEndpointDown_Id            = 180;
    static const int RcfError_BSerFileTransferNotSupported_Id = 181;
    static const int RcfError_CouldNotFindUpload_Id           = 182;
    static const int RcfError_ClientUninitialized_Id          = 183;
    static const int RcfError_OutgoingMessageLen_Id           = 184;
    static const int RcfError_NonSSLResponse_Id               = 185;
    static const int RcfError_NoProxyConnection_Id            = 186;
    static const int RcfError_ProxyServerMultiThreaded_Id     = 187;
    static const int RcfError_HttpRedirect_Id                 = 188;
    static const int RcfError_SetFileModTime_Id               = 189;
    static const int RcfError_GetFileModTime_Id               = 190;
    static const int RcfError_HttpMessageVerification_Id      = 191;
    static const int RcfError_HttpMessageVerificationAdmin_Id = 192;
    static const int RcfError_HttpSessionNotAvailable_Id      = 193;
    static const int RcfError_HttpInvalidMessage_Id           = 194;

    //[[[end]]]

    #define RcfError_User_Id                            1001
    #define RcfError_User                               ErrorMsg(1001)

}

#endif // ! INCLUDE_RCF_ERRORMSG_HPP
