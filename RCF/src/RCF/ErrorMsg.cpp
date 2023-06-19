
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

#include <RCF/ErrorMsg.hpp>
#include <RCF/Tools.hpp>

namespace RCF
{

    ErrorMsg::ErrorMsg(int errorId) : mErrorId(errorId)
    {
    }

    int ErrorMsg::getErrorId() const
    {
        return mErrorId;
    }

    std::string ErrorMsg::getLiteral() const
    {
        switch ( mErrorId )
        {
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
            cog.outl( ('case {} /' + '*{}*' + '/: return "{}"; ').format(str(errorNbr).ljust(5), errorId.ljust(40), errorMsg))
        cog.outl()
        ]]]*/

        case 0     /*RcfError_Ok                             */: return "No error."; 
        case 2     /*RcfError_ServerMessageLength            */: return "Server-side message length error."; 
        case 3     /*RcfError_ClientMessageLength            */: return "Client-side message length error."; 
        case 4     /*RcfError_Serialization                  */: return "Data serialization error. Type name: %1%. Exception type: %2%. Exception message: '%3%'."; 
        case 5     /*RcfError_Deserialization                */: return "Data deserialization error. Type name: %1%. Exception type: %2%. Exception message: '%3%'."; 
        case 6     /*RcfError_AppException                   */: return "Server-side application exception. Exception type: %1%. Exception message: '%2%'."; 
        case 8     /*RcfError_UnknownEndpoint                */: return "Unknown endpoint."; 
        case 9     /*RcfError_EndpointPassword               */: return "Incorrect endpoint password."; 
        case 10    /*RcfError_EndpointDown                   */: return "Endpoint unavailable."; 
        case 11    /*RcfError_EndpointRetry                  */: return "Endpoint temporarily unavailable (try again)."; 
        case 16    /*RcfError_ClientConnectTimeout           */: return "Network connection to %2% timed out after %1% ms (server not started?)."; 
        case 17    /*RcfError_PeerDisconnect                 */: return "Unexpected peer disconnection."; 
        case 18    /*RcfError_ClientCancel                   */: return "Remote call canceled by client."; 
        case 19    /*RcfError_StubAssignment                 */: return "Incompatible stub assignment."; 
        case 20    /*RcfError_PayloadFilterMismatch          */: return "Message filter mismatch."; 
        case 21    /*RcfError_OpenSslFilterInit              */: return "Failed to initialize OpenSSL filter. OpenSSL error: %1%"; 
        case 22    /*RcfError_OpenSslLoadCert                */: return "Failed to load OpenSSL certificate file. File: %1%. OpenSSL error: %2%"; 
        case 23    /*RcfError_UnknownPublisher               */: return "Unknown publishing topic."; 
        case 24    /*RcfError_UnknownFilter                  */: return "Unknown filter type."; 
        case 25    /*RcfError_NoServerStub                   */: return "Server-side binding not found. Binding: %1%."; 
        case 26    /*RcfError_Sspi                           */: return "SSPI error. Call to SSPI function %1% failed(). %2%"; 
        case 27    /*RcfError_SspiAuthFail                   */: return "SSPI authentication failed."; 
        case 28    /*RcfError_SspiInit                       */: return "Failed to initialize SSPI filter. %1% %2%"; 
        case 29    /*RcfError_UnknownSubscriber              */: return "Unknown subscriber."; 
        case 30    /*RcfError_ClientReadTimeout              */: return "Remote call timeout exceeded. No response from peer."; 
        case 31    /*RcfError_ClientReadFail                 */: return "Network connection error while waiting for response. %1%"; 
        case 32    /*RcfError_ClientWriteTimeout             */: return "Remote call timeout exceeded. Unable to send request."; 
        case 33    /*RcfError_ClientWriteFail                */: return "Network connection error while sending request. %1%"; 
        case 34    /*RcfError_ClientConnectFail              */: return "Unable to establish network connection. %1%"; 
        case 35    /*RcfError_Filter                         */: return "Filter error."; 
        case 36    /*RcfError_Socket                         */: return "Socket error. Call to OS function %1% failed. %2%"; 
        case 37    /*RcfError_FnId                           */: return "Invalid function id. Function id: %1%"; 
        case 38    /*RcfError_UnknownInterface               */: return "Unknown object interface. Interface: %1%."; 
        case 39    /*RcfError_NoEndpoint                     */: return "No endpoint."; 
        case 40    /*RcfError_TransportCreation              */: return "Failed to create transport."; 
        case 41    /*RcfError_FilterCount                    */: return "Invalid number of filters. Requested: %1%. Max allowed: %2%."; 
        case 42    /*RcfError_FilterMessage                  */: return "Failed to filter message."; 
        case 43    /*RcfError_UnfilterMessage                */: return "Failed to unfilter message."; 
        case 44    /*RcfError_SspiCredentials                */: return "SSPI credentials failure. Call to SSPI function %1% failed()."; 
        case 45    /*RcfError_SspiEncrypt                    */: return "SSPI encryption failure. Call to SSPI function %1% failed(). %2%"; 
        case 46    /*RcfError_SspiDecrypt                    */: return "SSPI decryption failure. Call to SSPI function %1% failed(). %2%"; 
        case 47    /*RcfError_SspiImpersonation              */: return "SSPI impersonation failure. Call to SSPI function %1% failed(). %2%"; 
        case 48    /*RcfError_NotConnected                   */: return "Send operation attempted without connecting."; 
        case 49    /*RcfError_SocketClose                    */: return "Failed to close socket. %1%"; 
        case 50    /*RcfError_ZlibDeflate                    */: return "Zlib compression error. %1%"; 
        case 51    /*RcfError_ZlibInflate                    */: return "Zlib decompression error. %1%"; 
        case 52    /*RcfError_Zlib                           */: return "Call to Zlib function '%1%' failed. %2%"; 
        case 53    /*RcfError_UnknownSerializationProtocol   */: return "Unknown serialization protocol. Protocol: %1%."; 
        case 54    /*RcfError_InvalidErrorMessage            */: return "Invalid error message from server."; 
        case 55    /*RcfError_SfNoCtor                       */: return "Construction not supported for this type."; 
        case 56    /*RcfError_SfRefMismatch                  */: return "Can't deserialize a reference into a non-reference object."; 
        case 57    /*RcfError_SfDataFormat                   */: return "Input data format error."; 
        case 58    /*RcfError_SfReadFailure                  */: return "Failed to read data from underlying stream."; 
        case 59    /*RcfError_SfWriteFailure                 */: return "Failed to write data to underlying stream."; 
        case 60    /*RcfError_SfBaseDerivedRegistration      */: return "Base/derived pair not registered. Base: %1%. Derived: %2%."; 
        case 61    /*RcfError_SfTypeRegistration             */: return "Type not registered. Type: %1%."; 
        case 62    /*RcfError_NonStdException                */: return "Non std::exception-derived exception was thrown."; 
        case 63    /*RcfError_SocketBind                     */: return "Failed to bind socket to port (port already in use?). Network interface: %1%. Port: %2%. Error: %3%"; 
        case 64    /*RcfError_Decoding                       */: return "Decoding error."; 
        case 65    /*RcfError_Encoding                       */: return "Encoding error."; 
        case 66    /*RcfError_TokenRequestFailed             */: return "No tokens available."; 
        case 67    /*RcfError_ObjectFactoryNotFound          */: return "Object factory not found."; 
        case 68    /*RcfError_PortInUse                      */: return "Port already in use. Network interface: %1%. Port: %2%."; 
        case 69    /*RcfError_DynamicObjectNotFound          */: return "Server-side object for given token not found. Token id: %1%."; 
        case 70    /*RcfError_VersionMismatch                */: return "Version mismatch."; 
        case 72    /*RcfError_SslCertVerification            */: return "SSL certificate verification failure. OpenSSL error: %1%"; 
        case 74    /*RcfError_FiltersLocked                  */: return "Filters locked."; 
        case 75    /*RcfError_Pipe                           */: return "Pipe error. %1%"; 
        case 76    /*RcfError_AnySerializerNotFound          */: return "Boost.Any serialization error: serializer not registered for the type '%1%'. Use SF::registerAny() to register a serializer."; 
        case 77    /*RcfError_ConnectionLimitExceeded        */: return "The server has reached its incoming connection limit."; 
        case 78    /*RcfError_DeserializationNullPointer     */: return "Null pointer deserialization error."; 
        case 79    /*RcfError_PipeNameTooLong                */: return "Pipe name too long. Pipe name: %1%. Max length: %2%."; 
        case 80    /*RcfError_PingBack                       */: return "Received ping back message from peer."; 
        case 81    /*RcfError_NoPingBackService              */: return "A ping back service is not available on the server."; 
        case 82    /*RcfError_NoDownload                     */: return "The specified download does not exist."; 
        case 83    /*RcfError_FileOffset                     */: return "The specified file offset is invalid. Expected offset: %1%. Actual offset: %2%."; 
        case 84    /*RcfError_NoUpload                       */: return "The specified upload does not exist.            "; 
        case 85    /*RcfError_FileOpen                       */: return "Failed to open file. Path: %1%. %2%"; 
        case 86    /*RcfError_FileRead                       */: return "Failed to read from file. Path: %1%. %2%."; 
        case 87    /*RcfError_FileWrite                      */: return "Failed to write to file. Path: %1%. %2%."; 
        case 88    /*RcfError_UploadFailed                   */: return "Upload failed to complete."; 
        case 89    /*RcfError_UploadInProgress               */: return "Upload still in progress."; 
        case 90    /*RcfError_ConcurrentUpload               */: return "Cannot upload on several connections simultaneously."; 
        case 91    /*RcfError_UploadFileSize                 */: return "File upload exceeding size limit."; 
        case 92    /*RcfError_AccessDenied                   */: return "Access denied."; 
        case 93    /*RcfError_PingBackTimeout                */: return "Failed to receive pingbacks from server. Expected pingback interval (ms): %1%."; 
        case 94    /*RcfError_AllThreadsBusy                 */: return "All server threads are busy."; 
        case 95    /*RcfError_UnsupportedRuntimeVersion      */: return "Unsupported RCF runtime version. Requested version: %1%. Max supported version: %2%."; 
        case 97    /*RcfError_FdSetSize                      */: return "FD_SETSIZE limit exceeded. Maximum: %1%."; 
        case 98    /*RcfError_DnsLookup                      */: return "DNS lookup of network address failed, for name '%1%'. %2%"; 
        case 99    /*RcfError_SspiHandshakeExtraData         */: return "SSPI handshake protocol error (extra data)."; 
        case 101   /*RcfError_ProtobufWrite                  */: return "Failed to serialize Protocol Buffer object. Type: %1%."; 
        case 102   /*RcfError_ProtobufRead                   */: return "Failed to deserialize Protocol Buffer object. Type: %1%."; 
        case 103   /*RcfError_ExtractSlice                   */: return "Failed to read from marshaling buffer. Position: %1%. Length: %2%. Marshaling buffer size: %3%."; 
        case 104   /*RcfError_ServerStubExpired              */: return "Server stub no longer available"; 
        case 105   /*RcfError_VariantDeserialization         */: return "Failed to deserialize variant object. Variant index: %1%. Variant size: %2%."; 
        case 106   /*RcfError_SspiAuthFailServer             */: return "Server-side SSPI authentication failed."; 
        case 107   /*RcfError_SspiAuthFailClient             */: return "Client-side SSPI authentication failed. %1%"; 
        case 108   /*RcfError_Win32ApiError                  */: return "The Win32 function %1% failed. %2%"; 
        case 109   /*RcfError_SspiLengthField                */: return "SSPI invalid format. Packet length exceeds total message length. Packet length: %1%. Message length: %2%."; 
        case 110   /*RcfError_DownloadFailed                 */: return "Download failed to complete. %1%"; 
        case 111   /*RcfError_FileSeek                       */: return "Failed to seek in file. File: %1%. Seek offset: %2%. %3%"; 
        case 112   /*RcfError_DownloadCancelled              */: return "Download canceled by server."; 
        case 113   /*RcfError_ParseSockAddr                  */: return "Unable to parse socket address structure. %1%"; 
        case 114   /*RcfError_GetSockName                    */: return "Unable to retrieve local address information from socket. %1%"; 
        case 115   /*RcfError_ProtobufWriteSize              */: return "Unable to determine serialized size of Protocol Buffers object. ByteSize() returned 0. Type: %1%."; 
        case 116   /*RcfError_ProtobufWriteInit              */: return "Failed to serialize Protocol Buffers object. Not all required fields were initialized. Type: %1%."; 
        case 117   /*RcfError_ArraySizeMismatch              */: return "Array size mismatch while deserializing. Array size: %1%. Array size from archive: %2%."; 
        case 118   /*RcfError_WcharSizeMismatch              */: return "Unicode character size mismatch. Native wchar_t size: %1%. wchar_t size from archive: %2%."; 
        case 119   /*RcfError_AnyTypeNotRegistered           */: return "Boost.Any serialization error: a type name has not been registered for the typeid '%1%'. Use SF::registerType() to register a type name."; 
        case 120   /*RcfError_CryptoApiError                 */: return "CryptoAPI error. Call to CryptoAPI function %1% failed. %2%"; 
        case 121   /*RcfError_ServerStubAccessDenied         */: return "Access to server binding was denied."; 
        case 122   /*RcfError_ApiError                       */: return "Call to %1% failed. %2%"; 
        case 123   /*RcfError_HttpProxyPort                  */: return "HTTP proxy port was not specified."; 
        case 124   /*RcfError_OpenSslError                   */: return "OpenSSL error: %1%"; 
        case 125   /*RcfError_ProtocolNotSupported           */: return "The requested transport protocol is not supported by this server."; 
        case 126   /*RcfError_ClearCommunicationNotAllowed   */: return "Server requires one of the following transport protocols to be used: %1%."; 
        case 127   /*RcfError_ThreadingError                 */: return "Threading error. Call to %1% failed."; 
        case 128   /*RcfError_RcfNotInitialized              */: return "RCF has not been initialized. Use the RCF::RcfInitDenit class, or call RCF::init() directly."; 
        case 129   /*RcfError_InvalidHttpMessage             */: return "Invalid HTTP message."; 
        case 130   /*RcfError_HttpRequestContentLength       */: return "Unexpected HTTP request. Content-Length header was not present."; 
        case 131   /*RcfError_HttpResponseContentLength      */: return "Unexpected HTTP response. Content-Length header was not present. HTTP status: %1%. HTTP response: %2%"; 
        case 132   /*RcfError_InvalidOpenSslCertificate      */: return "Invalid certificate format. OpenSSL-based SSL implementation requires certificate to be in PEM format and loaded with the RCF::PemCertificate class."; 
        case 133   /*RcfError_InvalidSchannelCertificate     */: return "Invalid certificate format. Schannel-based SSL implementation requires certificate to be loaded with the RCF::PfxCertificate or RCF::StoreCertificate classes."; 
        case 134   /*RcfError_HttpConnectFailed              */: return "Failed to connect via HTTPS proxy. HTTP CONNECT request to proxy failed. HTTP status: %1%. HTTP response: %2%"; 
        case 135   /*RcfError_SspiImpersonateNoSspi          */: return "Unable to impersonate client. Impersonation requires one of the following transport protocols: NTLM, Kerberos, Negotiate."; 
        case 136   /*RcfError_TransportProtocolNotSupported  */: return "The requested transport protocol is not supported by this RCF build. Transport protocol: %1%"; 
        case 137   /*RcfError_SslNotSupported                */: return "SSL transport protocol is not supported by this RCF build."; 
        case 138   /*RcfError_SessionObjectDoesNotExist      */: return "Session object does not exist. Session object type: %1%"; 
        case 139   /*RcfError_UploadAlreadyCompleted         */: return "The specified upload has already been completed."; 
        case 140   /*RcfError_FileIndex                      */: return "The specified file index is invalid. Expected index: %1%. Actual index: %2%."; 
        case 141   /*RcfError_ConcurrentCalls                */: return "Error: multiple concurrent calls attempted on the same RcfClient<> object. To make concurrent calls, use multiple RcfClient<> objects instead."; 
        case 142   /*RcfError_ParseJsonRpcRequest            */: return "Unable to parse JSON-RPC request. json_spirit::read_stream() returned false."; 
        case 143   /*RcfError_DllLoad                        */: return "Unable to load library. Library name: %1%. Error: %2%"; 
        case 144   /*RcfError_DllFuncLoad                    */: return "Unable to load function from dynamic library. Library name: %1%. Function name: %2%. Error: %3%"; 
        case 145   /*RcfError_UnixDllLoad                    */: return "Unable to load library. Library name: %1%. Error: %2%"; 
        case 146   /*RcfError_UnixDllFuncLoad                */: return "Unable to load function from dynamic library. Library name: %1%. Function name: %2%. Error: %3%"; 
        case 147   /*RcfError_PingBackInterval               */: return "Invalid ping back interval. Ping back interval must be at least %2% ms. Requested ping back interval was %1% ms."; 
        case 148   /*RcfError_FileOpenWrite                  */: return "Unable to open file for writing. File path: %1%"; 
        case 149   /*RcfError_CustomCertValidation           */: return "Certificate could not be validated. Error: %1%"; 
        case 150   /*RcfError_SupportedOnWindowsOnly         */: return "%1% is only supported on Windows platforms."; 
        case 151   /*RcfError_NotSupportedOnWindows          */: return "%1% is not supported on Windows platforms."; 
        case 152   /*RcfError_NotSupportedInThisBuild        */: return "%1% is not supported in this RCF build."; 
        case 153   /*RcfError_NoLongerSupported              */: return "%1% is no longer supported in this version of RCF."; 
        case 154   /*RcfError_SslCertVerificationCustom      */: return "SSL certificate verification failure."; 
        case 155   /*RcfError_ServerCallbacksNotSupported    */: return "The server has not been configured to accept callback connections."; 
        case 156   /*RcfError_ServerUnsupportedFeature       */: return "The server does not support this feature. Feature: %1%."; 
        case 157   /*RcfError_SyncPublishError               */: return "Synchronous error while sending to subscriber. Error: %1%."; 
        case 158   /*RcfError_DeserializeVectorBool          */: return "Bit count mismatch on deserialization of vector<bool>. Bit count: %1%. Buffer size: %2%."; 
        case 159   /*RcfError_HttpTunnelError                */: return "HTTP tunnel error. %1%"; 
        case 160   /*RcfError_HttpSessionTimeout             */: return "HTTP session was not found on server."; 
        case 161   /*RcfError_HttpRequestSessionIndex        */: return "HTTP session index mismatch on request. Expected index: %1% . Actual index: %2% ."; 
        case 162   /*RcfError_HttpResponseStatus             */: return "HTTP response error. HTTP response status: %1% . HTTP response: %2%"; 
        case 163   /*RcfError_HttpResponseSessionIndex       */: return "HTTP session index mismatch on response. Expected index: %1% . Actual index: %2% ."; 
        case 164   /*RcfError_HttpResponseSessionId          */: return "HTTP session ID mismatch on response. Expected ID: %1% . Actual ID: %2% ."; 
        case 165   /*RcfError_NotHttpResponse                */: return "The server responded with a non-HTTP response. The first bytes of the response were: '%1%'."; 
        case 166   /*RcfError_NotHttpPostRequest             */: return "This HTTP port only supports HTTP POST requests."; 
        case 167   /*RcfError_NotHttpRequest                 */: return "The server received a non-HTTP request."; 
        case 168   /*RcfError_NotSslHandshake                */: return "Protocol mismatch. Expected SSL handshake."; 
        case 169   /*RcfError_ClientStubParms                */: return "Unable to allocate client stub parameter structure."; 
        case 170   /*RcfError_ServerStubParms                */: return "Unable to allocate server stub parameter structure"; 
        case 171   /*RcfError_SessionObjectNotCreated        */: return "Could not create session object.Session object type: %1%"; 
        case 172   /*RcfError_MessageHeaderEncoding          */: return "Message header encoding error. Maximum length: %1% . Actual length: %2%"; 
        case 173   /*RcfError_OnewayHttp                     */: return "Oneway calls are not supported over the HTTP and HTTPS transports."; 
        case 174   /*RcfError_ProxyAuthRetry                 */: return "Proxy authentication is required. Reconnecting to proxy."; 
        case 175   /*RcfError_ProxyCredentialsNeeded         */: return "Proxy authentication is required. Please supply valid credentials."; 
        case 176   /*RcfError_ProxyCredentialsInvalid        */: return "Unable to authenticate to proxy. Invalid credentials."; 
        case 177   /*RcfError_UploadDirectory                */: return "RCF upload directory has not been configured on the server."; 
        case 178   /*RcfError_NotHttpConnection              */: return "This connection does not use HTTP or HTTPS."; 
        case 179   /*RcfError_ProxyEndpointsNotEnabled       */: return "Proxy endpoints are not enabled on this server."; 
        case 180   /*RcfError_ProxyEndpointDown              */: return "Proxy endpoint '%1%' is currently not available."; 
        case 181   /*RcfError_BSerFileTransferNotSupported   */: return "File transfers not supported when using Boost.Serialization as serialization protocol."; 
        case 182   /*RcfError_CouldNotFindUpload             */: return "No file upload found for upload ID '%1%'."; 
        case 183   /*RcfError_ClientUninitialized            */: return "RCF client has not been initialized."; 
        case 184   /*RcfError_OutgoingMessageLen             */: return "Outgoing message exceeded the maximum allowed length. Message length: %1%. Maximum allowed length: %2%."; 
        case 185   /*RcfError_NonSSLResponse                 */: return "The server responded with a non-SSL response. The first bytes of the response were: '%1%'."; 
        case 186   /*RcfError_NoProxyConnection              */: return "Unable to establish connection to proxy endpoint '%1%'."; 
        case 187   /*RcfError_ProxyServerMultiThreaded       */: return "Proxy server must be multi-threaded, in order to service proxy requests."; 
        case 188   /*RcfError_HttpRedirect                   */: return "The server returned a HTTP redirect response. The HTTP response was '%1%'."; 
        case 189   /*RcfError_SetFileModTime                 */: return "Unable to set last modified time for file '%1%'. %2%"; 
        case 190   /*RcfError_GetFileModTime                 */: return "Unable to retrieve last modified time for file '%1%'. %2%"; 
        case 191   /*RcfError_HttpMessageVerification        */: return "HTTP message verification failed. "; 
        case 192   /*RcfError_HttpMessageVerificationAdmin   */: return "HTTP message verification failed. %1%"; 
        case 193   /*RcfError_HttpSessionNotAvailable        */: return "HTTP session not available."; 
        case 194   /*RcfError_HttpInvalidMessage             */: return "Invalid HTTP message."; 

        //[[[end]]]

        default:
            RCF_ASSERT_ALWAYS("Invalid error message ID.");
        }

        return "";
    }

}
