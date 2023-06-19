
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

#include <RCF/Config.hpp>
#include <RCF/Warnings.hpp>

#include "AmiThreadPool.cpp"
#include "AsioHandlerCache.cpp"
#include "AsioServerTransport.cpp"
#include "BsdClientTransport.cpp"
#include "ByteBuffer.cpp"
#include "ByteOrdering.cpp"
#include "Certificate.cpp"
#include "ClientStub.cpp"
#include "ClientStubLegacy.cpp"
#include "ClientTransport.cpp"
#include "ConnectedClientTransport.cpp"
#include "CurrentSerializationProtocol.cpp"
#include "CurrentSession.cpp"
#include "DynamicLib.cpp"
#include "Enums.cpp"
#include "ErrorMsg.cpp"
#include "Exception.cpp"
#include "Filter.cpp"
#include "FilterService.cpp"
#include "Future.cpp"
#include "Globals.cpp"
#include "InitDeinit.cpp"
#include "IpAddress.cpp"
#include "IpClientTransport.cpp"
#include "IpServerTransport.cpp"
#include "Log.cpp"
#include "Marshal.cpp"
#include "MemStream.cpp"
#include "MethodInvocation.cpp"
#include "ObjectPool.cpp"
#include "PerformanceData.cpp"
#include "PeriodicTimer.cpp"
#include "Platform.cpp"
#include "RcfClient.cpp"
#include "RcfServer.cpp"
#include "RcfSession.cpp"
#include "RemoteCallContext.cpp"
#include "ReallocBuffer.cpp"
#include "SerializationProtocol.cpp"
#include "ServerStub.cpp"
#include "ServerTask.cpp"
#include "ServerTransport.cpp"
#include "Service.cpp"
#include "SessionTimeoutService.cpp"
#include "Tchar.cpp"
#include "ThreadLibrary.cpp"
#include "ThreadLocalData.cpp"
#include "ThreadPool.cpp"
#include "TimedBsdSockets.cpp"
#include "Timer.cpp"
#include "Tools.cpp"
#include "UsingBsdSockets.cpp"
#include "Uuid.cpp"
#include "Version.cpp"

#ifdef RCF_WINDOWS
#include "Win32Username.cpp"
#include "Win32Certificate.cpp"
#endif

#if RCF_FEATURE_TCP==1
#include "TcpServerTransport.cpp"
#include "TcpClientTransport.cpp"
#include "TcpEndpoint.cpp"
#endif


#if RCF_FEATURE_UDP==1
#include "UdpClientTransport.cpp"
#include "UdpEndpoint.cpp"
#include "UdpServerTransport.cpp"
#endif


#if RCF_FEATURE_HTTP==1
#include "HttpEndpoint.cpp"
#include "HttpClientTransport.cpp"
#include "HttpServerTransport.cpp"
#include "HttpsEndpoint.cpp"
#include "HttpsClientTransport.cpp"
#include "HttpsServerTransport.cpp"
#include "HttpFrameFilter.cpp"
#include "HttpConnectFilter.cpp"
#include "HttpSessionFilter.cpp"
#include "Base64.cpp"
#endif


#if RCF_FEATURE_SERVER==1
#include "CallbackConnectionService.cpp"
#include "PingBackService.cpp"
#include "ServerObjectService.cpp"
#endif

#if RCF_FEATURE_PROXYENDPOINT==1
#include "ProxyEndpoint.cpp"
#include "ProxyEndpointTransport.cpp"
#include "ProxyEndpointService.cpp"
#endif

#if RCF_FEATURE_PUBSUB==1
#include "MulticastClientTransport.cpp"
#include "PublishingService.cpp"
#include "SubscriptionService.cpp"
#include "SubscriptionServiceLegacy.cpp"
#endif


#include <RCF/Asio.hpp> // For RCF_HAS_LOCAL_SOCKETS

#if RCF_FEATURE_LOCALSOCKET==1 && defined(RCF_HAS_LOCAL_SOCKETS)
#include "UnixLocalServerTransport.cpp"
#include "UnixLocalClientTransport.cpp"
#include "UnixLocalEndpoint.cpp"
#elif RCF_FEATURE_LOCALSOCKET==1
namespace RCF {
    class UnixLocalEndpoint
    {
    public:
        UnixLocalEndpoint(const std::string & socketName)
        {
            RCF_UNUSED_VARIABLE(socketName);
            RCF_ASSERT(0 && "UNIX local socket endpoints are not supported on this platform.");
        }
    };
}
#endif


#if RCF_FEATURE_SSPI==1
#include "Schannel.cpp"
#include "SspiCredentials.cpp"
#include "SspiFilter.cpp"
#endif


#if RCF_FEATURE_NAMEDPIPE==1
#include "Win32NamedPipeClientTransport.cpp"
#include "Win32NamedPipeEndpoint.cpp"
#include "Win32NamedPipeServerTransport.cpp"
#endif


#if RCF_FEATURE_OPENSSL==1
#include "OpenSslEncryptionFilter.cpp"
#endif


#if RCF_FEATURE_ZLIB==1
#include "ZlibCompressionFilter.cpp"
#endif


#if RCF_FEATURE_SF==1
#include "../SF/SF.cpp"
#else
#include "../SF/Encoding.cpp"
#endif


#if RCF_FEATURE_FILETRANSFER==1
#include "FileIoThreadPool.cpp"
#include "FileTransferService.cpp"
#include "FileStream.cpp"
#include "FileSystem.cpp"
#endif


#if RCF_FEATURE_LOCALSOCKET==1 || RCF_FEATURE_NAMEDPIPE==1
#include "NamedPipeEndpoint.cpp"
#endif
