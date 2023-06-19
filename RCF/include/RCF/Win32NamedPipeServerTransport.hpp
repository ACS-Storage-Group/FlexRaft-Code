
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

#ifndef INCLUDE_RCF_WIN32NAMEDPIPESERVERTRANSPORT_HPP
#define INCLUDE_RCF_WIN32NAMEDPIPESERVERTRANSPORT_HPP

#include <RCF/AsioFwd.hpp>
#include <RCF/AsioServerTransport.hpp>
#include <RCF/Export.hpp>
#include <RCF/Tchar.hpp>

namespace RCF {

    class Win32NamedPipeServerTransport;

    // Win32NamedPipeNetworkSession

    class RCF_EXPORT Win32NamedPipeNetworkSession : public AsioNetworkSession
    {
    public:
        Win32NamedPipeNetworkSession(
            Win32NamedPipeServerTransport & transport,
            AsioIoService & ioService);

        ~Win32NamedPipeNetworkSession();

        const RemoteAddress & implGetRemoteAddress();

        void implRead(char * buffer, std::size_t bufferLen);

        void implWrite(const std::vector<ByteBuffer> & buffers);

        void implWrite(AsioNetworkSession &toBeNotified, const char * buffer, std::size_t bufferLen);

        void implAccept();

        bool implOnAccept();

        bool implIsConnected();

        void implClose();

        ClientTransportUniquePtr implCreateClientTransport();

        void implTransferNativeFrom(ClientTransport & clientTransport);

        static void closeSocket(AsioPipeHandlePtr socketPtr);

        HANDLE getNativeHandle();

    private:
        AsioPipeHandlePtr           mSocketPtr;
        tstring                     mRemotePipeName;
        NoRemoteAddress             mRemoteAddress;

        friend class Win32NamedPipeServerTransport;
        friend class Win32NamedPipeImpersonator;
    };

    class RCF_EXPORT Win32NamedPipeServerTransport : 
        public AsioServerTransport
    {
    public:

        Win32NamedPipeServerTransport(const tstring & pipeName);
        ~Win32NamedPipeServerTransport();

        TransportType getTransportType();

        ServerTransportPtr clone();

        AsioNetworkSessionPtr implCreateNetworkSession();
        void implOpen();
        ClientTransportUniquePtr implCreateClientTransport(
            const Endpoint &endpoint);

        tstring getPipeName() const;

        void onServerStart(RcfServer & server);
        void onServerStop(RcfServer & server);

        void setSecurityAttributes(LPSECURITY_ATTRIBUTES pSec);

    private:

        friend class Win32NamedPipeNetworkSession;

        tstring                         mPipeName;
        HANDLE                          mPipeNameLock;

        LPSECURITY_ATTRIBUTES           mpSec;
    };

    /// Allows the server side of a Win32 named pipe to impersonate the client.
    class RCF_EXPORT Win32NamedPipeImpersonator
    {
    public:
        /// Impersonates the named pipe client associated with the RcfSession. Impersonation lasts until revertToSelf() is called, or the Win32NamedPipeImpersonator object is destroyed.
        Win32NamedPipeImpersonator(RcfSession& session);

        Win32NamedPipeImpersonator(Win32NamedPipeNetworkSession & pipeSession);
        
        /// Destroys the Win32NamedPipeImpersonator object, and ceases any impersonation.
        ~Win32NamedPipeImpersonator();

        void impersonate();

        /// Ceases impersonation of the named pipe client.
        void revertToSelf() const;

    private:

        Win32NamedPipeNetworkSession & mPipeSession;
    };

    class RCF_EXPORT NullDacl
    {
    public:
        NullDacl();
        SECURITY_ATTRIBUTES mSa;
        SECURITY_DESCRIPTOR mSd;
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_WIN32NAMEDPIPESERVERTRANSPORT_HPP
