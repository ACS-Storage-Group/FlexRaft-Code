
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

#include <RCF/TcpClientTransport.hpp>

#include <chrono>
#include <functional>

#include <RCF/AmiIoHandler.hpp>
#include <RCF/AsioDeadlineTimer.hpp>
#include <RCF/AmiThreadPool.hpp>
#include <RCF/ClientStub.hpp>
#include <RCF/InitDeinit.hpp>
#include <RCF/OverlappedAmi.hpp>
#include <RCF/TcpEndpoint.hpp>
#include <RCF/ThreadLocalData.hpp>
#include <RCF/TimedBsdSockets.hpp>
#include <RCF/TcpEndpoint.hpp>
#include <RCF/Tools.hpp>

#include <RCF/RcfServer.hpp>

namespace RCF {

    TcpClientTransport::TcpClientTransport(const TcpClientTransport &rhs) :
        BsdClientTransport(rhs),
        mServerAddr(rhs.mServerAddr),
        mConnectionAddr(rhs.mConnectionAddr)
    {}

    TcpClientTransport::TcpClientTransport(const IpAddress &remoteAddr) :
        BsdClientTransport(),
        mServerAddr(remoteAddr),
        mConnectionAddr(remoteAddr)
    {}

    TcpClientTransport::TcpClientTransport(const std::string & ip, int port) :
        BsdClientTransport(),
        mServerAddr(ip, port),
        mConnectionAddr(ip, port)
    {}

    TcpClientTransport::TcpClientTransport(TcpSocketPtr socketPtr) : 
        BsdClientTransport(socketPtr)
    {
    }

    TcpClientTransport::~TcpClientTransport()
    {
        RCF_DTOR_BEGIN
            if (mOwn)
            {
                close();
            }
            mClosed = true;
        RCF_DTOR_END
    }

    TransportType TcpClientTransport::getTransportType()
    {
        if ( mWireFilters.size() == 1 )
        {
            return RCF::Tt_Http;
        }
        else if ( mWireFilters.size() == 2 )
        {
            return RCF::Tt_Https;
        }
        return Tt_Tcp;
    }

    std::unique_ptr<ClientTransport> TcpClientTransport::clone() const
    {
        return ClientTransportUniquePtr( new TcpClientTransport(*this) );
    }

    void TcpClientTransport::implConnect(
        ClientTransportCallback &clientStub,
        unsigned int timeoutMs)
    {
        // TODO: replace throw with return, where possible

        RCF_ASSERT(!mAsync);

        // TODO: should be closed already?
        implClose();

        // Are we going through a proxy?
        ClientStub * pClientStub = static_cast<ClientStub *>(&clientStub);
        if (pClientStub->getHttpProxy().size() > 0)
        {
            if (pClientStub->getHttpProxyPort() == 0)
            {
                RCF::Exception e( RcfError_HttpProxyPort );
                RCF_THROW(e);
            }

            mConnectionAddr = IpAddress(
                pClientStub->getHttpProxy(), 
                pClientStub->getHttpProxyPort());
        }
        else
        {
            if (mConnectionAddr != mServerAddr)
            {
                mConnectionAddr = mServerAddr;
            }
        }

        if (!mConnectionAddr.isResolved())
        {
            RCF::ExceptionPtr e;
            mConnectionAddr.resolve(e);
            RCF_VERIFY(!e, *e);
        }

        RCF_ASSERT(mConnectionAddr.isResolved());
        setupSocket();

        unsigned int startTimeMs = getCurrentTimeMs();
        mEndTimeMs = startTimeMs + timeoutMs;

        PollingFunctor pollingFunctor(
            mClientProgressPtr,
            RemoteCallPhase::Rcp_Connect,
            mEndTimeMs);

        int err = 0;

        sockaddr * pSockAddr = NULL;
        Platform::OS::BsdSockets::socklen_t sockAddrSize = 0;
        mConnectionAddr.getSockAddr(pSockAddr, sockAddrSize);

        int fd = getNativeHandle();

        int ret = timedConnect(
            pollingFunctor,
            err,
            fd,
            pSockAddr,
            sockAddrSize);

        if (ret != 0)
        {
            close();

            if (err == 0)
            {
                Exception e( RcfError_ClientConnectTimeout, 
                    timeoutMs, 
                    mConnectionAddr.string());
                
                RCF_THROW(e);
            }
            else
            {
                Exception e(RcfError_ClientConnectFail, osError(err));
                RCF_THROW(e);
            }
        }

        mAssignedLocalIp = IpAddress(fd, mConnectionAddr.getType());

        clientStub.onConnectCompleted();
    }

    void TcpClientTransport::doDnsLookup(
        std::size_t index,
        unsigned int timeoutMs,
        OverlappedAmiPtr overlappedPtr,
        IpAddress ipAddress)
    {
        setWin32ThreadName("RCF DNS Lookup Thread");

        // Resolving may take a while.
        RCF::ExceptionPtr e;
        ipAddress.resolve(e);

        RecursiveLock lock(overlappedPtr->mMutex);
        if (index == overlappedPtr->mIndex && overlappedPtr->mpTransport)
        {
            TcpClientTransport *pTcpClientTransport = 
                static_cast<TcpClientTransport *>(overlappedPtr->mpTransport);

            ++overlappedPtr->mIndex;

            pTcpClientTransport->onDnsLookupCompleted(timeoutMs, ipAddress, e);
            getTlsAmiNotification().run();
        }
    }

    void doBlockingConnect(
        int fd, 
        sockaddr * pAddr, 
        Platform::OS::BsdSockets::socklen_t addrSize, 
        OverlappedAmiPtr overlappedAmiPtr)
    {
        // Set our thread name.
        setWin32ThreadName("RCF AMI Blocking Connect Thread");

        Platform::OS::BsdSockets::setblocking(fd, true);

        int ret = Platform::OS::BsdSockets::connect(
            fd, 
            pAddr, 
            addrSize);

        int err = Platform::OS::BsdSockets::GetLastError();

        Platform::OS::BsdSockets::setblocking(fd, false);

        AmiIoHandler handler(overlappedAmiPtr);

        // Invoke the handler.
        if (ret == 0)
        {
            handler();
        }
        else
        {
            AsioErrorCode erc(err, ASIO_NS::error::system_category);
            handler(erc);
        }
    }


    void TcpClientTransport::onDnsLookupCompleted(
        unsigned int timeoutMs,
        IpAddress ipAddress, 
        ExceptionPtr e)
    {
        mConnectionAddr = ipAddress;

        if (e)
        {
            mpClientStub->onError(*e);
            return;
        }
        else if (!mConnectionAddr.isResolved())
        {
            mpClientStub->onError(
                Exception(RcfError_DnsLookup, mConnectionAddr.string(), ""));

            return;
        }

        RCF::Exception eBind;
        setupSocket(eBind);
        if (eBind.bad())
        {
            mpClientStub->onError(eBind);
            return;
        }

        sockaddr * pSockAddr = NULL;
        Platform::OS::BsdSockets::socklen_t sockAddrSize = 0;
        mConnectionAddr.getSockAddr(pSockAddr, sockAddrSize);

        ASIO_NS::ip::tcp::endpoint endpoint;

        if (mConnectionAddr.getType() == IpAddress::V4)
        {
            sockaddr_in * pSockAddrV4 = (sockaddr_in *) pSockAddr;
            unsigned long addr = pSockAddrV4->sin_addr.s_addr;
            addr = ntohl(addr);
            unsigned short port = static_cast<unsigned short>(mConnectionAddr.getPort());

            endpoint = ASIO_NS::ip::tcp::endpoint(
                ASIO_NS::ip::address( ASIO_NS::ip::address_v4(addr)),
                port);

        }
        else if (mConnectionAddr.getType() == IpAddress::V6)
        {
            sockaddr_in6 * pSockAddrV6 = (sockaddr_in6 *) pSockAddr;

            ASIO_NS::ip::address_v6::bytes_type bytes;
            memcpy(&bytes[0], pSockAddrV6->sin6_addr.s6_addr, 16);

            endpoint = ASIO_NS::ip::tcp::endpoint(
                ASIO_NS::ip::address(
                    ASIO_NS::ip::address_v6(bytes)),
                static_cast<unsigned short>(mConnectionAddr.getPort()));
        }
        else
        {
            RCF_ASSERT_ALWAYS("");
        }

        RecursiveLock lock(mOverlappedPtr->mMutex);

        if (AmiThreadPool::getEnableMultithreadedBlockingConnects())
        {
            // Launch a temporary thread and run a blocking connect() call.

            mOverlappedPtr->mOpType = Connect;

            RCF::ThreadPtr doConnectThread( new RCF::Thread( std::bind(
                &doBlockingConnect,
                static_cast<int>(mTcpSocketPtr->native()),
                pSockAddr,
                sockAddrSize,
                mOverlappedPtr)));
        }
        else
        {
            // Use Boost.Asio async_connect().

            mOverlappedPtr->mOpType = Connect;

            mTcpSocketPtr->async_connect( 
                endpoint, 
                AmiIoHandler(mOverlappedPtr));
        }

        mAsioTimerPtr->expires_from_now(std::chrono::milliseconds(timeoutMs));
        mAsioTimerPtr->async_wait( AmiTimerHandler(mOverlappedPtr) );
    }

    void TcpClientTransport::implConnectAsync(
        ClientTransportCallback &clientStub,
        unsigned int timeoutMs)
    {
        // TODO: sort this out
        RCF_UNUSED_VARIABLE(timeoutMs);

        RCF_ASSERT(mAsync);

        implClose();

        mpClientStub = &clientStub;

        // Are we going through a proxy?
        ClientStub * pClientStub = static_cast<ClientStub *>(mpClientStub);
        if (pClientStub->getHttpProxy().size() > 0)
        {
            if (pClientStub->getHttpProxyPort() == 0)
            {
                RCF::Exception e( RcfError_HttpProxyPort );
                RCF_THROW(e);
            }

            mConnectionAddr = IpAddress(
                pClientStub->getHttpProxy(), 
                pClientStub->getHttpProxyPort());
        }
        else
        {
            if (mConnectionAddr != mServerAddr)
            {
                mConnectionAddr = mServerAddr;
            }
        }

        if (!mConnectionAddr.isResolved())
        {
            // Fire and forget.

            RecursiveLock lock(mOverlappedPtr->mMutex);

            Thread thread( std::bind( 
                &TcpClientTransport::doDnsLookup, 
                mOverlappedPtr->mIndex,
                timeoutMs,
                mOverlappedPtr, 
                mConnectionAddr));

            thread.detach();
        }
        else
        {
            onDnsLookupCompleted(
                timeoutMs,
                mConnectionAddr, 
                ExceptionPtr());
        }
    }

    void TcpClientTransport::setupSocket()
    {
        RCF::Exception e;
        setupSocket(e);
        if (e.bad())
        {
            RCF_THROW(e);
        }
    }

    void TcpClientTransport::setupSocket(Exception & e)
    {
        e = Exception();

        RCF_ASSERT(mFd == INVALID_SOCKET);

        mFd = mConnectionAddr.createSocket();
        Platform::OS::BsdSockets::setblocking(mFd, false);

        // Bind to local interface, if one has been specified.
        if (!mLocalIp.empty())
        {
            mLocalIp.resolve();

            sockaddr * pSockAddr = NULL;
            Platform::OS::BsdSockets::socklen_t sockAddrSize = 0;
            mLocalIp.getSockAddr(pSockAddr, sockAddrSize);

            int ret = ::bind(
                mFd, 
                pSockAddr, 
                sockAddrSize);

            if (ret < 0)
            {
                int err = Platform::OS::BsdSockets::GetLastError();
                if (err == Platform::OS::BsdSockets::ERR_EADDRINUSE)
                {
                    e = Exception(RcfError_PortInUse, mLocalIp.getIp(), mLocalIp.getPort());
                }
                else
                {
                    e = Exception(RcfError_SocketBind, mLocalIp.getIp(), mLocalIp.getPort(), osError(err));
                }
            }
        }

        if (mpIoService)
        {
            mAsioTimerPtr.reset( new AsioDeadlineTimer(*mpIoService) );
            mTcpSocketPtr.reset( new AsioSocket(*mpIoService) );
            if (mConnectionAddr.getType() == IpAddress::V4)
            {
                mTcpSocketPtr->assign(ASIO_NS::ip::tcp::v4(), mFd);
            }
            else if (mConnectionAddr.getType() == IpAddress::V6)
            {
                mTcpSocketPtr->assign(ASIO_NS::ip::tcp::v6(), mFd);
            }
            else
            {
                RCF_ASSERT_ALWAYS("");
            }

            mFd = -1;
        }
    }

    void TcpClientTransport::associateWithIoService(AsioIoService & ioService)
    {
        if (mTcpSocketPtr)
        {
            RCF_ASSERT(mpIoService == & ioService);
        }
        else
        {
            mpIoService = &ioService;

            mTcpSocketPtr.reset( new TcpSocket(ioService) );
            if (mFd != -1)
            {
                if (mConnectionAddr.getType() == IpAddress::V4)
                {
                    mTcpSocketPtr->assign(ASIO_NS::ip::tcp::v4(), mFd);
                }
                else if (mConnectionAddr.getType() == IpAddress::V6)
                {
                    mTcpSocketPtr->assign(ASIO_NS::ip::tcp::v6(), mFd);
                }
                else
                {
                    RCF_ASSERT_ALWAYS("");
                }
            }
            mAsioTimerPtr.reset( new AsioDeadlineTimer(*mpIoService) );
            mFd = -1;
        }
    }

    bool TcpClientTransport::isAssociatedWithIoService()
    {
        return mpIoService ? true : false;
    }

    void TcpClientTransport::implClose()
    {
        if (mTcpSocketPtr)
        {
            if (mSocketOpsMutexPtr)
            {
                Lock lock(*mSocketOpsMutexPtr);
                mTcpSocketPtr->close();
            }
            else
            {
                mTcpSocketPtr->close();
            }

            mTcpSocketPtr.reset();
        }
        else if (mFd != -1)
        {
            int ret = Platform::OS::BsdSockets::closesocket(mFd);
            int err = Platform::OS::BsdSockets::GetLastError();
            mFd = -1;

            RCF_VERIFY(
                ret == 0,
                Exception(RcfError_Socket, "closesocket()", osError(err)));
        }

        mFd = -1;
    }

    EndpointPtr TcpClientTransport::getEndpointPtr() const
    {
        return EndpointPtr( new TcpEndpoint(mConnectionAddr) );
    }

    void TcpClientTransport::setRemoteAddr(const IpAddress &remoteAddr)
    {
        mConnectionAddr = remoteAddr;
    }

    IpAddress TcpClientTransport::getRemoteAddr() const
    {
        return mConnectionAddr;
    }

} // namespace RCF
