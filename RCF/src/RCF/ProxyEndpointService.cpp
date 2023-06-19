
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

#include <RCF/ProxyEndpointService.hpp>

#include <RCF/BsdClientTransport.hpp>
#include <RCF/Enums.hpp>
#include <RCF/Log.hpp>
#include <RCF/Uuid.hpp>
#include <RCF/ProxyEndpointTransport.hpp>

namespace RCF
{

    ProxyEndpointEntry::ProxyEndpointEntry()
    {
    }

    ProxyEndpointEntry::ProxyEndpointEntry(const std::string& endpointName) : mName(endpointName)
    {
    }

    void ProxyEndpointService::onServiceAdded(RCF::RcfServer &server)
    {
        mpRcfServer = &server;
        server.bind<I_ProxyEp>(*this);
    }

    void ProxyEndpointService::onServiceRemoved(RCF::RcfServer &server)
    {
        RCF_UNUSED_VARIABLE(server);

    }

    void ProxyEndpointService::onServerStart(RCF::RcfServer &server)
    {
        RCF_UNUSED_VARIABLE(server);

    }

    void ProxyEndpointService::onServerStop(RCF::RcfServer &server)
    {
        RCF_UNUSED_VARIABLE(server);

        {
            Lock lock(mEntriesMutex);
            mEntries.clear();
        }

        {
            Lock lock(mEndpointConnectionsMutex);
            mEndpointConnections.clear();
        }
    }

    void ProxyEndpointService::enumerateProxyEndpoints(std::vector<std::string>& endpoints)
    {
        if ( !mpRcfServer->getEnableProxyEndpoints() )
        {
            RCF_THROW(Exception(RcfError_ProxyEndpointsNotEnabled));
        }

        endpoints.clear();
        std::vector<std::string> duds;
        
        Lock lock(mEntriesMutex);
        
        for ( auto iter : mEntries )
        {
            const std::string& endpointName = iter.first;
            ProxyEndpointEntry & entry = iter.second;
            RcfSessionPtr sessionPtr = entry.mSessionWeakPtr.lock();
            if ( sessionPtr && sessionPtr->isConnected() )
            {
                endpoints.push_back(endpointName);
            }
            else
            {
                duds.push_back(endpointName);
            }
        }

        for (auto dud : duds)
        {
            mEntries.erase(mEntries.find(dud));
        }
    }

    ClientTransportUniquePtr ProxyEndpointService::makeProxyEndpointConnection(
        const std::string& proxyEndpointName)
    {

        if ( !mpRcfServer->getEnableProxyEndpoints() )
        {
            RCF_THROW(Exception(RcfError_ProxyEndpointsNotEnabled));
        }

        // If serving a proxy request over the network, the server must be multi-threaded.
        if ( getCurrentRcfSessionPtr() && mpRcfServer->getThreadPool()->getThreadMaxCount() < 2 )
        {
            RCF_THROW(Exception(RcfError_ProxyServerMultiThreaded));
        }

        std::string requestId = generateUuid();

        bool endpointOnline = true;
        {
            Lock lock(mEntriesMutex);
            auto iter = mEntries.find(proxyEndpointName);
            if (iter != mEntries.end())
            {
                ProxyEndpointEntry & entry = iter->second;

                RcfSessionPtr sessionPtr = entry.mSessionWeakPtr.lock();
                if ( !sessionPtr || !sessionPtr->isConnected() )
                {
                    mEntries.erase(iter);
                    endpointOnline = false;
                }
                else
                {
                    entry.mPendingRequests.push_back(requestId);
                    if ( entry.mAmdPtr  )
                    {
                        std::vector<std::string> & amdRequests = entry.mAmdPtr->parameters().a1.get();
                        amdRequests.clear();
                        amdRequests.swap(entry.mPendingRequests);
                        entry.mAmdPtr->commit();
                        entry.mAmdPtr.reset();
                    }
                }
            }
            else
            {
                endpointOnline = false;
            }
        }

        if ( !endpointOnline )
        {
            Exception e(RcfError_ProxyEndpointDown, proxyEndpointName);
            RCF_THROW(e);
        }

        // Wait for a connection to show up.
        auto requestKey = std::make_pair(proxyEndpointName, requestId);
        Timer connectTimer;
        
        while ( !connectTimer.elapsed(10*1000) )
        {
            Lock lock(mEndpointConnectionsMutex);
            auto iter = mEndpointConnections.find(requestKey);
            if ( iter != mEndpointConnections.end() )
            {
                ClientTransportUniquePtr transportPtr = std::move(iter->second);
                mEndpointConnections.erase(iter);
                return transportPtr;
            }

            using namespace std::chrono_literals;
            mEndpointConnectionsCond.wait_for(lock, 1000ms);
        }
        
        // If we're here, we've timed out waiting for the proxy endpoint to spool up a connection.
        Exception e(RcfError_NoProxyConnection, proxyEndpointName);
        RCF_THROW(e);
        return ClientTransportUniquePtr();
    }

    class RelaySocket;
    typedef std::shared_ptr<RelaySocket> RelaySocketPtr;

    class RelaySocket : public std::enable_shared_from_this<RelaySocket>
    {
    public:

        RelaySocket(TcpSocketPtr socketPtr1, TcpSocketPtr socketPtr2) :
            mSocketPtr1(socketPtr1),
            mSocketPtr2(socketPtr2),
            mBuffer1(4096),
            mBuffer2(4096)
        {

        }

        ~RelaySocket()
        {
        }

        class ReadHandler
        {
        public:
            ReadHandler(RelaySocketPtr relaySocketPtr, TcpSocketPtr socketPtr) : 
                mRelaySocketPtr(relaySocketPtr),
                mSocketPtr(socketPtr)
            {}

            void operator()(AsioErrorCode err, std::size_t bytes)
            {
                mRelaySocketPtr->onReadCompleted(mSocketPtr, err, bytes);
            }

            RelaySocketPtr mRelaySocketPtr;
            TcpSocketPtr mSocketPtr;
        };

        class WriteHandler
        {
        public:
            WriteHandler(RelaySocketPtr relaySocketPtr, TcpSocketPtr socketPtr) :
                mRelaySocketPtr(relaySocketPtr),
                mSocketPtr(socketPtr)
            {}

            void operator()(AsioErrorCode err, std::size_t bytes)
            {
                mRelaySocketPtr->onWriteCompleted(mSocketPtr, err, bytes);
            }

            RelaySocketPtr mRelaySocketPtr;
            TcpSocketPtr mSocketPtr;
        };

        void onReadCompleted(TcpSocketPtr socketPtr, AsioErrorCode err, std::size_t bytes)
        {
            RCF_LOG_4()(this)(err)(bytes) << "RelaySocket - read from socket completed.";

            if ( err )
            {
                if ( socketPtr == mSocketPtr2 )
                {
                    AsioErrorCode ec;
                    mSocketPtr1->shutdown(ASIO_NS::ip::tcp::socket::shutdown_send, ec);
                }
                else
                {
                    AsioErrorCode ec;
                    mSocketPtr2->shutdown(ASIO_NS::ip::tcp::socket::shutdown_send, ec);
                }
            }
            else
            {
                RCF_LOG_4()(this)(bytes) << "RelaySocket - issuing write on relayed socket.";
                if ( socketPtr == mSocketPtr1 )
                {
                    mBufferLen1 = bytes;
                    ASIO_NS::async_write(
                        *mSocketPtr2,
                        ASIO_NS::buffer(&mBuffer1[0], bytes),
                        WriteHandler(shared_from_this(), mSocketPtr2));
                }
                else
                {
                    mBufferLen2 = bytes;
                    ASIO_NS::async_write(
                        *mSocketPtr1,
                        ASIO_NS::buffer(&mBuffer2[0], bytes),
                        WriteHandler(shared_from_this(), mSocketPtr1));
                }
            }
        }

        void onWriteCompleted(TcpSocketPtr socketPtr, AsioErrorCode err, std::size_t bytes)
        {
            RCF_LOG_4()(this)(err)(bytes) << "RelaySocket - write to relayed socket completed.";
            if ( err )
            {
                if ( socketPtr == mSocketPtr2 )
                {
                    AsioErrorCode ec;
                    mSocketPtr1->shutdown(ASIO_NS::ip::tcp::socket::shutdown_send, ec);
                }
                else
                {
                    AsioErrorCode ec;
                    mSocketPtr2->shutdown(ASIO_NS::ip::tcp::socket::shutdown_send, ec);
                }
            }
            else
            {
                RCF_LOG_4()(this) << "RelaySocket - issuing read on socket.";
                if ( socketPtr == mSocketPtr2 )
                {
                    mSocketPtr1->async_read_some(
                        ASIO_NS::buffer(&mBuffer1[0], mBuffer1.size()),
                        ReadHandler(shared_from_this(), mSocketPtr1));
                }
                else
                {
                    mSocketPtr2->async_read_some(
                        ASIO_NS::buffer(&mBuffer2[0], mBuffer2.size()),
                        ReadHandler(shared_from_this(), mSocketPtr2));
                }
            }
        }

        void start()
        {
            RCF_LOG_3()(this) << "RelaySocket - start relaying.";
            onWriteCompleted(mSocketPtr1, RCF::AsioErrorCode(), 0);
            onWriteCompleted(mSocketPtr2, RCF::AsioErrorCode(), 0);
        }

    private:

        TcpSocketPtr mSocketPtr1;
        std::vector<char> mBuffer1;
        std::size_t mBufferLen1 = 0;

        TcpSocketPtr mSocketPtr2;
        std::vector<char> mBuffer2;
        std::size_t mBufferLen2 = 0;
    };

    void ProxyEndpointService::setupProxiedConnection(RcfSession& session, ClientTransportPtr proxyTransportPtr)
    {
        // Get the socket to the client.
        ServerTransportEx &serverTransport = dynamic_cast<ServerTransportEx &>(session.getNetworkSession().getServerTransport());
        ClientTransportPtr clientTransportPtr(serverTransport.createClientTransport(session.shared_from_this()).release());
        BsdClientTransport& clientTransport = dynamic_cast<BsdClientTransport&>(*clientTransportPtr);
        clientTransport.setRcfSession(RcfSessionWeakPtr());
        session.setCloseSessionAfterWrite(true);
        TcpSocketPtr clientSocketPtr = clientTransport.releaseTcpSocket();

        // Get the socket to the target server.
        BsdClientTransport& proxyTransport = dynamic_cast<BsdClientTransport&>(*proxyTransportPtr);
        TcpSocketPtr proxySocketPtr = proxyTransport.releaseTcpSocket();

        // Start relaying them.
        RelaySocketPtr relaySocketPtr(new RelaySocket(clientSocketPtr, proxySocketPtr));
        relaySocketPtr->start();
    }

    class ProxyEndpointSession
    {
    public:
        ProxyEndpointSession()
        {
        }

        ~ProxyEndpointSession()
        {
        }

        ProxyEndpointService *      mpEpService = NULL;
        std::string                 mEndpointName;
    };

    void ProxyEndpointService::removeEndpoint(const std::string& endpointName)
    {
        Lock lock(mEntriesMutex);
        auto iter = mEntries.find(endpointName);
        if ( iter != mEntries.end() )
        {
            mEntries.erase(iter);
        }
    }

    void ProxyEndpointService::SetupProxyEndpoint(const std::string& endpointName, const std::string& password)
    {
        if ( !mpRcfServer->getEnableProxyEndpoints() )
        {
            RCF_THROW(Exception(RcfError_ProxyEndpointsNotEnabled));
        }

        // TODO: check password.
        RCF_UNUSED_VARIABLE(password);

        ProxyEndpointSession * pEpSession = &getCurrentRcfSession().createSessionObject<ProxyEndpointSession>();
        pEpSession->mEndpointName = endpointName;
        pEpSession->mpEpService = this;

        Lock lock(mEntriesMutex);
        mEntries[endpointName] = ProxyEndpointEntry(endpointName);
        mEntries[endpointName].mSessionWeakPtr = getCurrentRcfSession().shared_from_this();
    }

    void ProxyEndpointService::GetConnectionRequests(
        std::vector<std::string>&   requests)
    {
        ProxyEndpointSession * pEpSession = 
            getCurrentRcfSession().querySessionObject<ProxyEndpointSession>();

        if ( !pEpSession )
        {
            // TODO: literal
            Exception e("Invalid proxy endpoint connection.");
            RCF_THROW(e);
        }

        std::string endpointName = pEpSession->mEndpointName;

        requests.clear();

        Lock lock(mEntriesMutex);
        auto iter = mEntries.find(endpointName);
        if (iter != mEntries.end())
        {
            ProxyEndpointEntry & entry = iter->second;
            if ( entry.mSessionWeakPtr.lock() == getCurrentRcfSession().shared_from_this() )
            {
                if ( entry.mPendingRequests.size() > 0 )
                {
                    // Synchronous return.
                    requests.clear();
                    requests.swap(entry.mPendingRequests);
                }
                else
                {
                    // Asynchronous return, once a request is made.
                    entry.mAmdPtr.reset(new AmdGetRequests(getCurrentRcfSession()));
                    entry.mSessionWeakPtr = getCurrentRcfSession().shared_from_this();
                }
            }
        }
    }

    void ProxyEndpointService::onConnectionAvailable(
        const std::string& endpointName,
        const std::string& requestId,
        RCF::RcfSessionPtr sessionPtr,
        RCF::ClientTransportUniquePtr transportPtr)
    {
        Lock lock(mEntriesMutex);
        mEndpointConnections[std::make_pair(endpointName, requestId)].reset( transportPtr.release() );
        mEndpointConnectionsCond.notify_all();
    }

    void ProxyEndpointService::MakeConnectionAvailable(
        const std::string&          endpointName, 
        const std::string&          requestId)
    {
        RCF::convertRcfSessionToRcfClient(
            [=](RcfSessionPtr sessionPtr, ClientTransportUniquePtr transportPtr)
            {
                onConnectionAvailable(endpointName, requestId, sessionPtr, std::move(transportPtr));
            },
            RCF::Twoway);
    }

} // namespace RCF
