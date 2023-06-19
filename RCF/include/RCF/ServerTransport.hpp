
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

#ifndef INCLUDE_RCF_SERVERTRANSPORT_HPP
#define INCLUDE_RCF_SERVERTRANSPORT_HPP

#include <memory>
#include <set>
#include <string>
#include <vector>

#include <cstdint>

#include <RCF/Enums.hpp>
#include <RCF/Export.hpp>
#include <RCF/RcfFwd.hpp>
#include <RCF/ThreadLibrary.hpp>

namespace RCF {

    /// Describes the network address of a remote peer.
    class RemoteAddress
    {
    public:
        virtual ~RemoteAddress()
        {}

        virtual std::string string() const
        {
            return "";
        }
    };

    enum TransportProtocol;
    enum TransportType;

    /// Indicates that no remote address is available.
    class NoRemoteAddress : public RemoteAddress
    {};
   
    class RCF_EXPORT NetworkSession : public std::enable_shared_from_this<NetworkSession>
    {
    public:

        NetworkSession();
        virtual ~NetworkSession();

        virtual void        postRead() = 0;
        virtual ByteBuffer  getReadByteBuffer() = 0;
        virtual void        postWrite(std::vector<ByteBuffer> &byteBuffers) = 0;
        virtual void        postClose() = 0;
       
        virtual ServerTransport & 
                            getServerTransport() = 0;

        virtual const RemoteAddress & 
                            getRemoteAddress() = 0;

        virtual bool        isConnected() = 0;


        virtual void        setTransportFilters(const std::vector<FilterPtr> &filters) = 0;
        virtual void        getTransportFilters(std::vector<FilterPtr> &filters) = 0;
        void                setEnableReconnect(bool enableReconnect);
        bool                getEnableReconnect();
        
        virtual void        getWireFilters(std::vector<FilterPtr> &filters);



        std::uint64_t       getTotalBytesReceived() const;
        std::uint64_t       getTotalBytesSent() const;

        RcfSessionPtr       getSessionPtr() const;

        std::uint32_t       getLastActivityTimestamp() const;
        void                setLastActivityTimestamp();

    protected:
        bool                mEnableReconnect;
        std::uint64_t       mBytesReceivedCounter;
        std::uint64_t       mBytesSentCounter;
        std::uint32_t       mLastActivityTimestampMs;

        RcfSessionPtr       mRcfSessionPtr;

    };

    typedef std::shared_ptr<NetworkSession>     NetworkSessionPtr;
    typedef std::weak_ptr<NetworkSession>       NetworkSessionWeakPtr;

    class RcfSession;
    typedef std::shared_ptr<RcfSession>         RcfSessionPtr;
    
    typedef RcfSessionPtr                       SessionPtr;

    class ThreadPool;
    typedef std::shared_ptr<ThreadPool>         ThreadPoolPtr;

    enum RpcProtocol
    {
        Rp_Rcf = 0,
        Rp_JsonRpc = 1,
    };

    /// Base class for all server transports.
    class RCF_EXPORT ServerTransport
    {
    public:
        ServerTransport();

        virtual ~ServerTransport() {}

        virtual ServerTransportPtr 
                        clone() = 0;

        // *** SWIG BEGIN ***

        /// Returns the transport type of this server transport.
        virtual TransportType getTransportType() = 0;

        /// Sets maximum incoming message length. Incoming messages that are larger
        /// than this size will be dropped.
        void                setMaxIncomingMessageLength(std::size_t maxMessageLength);

        /// Returns maximum incoming message length.
        std::size_t         getMaxIncomingMessageLength() const;

        /// Sets the maximum number of simultaneous connections to the server transport.
        void                setConnectionLimit(std::size_t connectionLimit);

        /// Returns the maximum number of simultaneous connections to the server transport.
        std::size_t         getConnectionLimit() const;

        /// Sets the initial number of listening connections that are created when the server transport starts.
        void                setInitialNumberOfConnections(std::size_t initialNumberOfConnections);

        /// Returns the initial number of listening connections that are created when the server transport starts.
        std::size_t         getInitialNumberOfConnections() const;

        /// Sets the thread pool that the server transport will use.
        void                setThreadPool(ThreadPoolPtr threadPoolPtr);

        /// Sets the list of supported protocols the server transport supports. Clients
        /// that connect without using one of the supported protocols are dropped. If
        /// the list of supported protocols is empty, all protocols are allowed.
        void                setSupportedProtocols(const std::vector<TransportProtocol> & protocols);

        /// Returns the list of supported protocols for the server transport.
        const std::vector<TransportProtocol> & getSupportedProtocols() const;

        // *** SWIG END ***

        void                setRpcProtocol(RpcProtocol rpcProtocol);
        RpcProtocol         getRpcProtocol() const;

        
    protected:

        RpcProtocol                 mRpcProtocol;
        bool                        mCustomFraming;

    private:

        mutable ReadWriteMutex      mReadWriteMutex;
        std::size_t                 mMaxMessageLength;
        std::size_t                 mConnectionLimit;       
        std::size_t                 mInitialNumberOfConnections;

        std::vector<TransportProtocol> mSupportedProtocols;

    protected:

        Mutex                               mSessionsMutex;
        std::set<NetworkSessionWeakPtr>     mSessions;

    public:

        template<typename Iter>
        void enumerateSessions(const Iter & iter)
        {
            Lock lock(mSessionsMutex);
            std::copy(mSessions.begin(), mSessions.end(), iter);
        }

    };

    class ServerTransportEx
    {
    public:

        virtual ~ServerTransportEx() {}

        virtual ClientTransportUniquePtr 
            createClientTransport(
                const Endpoint &endpoint) = 0;
       
        virtual SessionPtr 
            createServerSession(
                ClientTransportUniquePtr & clientTransportUniquePtr,
                RcfClientPtr stubEntryPtr,
                bool keepClientConnection) = 0;

        virtual ClientTransportUniquePtr 
            createClientTransport(
                SessionPtr sessionPtr) = 0;
       
    };   

    RCF_EXPORT std::size_t  getDefaultMaxMessageLength();

    RCF_EXPORT void         setDefaultMaxMessageLength(
                                std::size_t maxMessageLength);

} // namespace RCF

#endif // ! INCLUDE_RCF_SERVERTRANSPORT_HPP
