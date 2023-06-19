
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

#ifndef INCLUDE_RCF_ASIOSERVERTRANSPORT_HPP
#define INCLUDE_RCF_ASIOSERVERTRANSPORT_HPP

#include <memory>
#include <set>
#include <vector>

#include <RCF/Asio.hpp>
#include <RCF/AsioBuffers.hpp>
#include <RCF/ByteBuffer.hpp>
#include <RCF/Export.hpp>
#include <RCF/ServerTransport.hpp>
#include <RCF/Service.hpp>
#include <RCF/ThreadLibrary.hpp>
#include <RCF/Tools.hpp>

namespace RCF {

    class RcfServer;
    class TcpClientTransport;
    class AsioNetworkSession;
    class AsioServerTransport;

    typedef std::shared_ptr<AsioNetworkSession>         AsioNetworkSessionPtr;
    typedef std::weak_ptr<AsioNetworkSession>           AsioNetworkSessionWeakPtr;

    class ReallocBuffer;
    typedef std::shared_ptr<ReallocBuffer> ReallocBufferPtr;

    class Exception;

    enum WireProtocol;

    class I_RcfClient;
    typedef std::shared_ptr<I_RcfClient> RcfClientPtr;

    class AsioAcceptor
    {
    public:
        virtual ~AsioAcceptor();
    };

    typedef std::unique_ptr<AsioAcceptor>           AsioAcceptorPtr;

    class RCF_EXPORT AsioServerTransport :
        public ServerTransport,
        public ServerTransportEx,
        public I_Service
    {
    private:

        // Needs to call open().
        friend class TcpTransportFactory;
        friend class HttpTransportFactory;
        friend class HttpsTransportFactory;

        friend class Win32NamedPipeNetworkSession;

        typedef std::weak_ptr<RcfSession>              SessionWeakPtr;

        AsioNetworkSessionPtr createNetworkSession();

    protected:

        // I_ServerTransportEx implementation
        ClientTransportUniquePtr  
                            createClientTransport(
                                const Endpoint &endpoint);

        SessionPtr          createServerSession(
                                ClientTransportUniquePtr & clientTransportUniquePtr, 
                                RcfClientPtr stubEntryPtr,
                                bool keepClientConnection);

        ClientTransportUniquePtr  
                            createClientTransport(
                                SessionPtr sessionPtr);

    private:

        // I_Service implementation
        void                open();
        void                close();
        void                stop();

        void                onServiceAdded(     RcfServer & server);
        void                onServiceRemoved(   RcfServer & server);

    protected:

        void                onServerStart(      RcfServer & server);
        void                onServerStop(       RcfServer & server);
        void                setServer(          RcfServer & server);
        
        void                startAccepting();

    private:

        void                startAcceptingThread(Exception & eRet);

    public:

        RcfServer &         getServer();
        RcfServer &         getSessionManager();

    private:

        void                registerSession(NetworkSessionWeakPtr session);
        void                unregisterSession(NetworkSessionWeakPtr session);
        void                cancelOutstandingIo();

        friend class AsioNetworkSession;
        friend class FilterAdapter;
        friend class ServerTcpFrame;
        friend class ServerHttpFrame;

    protected:

        AsioServerTransport();
        ~AsioServerTransport();
        
        AsioIoService *                 mpIoService;
        AsioAcceptorPtr                 mAcceptorPtr;

        WireProtocol                    mWireProtocol;

    private:
        
        volatile bool                   mStopFlag;
        RcfServer *                     mpServer;

    private:

        virtual AsioNetworkSessionPtr     implCreateNetworkSession() = 0;
        virtual void                    implOpen() = 0;

        virtual ClientTransportUniquePtr  implCreateClientTransport(
                                            const Endpoint &endpoint) = 0;

    public:

        AsioAcceptor &                getAcceptor();

        AsioIoService &                 getIoService();
    };

    class ReadHandler
    {
    public:
        ReadHandler(AsioNetworkSessionPtr networkSessionPtr);
        void operator()(AsioErrorCode err, std::size_t bytes);
        void * allocate(std::size_t size);
        AsioNetworkSessionPtr mNetworkSessionPtr;
    };

    class WriteHandler
    {
    public:
        WriteHandler(AsioNetworkSessionPtr networkSessionPtr);
        void operator()(AsioErrorCode err, std::size_t bytes);
        void * allocate(std::size_t size);
        AsioNetworkSessionPtr mNetworkSessionPtr;
    };

    void *  asio_handler_allocate(std::size_t size, ReadHandler * pHandler);
    void    asio_handler_deallocate(void * pointer, std::size_t size, ReadHandler * pHandler);
    void *  asio_handler_allocate(std::size_t size, WriteHandler * pHandler);
    void    asio_handler_deallocate(void * pointer, std::size_t size, WriteHandler * pHandler);

    class RCF_EXPORT AsioNetworkSession :
        public NetworkSession,
        Noncopyable
    {
    public:

        friend class ReadHandler;
        friend class WriteHandler;
        friend class ServerTcpFrame;
        friend class ServerHttpFrame;


        typedef std::weak_ptr<AsioNetworkSession>       AsioNetworkSessionWeakPtr;
        typedef std::shared_ptr<AsioNetworkSession>     AsioNetworkSessionPtr;

        AsioNetworkSession(
            AsioServerTransport &transport,
            AsioIoService & ioService);

        virtual ~AsioNetworkSession();

        AsioNetworkSessionPtr sharedFromThis();

        void            close();

        AsioErrorCode   getLastError();

        void            setCloseAfterWrite();

    protected:
        AsioIoService &         mIoService;

        std::vector<char>       mReadHandlerBuffer;
        std::vector<char>       mWriteHandlerBuffer;

        AsioErrorCode           mLastError;

        void            runOnDestroyCallbacks();

    private:

        // read()/write() are used to connect with the filter sequence.
        void            read(
                            const ByteBuffer &byteBuffer, 
                            std::size_t bytesRequested);

        void            write(
                            const std::vector<ByteBuffer> &byteBuffers);

    public:

        void            setTransportFilters(
                            const std::vector<FilterPtr> &filters);

        void            getTransportFilters(
                            std::vector<FilterPtr> &filters);

        void            setWireFilters(
                            const std::vector<FilterPtr> &filters);

        void            getWireFilters(
                            std::vector<FilterPtr> &filters);

        AsioServerTransport &   getAsioServerTransport();

    private:
    
        void            beginAccept();
        void            beginRead();
        void            beginWrite();

        void            onAcceptCompleted(const AsioErrorCode & error);

        void            onNetworkReadCompleted(
                            AsioErrorCode error, 
                            size_t bytesTransferred);

        void            onNetworkWriteCompleted(
                            AsioErrorCode error, 
                            size_t bytesTransferred);

        friend class HttpSessionFilter;
        void            onAppReadWriteCompleted(
                            size_t bytesTransferred);

        void            sendServerError(int error);

        void            doCustomFraming(size_t bytesTransferred);
        void            doRegularFraming(size_t bytesTransferred);

        // TODO: too many friends
        friend class    AsioServerTransport;
        friend class    TcpNetworkSession;
        friend class    UnixLocalNetworkSession;
        friend class    Win32NamedPipeNetworkSession;
        friend class    FilterAdapter;

        enum State
        {
            Ready,
            Accepting,
            ReadingData,
            ReadingDataCount,
            WritingData
        };

        State                       mState;
        bool                        mIssueZeroByteRead;
        std::size_t                 mReadBufferRemaining;
        std::size_t                 mWriteBufferRemaining;

        std::vector<FilterPtr>      mTransportFilters;

        friend class HttpServerTransport;
        friend class PublishingService;
        std::vector<FilterPtr>      mWireFilters;        

        AsioServerTransport &       mTransport;

        std::vector<ByteBuffer>     mWriteByteBuffers;
        std::vector<ByteBuffer>     mSlicedWriteByteBuffers;

        ReallocBufferPtr            mAppReadBufferPtr;
        ByteBuffer                  mAppReadByteBuffer;
        
        ReallocBufferPtr            mNetworkReadBufferPtr;
        ByteBuffer                  mNetworkReadByteBuffer;

        // So we can connect our read()/write() functions to the filter sequence.
        FilterPtr                   mFilterAdapterPtr;

        bool                        mCloseAfterWrite;

        NetworkSessionWeakPtr       mWeakThisPtr;

        AsioBuffers                 mBufs;

        std::shared_ptr<Mutex>    mSocketOpsMutexPtr;

        // I_NetworkSession

    private:
        
        void                    postRead();
        ByteBuffer              getReadByteBuffer();
        void                    postWrite(std::vector<ByteBuffer> &byteBuffers);
        void                    postClose();
        ServerTransport &       getServerTransport();
        const RemoteAddress &   getRemoteAddress();
        bool                    isConnected();

    private:

        virtual const RemoteAddress & implGetRemoteAddress() = 0;
        virtual void implRead(char * buffer, std::size_t bufferLen) = 0;
        virtual void implWrite(const std::vector<ByteBuffer> & buffers) = 0;
        virtual void implAccept() = 0;
        virtual bool implOnAccept() = 0;
        virtual bool implIsConnected() = 0;
        virtual void implClose() = 0;
        virtual void implCloseAfterWrite() {}
        virtual void implTransferNativeFrom(ClientTransport & clientTransport) = 0;
        virtual ClientTransportUniquePtr implCreateClientTransport() = 0;
    };

} // namespace RCF


#endif // ! INCLUDE_RCF_ASIOSERVERTRANSPORT_HPP
