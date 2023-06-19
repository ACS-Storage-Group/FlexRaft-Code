
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

#include <RCF/ConnectedClientTransport.hpp>

#include <RCF/ClientStub.hpp>
#include <RCF/Exception.hpp>
#include <RCF/Filter.hpp>
#include <RCF/OverlappedAmi.hpp>
#include <RCF/RcfSession.hpp>
#include <RCF/ThreadLocalData.hpp>
#include <RCF/Log.hpp>

#include <chrono>

namespace RCF {

    class ClientFilterProxy : public Filter, Noncopyable
    {
    public:
        ClientFilterProxy(
            ConnectedClientTransport & transport, 
            Filter & filter, 
            bool top) :
                mTransport(transport),
                mFilter(filter),
                mTop(top)
        {}

    private:

        void read(const ByteBuffer &byteBuffer, std::size_t bytesRequested)
        {
            mTop ?
                mFilter.read(byteBuffer, bytesRequested) :
                mTransport.read(byteBuffer, bytesRequested);
        }

        void write(const std::vector<ByteBuffer> &byteBuffers)
        {
            mTop ?
                mFilter.write(byteBuffers) :
                mTransport.write(byteBuffers);
        }

        void onReadCompleted(const ByteBuffer &byteBuffer)
        {
            mTop ?
                mTransport.onReadCompleted(byteBuffer) :
                mFilter.onReadCompleted(byteBuffer);
        }

        void onWriteCompleted(std::size_t bytesTransferred)
        {
            mTop ?
                mTransport.onWriteCompleted(bytesTransferred) :
                mFilter.onWriteCompleted(bytesTransferred);
        }

        int getFilterId() const
        {
            return RcfFilter_Unknown;
        }

        ConnectedClientTransport &mTransport;
        Filter &mFilter;
        bool mTop;
    };

    void ConnectedClientTransport::onCompletion(int bytesTransferred)
    {
        
        ClientStub * pStub = NULL;
        if (mpClientStub->isClientStub())
        {
            pStub = static_cast<ClientStub *>(mpClientStub);
        }

        CurrentClientStubSentry sentry(pStub);

        switch (mPostState)
        {
        case Connecting:
            onConnectCompleted(0);
            break;

        case Reading:
            onTimedRecvCompleted(bytesTransferred, 0);
            break;

        case Writing:
            onTimedSendCompleted(bytesTransferred, 0);
            break;

        default:
            RCF_ASSERT_ALWAYS("");
        }
    }

#ifdef _MSC_VER
#pragma warning( push )
// warning C4355: 'this' : used in base member initializer list
#pragma warning( disable : 4355 ) 
#endif

    ConnectedClientTransport::ConnectedClientTransport() :
        mOwn(true),
        mClosed(true),
        mMaxSendSize(1024*1024*10),
        mError(),
        mNoTimeout(false),
        mEndTimeMs(),

        mPreState(Connecting),
        mPostState(Connecting),
        mReadBufferPos(),
        mWriteBufferPos(),
        mpClientStub(NULL),
        mpClientStubReadBuffer(),
        mBytesToRead(),
        mBytesRequested(),
        mReadBuffer2(),
        
        mOverlappedPtr( new OverlappedAmi(this) )
    {
        setTransportFilters( std::vector<FilterPtr>() );
    }

    ConnectedClientTransport::ConnectedClientTransport(
        const ConnectedClientTransport &rhs) :
            ClientTransport(rhs),        
            mOwn(true),
            mClosed(true),
            mMaxSendSize(1024*1024*10),
            mError(),
            mNoTimeout(false),
            mEndTimeMs(),

            mPreState(Connecting),
            mPostState(Connecting),
            mReadBufferPos(),
            mWriteBufferPos(),
            mpClientStub(NULL),
            mpClientStubReadBuffer(),
            mBytesToRead(),
            mBytesRequested(),
            mReadBuffer2(),
            
            mOverlappedPtr( new OverlappedAmi(this) )
            
    {
        setTransportFilters( std::vector<FilterPtr>() );
    }

#ifdef _MSC_VER
#pragma warning( pop )
#endif

    ConnectedClientTransport::~ConnectedClientTransport()
    {
        RCF_DTOR_BEGIN
            
            RCF_ASSERT(mClosed);

        RCF_DTOR_END
    }

    void ConnectedClientTransport::connect(
        ClientTransportCallback &clientStub, 
        unsigned int timeoutMs)
    {
        if (!isConnected())
        {
            mClosed = false;

            mPreState = Connecting;
            mPostState = Connecting;

            fireProgressEvent();

            if (mAsync)
            {
                implConnectAsync(clientStub, timeoutMs);
            }
            else
            {
                implConnect(clientStub, timeoutMs);
            }
        }
    }

    void ConnectedClientTransport::onConnectCompleted(int err)
    {
        RCF_UNUSED_VARIABLE(err);
        mpClientStub->onConnectCompleted();
    }

    void ConnectedClientTransport::disconnect(unsigned int timeoutMs)
    {
        RCF_UNUSED_VARIABLE(timeoutMs);

        for (std::size_t i=0; i<mWireFilters.size(); ++i)
        {
            mWireFilters[i]->resetState();
        }

        close();
    }

    void ConnectedClientTransport::read(
        const ByteBuffer &byteBuffer_, 
        std::size_t bytesRequested)
    {
        mBytesRequested = bytesRequested;

        if (byteBuffer_.getLength() == 0)
        {
            if (mReadBuffer2Ptr.get() == NULL || !mReadBuffer2Ptr.unique())
            {
                mReadBuffer2Ptr.reset( new ReallocBuffer() );
            }
            mReadBuffer2Ptr->resize(bytesRequested);
            mReadBuffer2 = ByteBuffer(mReadBuffer2Ptr);
        }
        else
        {
            mReadBuffer2 = byteBuffer_;
        }

        mBytesToRead = RCF_MIN(bytesRequested, mReadBuffer2.getLength());
        std::size_t & bytesToRead = mBytesToRead;

        mPostState = Reading;

        if (!mAsync)
        {
            std::size_t bytesRead = implRead(mReadBuffer2, bytesToRead);
            RCF_UNUSED_VARIABLE(bytesRead);
        }
        else
        {
            std::size_t bytesRead = implReadAsync(mReadBuffer2, bytesToRead);            
            RCF_UNUSED_VARIABLE(bytesRead);
        }
    }

    void ConnectedClientTransport::onTimedRecvCompleted(int ret, int err)
    {
        switch (ret)
        {
        case -2:
            {
                Exception e(RcfError_ClientReadTimeout);
                RCF_THROW(e);
            }
            break;

        case -1:
            {
                Exception e(
                    RcfError_ClientReadFail,
                    osError(err));

                RCF_THROW(e);
            }
            break;

        case  0:
            {
                Exception e(RcfError_PeerDisconnect);
                RCF_THROW(e);
            }
            break;

        default:
            RCF_ASSERT(0 < ret && ret <= static_cast<int>(mBytesRequested));

            mLastResponseSize += ret;
            mRunningTotalBytesReceived += ret;

            ByteBuffer b(mReadBuffer2.release(), 0, ret);
            mTransportFilters.empty() ?
                onReadCompleted(b) :
                mTransportFilters.back()->onReadCompleted(b);
        }

    }

    void ConnectedClientTransport::onTimedSendCompleted(int ret, int err)
    {
        mLastRequestSize += ret;
        mRunningTotalBytesSent += ret;

        RCF_UNUSED_VARIABLE(err);
        mTransportFilters.empty() ?
            onWriteCompleted(ret) :
            mTransportFilters.back()->onWriteCompleted(ret);
    }

    void ConnectedClientTransport::write(
        const std::vector<ByteBuffer> &byteBuffers)
    {
        mPostState = Writing;

        std::size_t maxOutgoingLen = getMaxOutgoingMessageLength();
        if ( maxOutgoingLen > 0)
        {
            std::size_t messageLen = lengthByteBuffers(byteBuffers);

            RCF_VERIFY(
                0 < messageLen && messageLen <= maxOutgoingLen,
                Exception(RcfError_OutgoingMessageLen, messageLen, maxOutgoingLen));
        }

        if (!mAsync)
        {
            std::size_t bytesWritten = implWrite(byteBuffers);
            RCF_UNUSED_VARIABLE(bytesWritten);
        }
        else
        {
            std::size_t bytesWritten = implWriteAsync(byteBuffers);
            RCF_UNUSED_VARIABLE(bytesWritten);
        }
    }

    // helper to disambiguate std::vector<ByteBuffer>::resize()
    typedef void (std::vector<ByteBuffer>::*PfnResize)(std::vector<ByteBuffer>::size_type);

    void clearByteBuffers( std::vector<ByteBuffer> &byteBuffers)
    {
        byteBuffers.resize(0);
    }

    void ConnectedClientTransport::onReadCompleted(
        const ByteBuffer &byteBuffer)
    {
        onTransitionCompleted(byteBuffer.getLength());
    }

    void ConnectedClientTransport::onWriteCompleted(
        std::size_t bytesTransferred)
    {
        onTransitionCompleted(bytesTransferred);
    }


    int ConnectedClientTransport::send(
        ClientTransportCallback &clientStub, 
        const std::vector<ByteBuffer> &data,
        unsigned int totalTimeoutMs)
    {
        RCF_LOG_4()(lengthByteBuffers(data))(totalTimeoutMs)
            << "ConnectionOrientedClientTransport - initiating timed send operation.";

        // This counter will be incremented as data passes to the network.
        mLastRequestSize = 0;

        if (totalTimeoutMs)
        {
            unsigned int startTimeMs = getCurrentTimeMs();
            mEndTimeMs = startTimeMs + totalTimeoutMs;
            mNoTimeout = false;
        }
        else
        {
            mEndTimeMs = 0;
            mNoTimeout = true;
        }

        mWriteBuffers.resize(0);
        std::copy(data.begin(), data.end(), std::back_inserter(mWriteBuffers));

        mpClientStub = &clientStub;

        mPreState = Writing;
        mWriteBufferPos = 0;
        transition();
        return 1;
    }

    int ConnectedClientTransport::receive(
        ClientTransportCallback &clientStub, 
        ByteBuffer &byteBuffer,
        unsigned int timeoutMs)
    {
        // This counter will be incremented as data passes from the network.
        mLastResponseSize = 0;

        if (timeoutMs)
        {
            mEndTimeMs = getCurrentTimeMs() + timeoutMs;
            mNoTimeout = false;
        }
        else
        {
            mEndTimeMs = 0;
            mNoTimeout = true;
        }

        mPreState = Reading;
        mReadBufferPos = 0;
        
        mpClientStub = &clientStub;

        mpClientStubReadBuffer = &byteBuffer;
        transition();

        return 1;
    }

    void ConnectedClientTransport::close()
    {
        {
            RecursiveLock lock(mOverlappedPtr->mMutex);
            ++mOverlappedPtr->mIndex;
        }
            
        if (!mClosed)
        {
            mpClientStub = NULL;
            implClose();
            mClosed = true;
        }
    }

    void ConnectedClientTransport::setTransportFilters(
        const std::vector<FilterPtr> &filters)
    {
        std::vector<FilterPtr> filtersTemp(filters);
        if (mWireFilters.size() > 0)
        {
            std::copy(
                mWireFilters.begin(),
                mWireFilters.end(),
                std::back_inserter(filtersTemp));
        }

        mTransportFilters.clear();

        if (filtersTemp.size() > 0)
        {
            mTransportFilters.push_back(
                FilterPtr(new ClientFilterProxy(*this, *filtersTemp.front(), true)));

            std::copy(
                filtersTemp.begin(),
                filtersTemp.end(),
                std::back_inserter(mTransportFilters));

            mTransportFilters.push_back(
                FilterPtr(new ClientFilterProxy(*this, *filtersTemp.back(), false)));

            RCF::connectFilters(mTransportFilters);
        }
    }

    void ConnectedClientTransport::getTransportFilters(
        std::vector<FilterPtr> &filters)
    {
        // TODO: keep the adapter filters out of mTransportFilters?
        if (mTransportFilters.empty())
        {
            filters.clear();
            return;
        }

        RCF_ASSERT(mTransportFilters.size() >= 2);

        std::vector<FilterPtr>::const_iterator iter0(mTransportFilters.begin());
        std::vector<FilterPtr>::const_iterator iter1(mTransportFilters.end());

        iter0 += 1;

        iter1 -= 1;
        iter1 -= mWireFilters.size();
        filters.assign(iter0, iter1);
    }

    void ConnectedClientTransport::getWireFilters(std::vector<FilterPtr> & filters)
    {
        filters = mWireFilters;
    }

    void ConnectedClientTransport::setWireFilters(const std::vector<FilterPtr> & wireFilters)
    {
        std::vector<FilterPtr> currentFilters;
        getTransportFilters(currentFilters);
        mWireFilters = wireFilters;
        setTransportFilters(currentFilters);
    }

    void ConnectedClientTransport::setMaxSendSize(std::size_t maxSendSize)
    {
        mMaxSendSize = maxSendSize;
    }

    std::size_t ConnectedClientTransport::getMaxSendSize()
    {
        return mMaxSendSize;
    }

    void ConnectedClientTransport::issueRead(
        const ByteBuffer &buffer, 
        std::size_t bytesToRead)
    {
        RCF_LOG_4()(bytesToRead) << "ConnectionOrientedClientTransport - initiating read.";

        mTransportFilters.empty() ?
            read(buffer, bytesToRead):
            mTransportFilters.front()->read(buffer, bytesToRead);
    }

    void ConnectedClientTransport::issueWrite(const std::vector<ByteBuffer> &byteBuffers)
    {
        RCF_LOG_4()(lengthByteBuffers(byteBuffers)) << "ConnectionOrientedClientTransport - initiating write.";

        mTransportFilters.empty() ?
            write(byteBuffers):
            mTransportFilters.front()->write(byteBuffers);
    }

    void ConnectedClientTransport::fireProgressEvent()
    {
        ClientStub * pClientStub = getTlsClientStubPtr();
        if ( pClientStub )
        {
            pClientStub->onPollingTimeout(true);
        }
    }

    void ConnectedClientTransport::transition()
    {
        switch (mPreState)
        {
        case Reading:
            if (mReadBufferPos == 0)
            {
                fireProgressEvent();

                if (mReadBufferPtr.get() == NULL || !mReadBufferPtr.unique())
                {
                    mReadBufferPtr.reset( new ReallocBuffer() );
                }
                mReadBufferPtr->resize(4);
                mReadBuffer = ByteBuffer(mReadBufferPtr);

                issueRead(ByteBuffer(mReadBuffer, mReadBufferPos), 4-mReadBufferPos);
            }
            else if (mReadBufferPos < 4)
            {
                issueRead(
                    ByteBuffer(mReadBuffer, mReadBufferPos), 
                    mReadBuffer.getLength() - mReadBufferPos);
            }
            else if (mReadBufferPos == 4)
            {
                std::uint32_t length = *(std::uint32_t*)mReadBuffer.getPtr();
                networkToMachineOrder(&length, sizeof(length), 1);

                if ( getMaxIncomingMessageLength())
                {
                    RCF_VERIFY(
                        0 < length && length <= getMaxIncomingMessageLength(),
                        Exception(RcfError_ClientMessageLength));
                }

                mReadBufferPtr->resize(4+length);
                mReadBuffer = ByteBuffer(mReadBufferPtr);

                issueRead(
                    ByteBuffer(mReadBuffer, mReadBufferPos), 
                    mReadBuffer.getLength() - mReadBufferPos);
            }
            else if (mReadBufferPos < mReadBuffer.getLength())
            {
                issueRead(
                    ByteBuffer(mReadBuffer, mReadBufferPos), 
                    mReadBuffer.getLength() - mReadBufferPos);

            }
            else if (mReadBufferPos == mReadBuffer.getLength())
            {
                fireProgressEvent();
                
                *mpClientStubReadBuffer = ByteBuffer(mReadBuffer, 4);
                mReadBuffer.clear();
                mRecursionState.clear();
                mpClientStub->onReceiveCompleted();
            }
            else
            {
                RCF_ASSERT_ALWAYS("");
            }

            break;

        case Writing:

            if ( mWriteBufferPos == 0 )
            {
                fireProgressEvent();
            }

            if (mWriteBufferPos < lengthByteBuffers(mWriteBuffers))
            {
                sliceByteBuffers(mSlicedByteBuffers, mWriteBuffers, mWriteBufferPos);

                issueWrite(mSlicedByteBuffers);
            }
            else 
            {
                fireProgressEvent();

                RCF_ASSERT( mWriteBufferPos == lengthByteBuffers(mWriteBuffers));
                mWriteBuffers.resize(0);
                mSlicedByteBuffers.resize(0);
                mpClientStub->onSendCompleted();
            }

            break;

        default:

            RCF_ASSERT_ALWAYS("");

        }
    }

    void ConnectedClientTransport::getProgressInfo(RemoteCallProgressInfo & info)
    {
        if ( mPreState == Connecting )
        {
            info.mPhase = Rcp_Connect;
            info.mBytesTotal = 0;
            info.mBytesTransferred = 0;
        }
        else if ( mPreState == Reading )
        {
            info.mPhase = Rcp_Receive;
            if ( mReadBuffer.getLength() <= 4 )
            {
                // At this point we still haven't received the length field from the server.
                info.mBytesTotal = 0;
                info.mBytesTransferred = 0;
            }
            else
            {
                info.mBytesTotal = mReadBuffer.getLength();
                info.mBytesTransferred = mReadBufferPos;
            }
        }
        else if ( mPreState == Writing )
        {
            info.mPhase = Rcp_Send;
            info.mBytesTotal = lengthByteBuffers(mWriteBuffers);
            info.mBytesTransferred = mWriteBufferPos;
        }
    }

    void ConnectedClientTransport::cancel()
    {
        RecursiveLock lock(mOverlappedPtr->mMutex);
        ++mOverlappedPtr->mIndex;
        if (mOverlappedPtr->mpTransport)
        {
            RCF_ASSERT(mOverlappedPtr->mpTransport == this);
            RCF::Exception e(RcfError_ClientCancel);
            mpClientStub->onError(e);
        }
    }

    void ConnectedClientTransport::setTimer(
        std::uint32_t timeoutMs,
        ClientTransportCallback *pClientStub)
    {
        mpClientStub = pClientStub;

        RecursiveLock lock(mOverlappedPtr->mMutex);

        mOverlappedPtr->mOpType = Wait;

        mAsioTimerPtr->expires_from_now(std::chrono::milliseconds(timeoutMs) );

        mAsioTimerPtr->async_wait( std::bind(
            &OverlappedAmi::onTimerExpired, 
            mOverlappedPtr, 
            mOverlappedPtr->mIndex,
            std::placeholders::_1));
    }

    void ConnectedClientTransport::onTimerExpired()
    {
        try
        {                
            mpClientStub->onTimerExpired();
        }
        catch(const std::exception &e)
        {
            mpClientStub->onError(e);
        }
    }

    void ConnectedClientTransport::onTransitionCompleted(
        std::size_t bytesTransferred)
    {
        if (mAsync)
        {
            onTransitionCompleted_(bytesTransferred);
        }
        else
        {
            applyRecursionLimiter(
                mRecursionState,
                &ConnectedClientTransport::onTransitionCompleted_,
                *this,
                bytesTransferred);
        }
    }

    void ConnectedClientTransport::onTransitionCompleted_(std::size_t bytesTransferred)
    {
        if (mPreState == Reading)
        {
            mReadBufferPos += bytesTransferred;
        }
        else if (mPreState == Writing)
        {
            mWriteBufferPos += bytesTransferred;
        }

        transition();
    }

    void ConnectedClientTransport::setSocketOpsMutex(MutexPtr mutexPtr)
    {
        mSocketOpsMutexPtr = mutexPtr;
    }

} // namespace RCF
