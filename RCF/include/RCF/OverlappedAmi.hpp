
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

#ifndef INCLUDE_RCF_OVERLAPPEDAMI_HPP
#define INCLUDE_RCF_OVERLAPPEDAMI_HPP

#include <functional>
#include <memory>

#include <RCF/Asio.hpp>
#include <RCF/Enums.hpp>
#include <RCF/ThreadLibrary.hpp>

namespace RCF {

    class RCF_EXPORT AmiNotification
    {
    public:

        typedef std::function<void()> Cb;

        // Need mutexPtr so that the mutex doesn't get destroyed before the lock.
        void set(Cb cb, LockPtr lockPtr, MutexPtr mutexPtr);
        void run();
        void clear();

    private:
        Cb          mCb;
        MutexPtr   mMutexPtr;
        LockPtr    mLockPtr;
    };


    class OverlappedAmi;
    typedef std::shared_ptr<OverlappedAmi> OverlappedAmiPtr;

    class ConnectedClientTransport;

    class OverlappedAmi : 
        public std::enable_shared_from_this<OverlappedAmi>
    {
    public:

        OverlappedAmi(ConnectedClientTransport *pTcpClientTransport);

        ~OverlappedAmi();

        void onCompletion(
            std::size_t index,
            const AsioErrorCode & ec, 
            std::size_t bytesTransferred);

        void onTimerExpired(
            std::size_t index,
            const AsioErrorCode & ec);

        void ensureLifetime(const ByteBuffer & byteBuffer);
        void ensureLifetime(const std::vector<ByteBuffer> & byteBuffers);

        // TODO: should make these private.

        RecursiveMutex                      mMutex;
        ConnectedClientTransport *          mpTransport;
        std::size_t                         mIndex;
        AsyncOpType                         mOpType;

    private:

        // This is the underlying memory for the asio buffers. This memory has to
        // be held on to, until the async op completes.
        std::vector<ByteBuffer>             mByteBuffers;   
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_OVERLAPPEDAMI_HPP
