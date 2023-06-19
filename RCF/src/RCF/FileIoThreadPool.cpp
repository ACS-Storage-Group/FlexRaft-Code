
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

#include <RCF/FileIoThreadPool.hpp>

#include <RCF/FileStream.hpp>
#include <RCF/InitDeinit.hpp>
#include <RCF/ThreadLocalData.hpp>
#include <RCF/Tools.hpp>
#include <RCF/Log.hpp>

#include <chrono>

namespace RCF {

    FileIoThreadPool::FileIoThreadPool() : 
        mSerializeFileIo(false),
        mThreadPool(1,10) 
    {
        mThreadPool.setThreadName("RCF Async File IO");
        mThreadPool.setThreadIdleTimeoutMs(30*1000);
        mThreadPool.setReserveLastThread(false);

        mThreadPool.setTask( std::bind(
            &FileIoThreadPool::ioTask,
            this));

        mThreadPool.setStopFunctor( std::bind(
            &FileIoThreadPool::stopIoTask,
            this));
    }

    FileIoThreadPool::~FileIoThreadPool()
    {
        RCF_DTOR_BEGIN
            mThreadPool.stop();
        RCF_DTOR_END
    }

    void FileIoThreadPool::setSerializeFileIo(bool serializeFileIo)
    {
        mSerializeFileIo = serializeFileIo;
    }

    void FileIoThreadPool::stop()
    {
        mThreadPool.stop();
    }

    void FileIoThreadPool::registerOp(FileIoRequestPtr opPtr)
    {
        RCF::Lock lock(mOpsMutex);

        // Lazy start of the thread pool.
        if (!mThreadPool.isStarted())
        {
            mThreadPool.start();
        }

        if (    std::find(mOpsQueued.begin(), mOpsQueued.end(), opPtr) 
            !=  mOpsQueued.end())
        {
            RCF_ASSERT_ALWAYS("");
        }
        else if (       std::find(mOpsInProgress.begin(), mOpsInProgress.end(), opPtr) 
                    !=  mOpsInProgress.end())
        {
            RCF_ASSERT_ALWAYS("");
        }
        else
        {
            mOpsQueued.push_back(opPtr);
            mOpsCondition.notify_all();
        }
    }

    void FileIoThreadPool::unregisterOp(FileIoRequestPtr opPtr)
    {
        RCF::Lock lock(mOpsMutex);
        RCF::eraseRemove(mOpsQueued, opPtr);
        RCF::eraseRemove(mOpsInProgress, opPtr);
    }

    bool FileIoThreadPool::ioTask()
    {
        FileIoRequestPtr opPtr;

        {
            RCF::Lock lock(mOpsMutex);
            while (mOpsQueued.empty() && !mThreadPool.shouldStop())
            {
                using namespace std::chrono_literals;
                mOpsCondition.wait_for(lock, 1000ms);
            }
            if (mOpsQueued.empty() || mThreadPool.shouldStop())
            {
                return false;
            }
            RCF_ASSERT(mOpsQueued.size() > 0);
            mOpsInProgress.push_back( mOpsQueued.front() );
            mOpsQueued.pop_front();
            opPtr = mOpsInProgress.back();
        }

        RCF::ThreadInfoPtr threadInfoPtr = RCF::getTlsThreadInfoPtr();
        if (threadInfoPtr)
        {
            threadInfoPtr->notifyBusy();
        }

        // This is the part that blocks.
        opPtr->doTransfer();

        // Unregister op.
        unregisterOp(opPtr);

        // Notify completion.
        {
            RCF::Lock lock(mCompletionMutex);
            opPtr->mCompleted = true;
            mCompletionCondition.notify_all();
        }

        return false;
    }

    void FileIoThreadPool::stopIoTask()
    {
        RCF::Lock lock(mOpsMutex);
        mOpsCondition.notify_all();
    }

    FileIoRequest::FileIoRequest() :
        mFts( getFileIoThreadPool() ),
        mBytesTransferred(0),
        mInitiated(false),
        mCompleted(true)
    {
        RCF_LOG_4() << "FileIoRequest::FileIoRequest()";
    }

    FileIoRequest::~FileIoRequest()
    {
        RCF_LOG_4() << "FileIoRequest::~FileIoRequest()";
    }

    bool FileIoRequest::isInitiated()
    {
        RCF_LOG_4() << "FileIoRequest::isInitiated()";

        RCF::Lock lock(mFts.mCompletionMutex);
        return mInitiated;
    }

    bool FileIoRequest::isCompleted()
    {
        RCF_LOG_4() << "FileIoRequest::isCompleted()";

        RCF::Lock lock(mFts.mCompletionMutex);
        return mCompleted;
    }

    void FileIoRequest::complete()
    {
        RCF_LOG_4() << "FileIoRequest::complete() - entry";

        RCF::Lock lock(mFts.mCompletionMutex);
        while (!mCompleted)
        {
            using namespace std::chrono_literals;
            mFts.mCompletionCondition.wait_for(lock, 1000ms);
        }
        mInitiated = false;

        RCF_LOG_4() << "FileIoRequest::complete() - exit";
    }

    void FileIoRequest::initiateRead(FileHandlePtr finPtr, RCF::ByteBuffer buffer)
    {
        RCF_LOG_4()(finPtr.get())((void*)buffer.getPtr())(buffer.getLength()) << "FileIoRequest::read()";

        mFinPtr = finPtr;
        mFoutPtr.reset();
        mBuffer = buffer;
        mBytesTransferred = 0;
        mInitiated = true;
        mCompleted = false;

        mFts.registerOp( shared_from_this() );

        // For debugging purposes, we can wait in this function until the file I/O is completed.
        if (mFts.mSerializeFileIo)
        {
            RCF::Lock lock(mFts.mCompletionMutex);
            while (!mCompleted)
            {
                mFts.mCompletionCondition.wait(lock);
            }
        }
    }

    void FileIoRequest::initateWrite(FileHandlePtr foutPtr, RCF::ByteBuffer buffer)
    {
        RCF_LOG_4()(foutPtr.get())((void*)buffer.getPtr())(buffer.getLength()) << "FileIoRequest::write()";

        mFinPtr.reset();
        mFoutPtr = foutPtr;
        mBuffer = buffer;
        mBytesTransferred = 0;
        mInitiated = true;
        mCompleted = false;

        mFts.registerOp( shared_from_this() );

        // For debugging purposes, we can wait in this function until the file I/O is completed.
        if (mFts.mSerializeFileIo)
        {
            RCF::Lock lock(mFts.mCompletionMutex);
            while (!mCompleted)
            {
                mFts.mCompletionCondition.wait(lock);
            }
        }
    }

    void FileIoRequest::doTransfer()
    {
        if (mFinPtr)
        {
            RCF_LOG_4()(mBuffer.getLength()) << "FileIoRequest::doTransfer() - initiate read.";

            mBytesTransferred = mFinPtr->read(mBuffer);
            mFinPtr.reset();

            RCF_LOG_4()(mBytesTransferred) << "FileIoRequest::doTransfer() - read complete.";
        }
        else if (mFoutPtr)
        {
            RCF_LOG_4()(mBuffer.getLength()) << "FileIoRequest::doTransfer() - initiate write.";

            mBytesTransferred = mFoutPtr->write(mBuffer);
            RCF_ASSERT(mBytesTransferred == mBuffer.getLength());
            mFoutPtr->flush();
            mFoutPtr.reset();

            RCF_LOG_4()(mBytesTransferred) << "FileIoRequest::doTransfer() - write complete.";
        }
        else
        {
            RCF_ASSERT_ALWAYS("");
            mBytesTransferred = 0;
        }
    }

    std::uint64_t FileIoRequest::getBytesTransferred()
    {
        RCF_LOG_4()(mBytesTransferred) << "FileIoRequest::getBytesTransferred()";

        return mBytesTransferred;
    }

    static FileIoThreadPool * gpFileIoThreadPool = NULL;

    void initFileIoThreadPool()
    {
        gpFileIoThreadPool = new FileIoThreadPool();
    }

    void deinitFileIoThreadPool()
    {
        delete gpFileIoThreadPool; 
        gpFileIoThreadPool = NULL;
    }

    FileIoThreadPool & getFileIoThreadPool()
    {
        FileIoThreadPool * pFileIoThreadPool = gpFileIoThreadPool;
        return *pFileIoThreadPool;
    }

} // namespace RCF
