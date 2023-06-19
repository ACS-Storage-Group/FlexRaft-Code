
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

#ifndef INCLUDE_RCF_FILEIOTHREADPOOL_HPP
#define INCLUDE_RCF_FILEIOTHREADPOOL_HPP

#include <deque>
#include <memory>

#include <RCF/ByteBuffer.hpp>
#include <RCF/Exception.hpp>
#include <RCF/Export.hpp>
#include <RCF/ThreadLibrary.hpp>
#include <RCF/ThreadPool.hpp>

namespace RCF {
    
    class FileIoRequest;
    typedef std::shared_ptr<FileIoRequest> FileIoRequestPtr;

    class FileHandle;
    typedef std::shared_ptr<FileHandle> FileHandlePtr;
    
    class RCF_EXPORT FileIoThreadPool
    {
    public:
        FileIoThreadPool();
        ~FileIoThreadPool();

        void stop();
        void registerOp(FileIoRequestPtr opPtr);
        void unregisterOp(FileIoRequestPtr opPtr);

        void setSerializeFileIo(bool serializeFileIo);

    private:

        friend class FileIoRequest;

        bool ioTask();
        void stopIoTask();

        bool                            mSerializeFileIo;
    
        RCF::Mutex                      mOpsMutex;
        RCF::Condition                  mOpsCondition;
        std::deque< FileIoRequestPtr >  mOpsQueued;
        std::deque< FileIoRequestPtr >  mOpsInProgress;
        
        RCF::ThreadPool                 mThreadPool;
    
        RCF::Mutex                      mCompletionMutex;
        RCF::Condition                  mCompletionCondition;
    };
   
    class RCF_EXPORT FileIoRequest : 
        public std::enable_shared_from_this<FileIoRequest>
    {
    public:
        FileIoRequest();    
        ~FileIoRequest();   

        bool isInitiated();
        bool isCompleted();
        void complete();
        void initiateRead(FileHandlePtr finPtr, RCF::ByteBuffer buffer);
        void initateWrite(FileHandlePtr foutPtr, RCF::ByteBuffer buffer);

        std::uint64_t getBytesTransferred();

    private:

        friend class FileIoThreadPool;

        void doTransfer();
    
        FileIoThreadPool &                  mFts;
    
        FileHandlePtr                       mFinPtr;
        FileHandlePtr                       mFoutPtr;
    
        RCF::ByteBuffer                     mBuffer;
        std::uint64_t                       mBytesTransferred;
        bool                                mInitiated;
        bool                                mCompleted;
        RCF::Exception                      mError;
    };

    RCF_EXPORT FileIoThreadPool & getFileIoThreadPool();


} // namespace RCF

#endif // ! INCLUDE_RCF_FILEIOTHREADPOOL_HPP
