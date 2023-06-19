
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

#include <RCF/ThreadLocalData.hpp>

#include <RCF/AmiThreadPool.hpp>
#include <RCF/ByteBuffer.hpp>
#include <RCF/Exception.hpp>
#include <RCF/InitDeinit.hpp>
#include <RCF/OverlappedAmi.hpp>
#include <RCF/RecursionLimiter.hpp>
#include <RCF/Log.hpp>

#if RCF_FEATURE_FILETRANSFER==1
#include <RCF/FileUpload.hpp>
#endif

namespace RCF {

    class ThreadLocalData;
    thread_local std::unique_ptr<ThreadLocalData> gTldPtr;

    class ThreadLocalData
    {
        
    public:
        
        ThreadLocalData() : 
            mpCurrentRcfSession(NULL)
        {
        }

        ~ThreadLocalData()
        {
            RCF_DTOR_BEGIN

                for(std::size_t i=0; i<mByteBufferVecCache.size(); ++i)
                {
                    delete mByteBufferVecCache[i];
                    mByteBufferVecCache[i] = NULL;
                }

                for(std::size_t i=0; i<mIntVecCache.size(); ++i)
                {
                    delete mIntVecCache[i];
                    mIntVecCache[i] = NULL;
                }

                for(std::size_t i=0; i<mWsabufVecCache.size(); ++i)
                {
                    delete mWsabufVecCache[i];
                    mWsabufVecCache[i] = NULL;
                }

                for(std::size_t i=0; i<mFilterVecPtr.size(); ++i)
                {
                    delete mFilterVecPtr[i];
                    mFilterVecPtr[i] = NULL;
                }

                for(std::size_t i=0; i<mRcfSessionCbVecCache.size(); ++i)
                {
                    delete mRcfSessionCbVecCache[i];
                    mRcfSessionCbVecCache[i] = NULL;
                }

#if RCF_FEATURE_FILETRANSFER==1
                for(std::size_t i=0; i<mFileUploadVecCache.size(); ++i)
                {
                    delete mFileUploadVecCache[i];
                    mFileUploadVecCache[i] = NULL;
                }
#endif

            RCF_DTOR_END
        }

        std::vector<ClientStub *>       mCurrentClientStubs;
        RcfSession *                    mpCurrentRcfSession;
        ThreadInfoPtr                   mThreadInfoPtr;
        UdpNetworkSessionPtr            mUdpNetworkSessionPtr;
        RecursionState<int, int>        mRcfSessionRecursionState;
        AmiNotification                 mAmiNotification;
        OverlappedAmiPtr                mOverlappedPtr;
        LogBuffers                      mLogBuffers;
        std::wstring_convert<std::codecvt_utf8<wchar_t> > mUtf8Converter;

        // For ThreadLocalCached<>.
        std::vector< std::vector<ByteBuffer> * >            mByteBufferVecCache;
        std::vector< std::vector<int> * >                   mIntVecCache;
        std::vector< std::vector<WSABUF> * >                mWsabufVecCache;
        std::vector< std::vector<FilterPtr> * >             mFilterVecPtr;
        std::vector< std::vector<RcfSessionCallback> * >    mRcfSessionCbVecCache;
        std::vector< std::vector<FileUpload> * >            mFileUploadVecCache;

        std::vector<RcfSession *>                           mRcfSessionSentryStack;
    };


    void initThreadLocalData()
    {
    }

    void deinitThreadLocalData()
    {
        // Only deletes TLD data for the current thread.
        gTldPtr.reset();
    }


    ThreadLocalData &getThreadLocalData()
    {       
        if ( !gTldPtr )
        { 
            gTldPtr.reset(new ThreadLocalData());
        }
        return *gTldPtr;
    }

    // access to the various thread local entities

    ClientStub * getTlsClientStubPtr()
    {
        ThreadLocalData & tld = getThreadLocalData();
        if (!tld.mCurrentClientStubs.empty())
        {
            return tld.mCurrentClientStubs.back();
        }
        return NULL;
    }

    void pushTlsClientStub(ClientStub * pClientStub)
    {
        ThreadLocalData & tld = getThreadLocalData();
        tld.mCurrentClientStubs.push_back(pClientStub);
    }

    void popTlsClientStub()
    {
        ThreadLocalData & tld = getThreadLocalData();
        tld.mCurrentClientStubs.pop_back();
    }

    RcfSession * getCurrentRcfSessionPtr()
    {
        return getTlsRcfSessionPtr();
    }

    RcfSession * getTlsRcfSessionPtr()
    {
        ThreadLocalData & tld = getThreadLocalData();
        return tld.mpCurrentRcfSession;
    }

    void setTlsRcfSessionPtr(RcfSession * pRcfSessionPtr)
    {
        ThreadLocalData & tld = getThreadLocalData();
        tld.mpCurrentRcfSession = pRcfSessionPtr;
    }

    std::vector<RcfSession*>& getRcfSessionSentryStack()
    {
        ThreadLocalData & tld = getThreadLocalData();
        return tld.mRcfSessionSentryStack;
    }

    ThreadInfoPtr getTlsThreadInfoPtr()
    {
        ThreadLocalData & tld = getThreadLocalData();
        return tld.mThreadInfoPtr;
    }

    void setTlsThreadInfoPtr(ThreadInfoPtr threadInfoPtr)
    {
        ThreadLocalData & tld = getThreadLocalData();
        tld.mThreadInfoPtr = threadInfoPtr;
    }

    UdpNetworkSessionPtr getTlsUdpNetworkSessionPtr()
    {
        ThreadLocalData & tld = getThreadLocalData();
        return tld.mUdpNetworkSessionPtr;
    }

    void setTlsUdpNetworkSessionPtr(UdpNetworkSessionPtr udpNetworkSessionPtr)
    {
        ThreadLocalData & tld = getThreadLocalData();
        tld.mUdpNetworkSessionPtr = udpNetworkSessionPtr;
    }

    RcfSession & getCurrentRcfSession()
    {
        return getTlsRcfSession();
    }

    RcfSession & getTlsRcfSession()
    {
        return *getTlsRcfSessionPtr();
    }

    RecursionState<int, int> & getTlsRcfSessionRecursionState()
    {
        ThreadLocalData & tld = getThreadLocalData();
        return tld.mRcfSessionRecursionState;
    }

    AmiNotification & getTlsAmiNotification()
    {
        ThreadLocalData & tld = getThreadLocalData();
        return tld.mAmiNotification;
    }

    LogBuffers & getTlsLogBuffers()
    {
        ThreadLocalData & tld = getThreadLocalData();
        return tld.mLogBuffers;
    }

    std::wstring_convert<std::codecvt_utf8<wchar_t> > & getTlsUtf8Converter()
    {
        ThreadLocalData & tld = getThreadLocalData();
        return tld.mUtf8Converter;
    }

    std::vector< std::vector<ByteBuffer> * > &      
        getTlsCache(std::vector<ByteBuffer> *)
    {
        ThreadLocalData & tld = getThreadLocalData();
        return tld.mByteBufferVecCache;
    }

    std::vector< std::vector<int> * > &                 
        getTlsCache(std::vector<int> *)
    {
        ThreadLocalData & tld = getThreadLocalData();
        return tld.mIntVecCache;
    }

    std::vector< std::vector<WSABUF> * > &              
        getTlsCache(std::vector<WSABUF> *)
    {
        ThreadLocalData & tld = getThreadLocalData();
        return tld.mWsabufVecCache;
    }

    std::vector< std::vector<FilterPtr> * > &               
        getTlsCache(std::vector<FilterPtr> *)
    {
        ThreadLocalData & tld = getThreadLocalData();
        return tld.mFilterVecPtr;
    }

    std::vector< std::vector<RcfSessionCallback> * > &  
        getTlsCache(std::vector<RcfSessionCallback> *)
    {
        ThreadLocalData & tld = getThreadLocalData();
        return tld.mRcfSessionCbVecCache;
    }

    std::vector< std::vector<FileUpload> * > &  
        getTlsCache(std::vector<FileUpload> *)
    {
        ThreadLocalData & tld = getThreadLocalData();
        return tld.mFileUploadVecCache;
    }

} // namespace RCF
