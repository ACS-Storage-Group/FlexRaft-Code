
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

#ifndef INCLUDE_RCF_THREADLOCALDATA_HPP
#define INCLUDE_RCF_THREADLOCALDATA_HPP

#include <codecvt>
#include <functional>
#include <locale>
#include <memory>
#include <vector>

#include <RCF/Config.hpp>
#include <RCF/Export.hpp>

#ifdef RCF_WINDOWS
#include <WinSock2.h>
#endif

#ifndef RCF_WINDOWS
struct iovec;
#endif

namespace RCF {

    class ObjectCache;
    class ClientStub;
    class RcfSession;
    class ThreadInfo;
    class UdpNetworkSession;
    class I_Future;
    class AmiNotification;
    class OverlappedAmi;
    class LogBuffers;
    class Filter;
    class FileUpload;
    class ByteBuffer;

    typedef std::shared_ptr<ClientStub>       ClientStubPtr;
    typedef std::shared_ptr<RcfSession>       RcfSessionPtr;
    typedef std::shared_ptr<ThreadInfo>       ThreadInfoPtr;
    typedef std::shared_ptr<UdpNetworkSession>  UdpNetworkSessionPtr;   
    typedef std::shared_ptr<OverlappedAmi>    OverlappedAmiPtr;
    typedef std::shared_ptr<LogBuffers>       LogBuffersPtr;
    typedef std::function<void(RcfSession&)> RcfSessionCallback;
    typedef std::shared_ptr<Filter>           FilterPtr;

    template<typename T1, typename T2>
    class RecursionState;

#ifndef RCF_WINDOWS
    typedef iovec WSABUF;
#endif

    class ThreadLocalData;
    ThreadLocalData &               getThreadLocalData();

    RCF_EXPORT ClientStub *         getTlsClientStubPtr();
    
    RCF_EXPORT void                 pushTlsClientStub(
                                        ClientStub * pClientStub);

    RCF_EXPORT void                 popTlsClientStub();

    RCF_EXPORT RcfSession *         getCurrentRcfSessionPtr();
    RCF_EXPORT RcfSession *         getTlsRcfSessionPtr();

    RCF_EXPORT void                 setTlsRcfSessionPtr(
                                        RcfSession * pRcfSession = NULL);

    RCF_EXPORT std::vector<RcfSession*>& getRcfSessionSentryStack();

    RCF_EXPORT ThreadInfoPtr        getTlsThreadInfoPtr();

    RCF_EXPORT void                 setTlsThreadInfoPtr(
                                        ThreadInfoPtr threadInfoPtr);

    RCF_EXPORT UdpNetworkSessionPtr   getTlsUdpNetworkSessionPtr();

    RCF_EXPORT void                 setTlsUdpNetworkSessionPtr(
                                        UdpNetworkSessionPtr udpNetworkSessionPtr);

    RCF_EXPORT RcfSession &         getTlsRcfSession();

    RecursionState<int, int> &      getTlsRcfSessionRecursionState();

    RCF_EXPORT AmiNotification &    getTlsAmiNotification();

    RCF_EXPORT LogBuffers &         getTlsLogBuffers();

    RCF_EXPORT std::vector< std::vector<RCF::ByteBuffer> * > &      
                                    getTlsCache(std::vector<RCF::ByteBuffer> *);

    RCF_EXPORT std::vector< std::vector<int> * > &                  
                                    getTlsCache(std::vector<int> *);

    RCF_EXPORT std::vector< std::vector<WSABUF> * > &               
                                    getTlsCache(std::vector<WSABUF> *);

    RCF_EXPORT std::vector< std::vector<FilterPtr> * > &                
                                    getTlsCache(std::vector<FilterPtr> *);

    RCF_EXPORT std::vector< std::vector<RcfSessionCallback> * > &   
                                    getTlsCache(std::vector<RcfSessionCallback> *);

    RCF_EXPORT std::vector< std::vector<FileUpload> * > &      
                                    getTlsCache(std::vector<FileUpload> *);

    std::wstring_convert<std::codecvt_utf8<wchar_t> > &
                                    getTlsUtf8Converter();

    template<typename T>
    class ThreadLocalCached
    {
    public:

        ThreadLocalCached() : mpt(NULL)
        {
            std::vector<T *> & tlsCache = getTlsCache( (T *) NULL);
            if (tlsCache.empty())
            {
                mpt = new T();
            }
            else
            {
                mpt = tlsCache.back();
                tlsCache.pop_back();
            }
            RCF_ASSERT(mpt->empty());
        }

        ~ThreadLocalCached()
        {
            mpt->clear();
            std::vector<T *> & tlsCache = getTlsCache( (T *) NULL);
            tlsCache.push_back(mpt);
            mpt = NULL;
        }

        T & get()
        {
            return *mpt;
        }

    private:

        T * mpt;
    };


} // namespace RCF

#endif // ! INCLUDE_RCF_THREADLOCALDATA_HPP
