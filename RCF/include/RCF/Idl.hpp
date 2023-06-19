
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

#ifndef INCLUDE_RCF_IDL_HPP
#define INCLUDE_RCF_IDL_HPP

#include <memory>
#include <type_traits>

#include <RCF/Config.hpp>
#include <RCF/Exception.hpp>
#include <RCF/Future.hpp>
#include <RCF/RcfClient.hpp>
#include <RCF/ServerStub.hpp>
#include <RCF/Tools.hpp> // RCF_ASSERT etc, used in RCF_BEGIN().

namespace RCF
{
    class ClientStub;
    typedef std::shared_ptr<ClientStub> ClientStubPtr;
}

#define RCF_BEGIN(InterfaceT, Name)                                         \
                                                                            \
    template<typename T>                                                    \
    class RcfClient;                                                        \
                                                                            \
    class InterfaceT                                                        \
    {                                                                       \
    public:                                                                 \
        typedef RcfClient<InterfaceT> RcfClientT;                           \
        static std::string getInterfaceName()                               \
        {                                                                   \
            std::string interfaceName(Name);                                \
            if (interfaceName.empty())                                      \
            {                                                               \
                interfaceName = #InterfaceT;                                \
            }                                                               \
            return interfaceName;                                           \
        }                                                                   \
    };                                                                      \
                                                                            \
    template<>                                                              \
    class RcfClient< InterfaceT > : public ::RCF::I_RcfClient               \
    {                                                                       \
    public:                                                                 \
                                                                            \
        RcfClient() :                                                       \
            I_RcfClient( ::RCF::getInterfaceName( (InterfaceT *) NULL) )    \
        {                                                                   \
        }                                                                   \
                                                                            \
        /* Server side constructor. */                                      \
                                                                            \
        template<typename RefWrapperT>                                      \
        RcfClient(                                                          \
            ::RCF::ServerBindingPtr         serverStubPtr,                  \
            RefWrapperT                     refWrapper,                     \
            RCF::TrueType *) :                                              \
                I_RcfClient(                                                \
                    ::RCF::getInterfaceName( (InterfaceT *) NULL),          \
                    serverStubPtr)                                          \
        {                                                                   \
            getServerStub().addServerMethods(*this, refWrapper);            \
        }                                                                   \
                                                                            \
        /* Client side constructors. */                                     \
                                                                            \
        RcfClient(                                                          \
            const ::RCF::Endpoint &         endpoint,                       \
            const std::string &             targetName = "") :              \
                I_RcfClient(                                                \
                    ::RCF::getInterfaceName( (InterfaceT *) NULL),          \
                    endpoint,                                               \
                    targetName)                                             \
        {                                                                   \
        }                                                                   \
                                                                            \
        RcfClient(                                                          \
            ::RCF::ClientTransportUniquePtr clientTransportUniquePtr,       \
            const std::string &             targetName = "") :              \
                I_RcfClient(                                                \
                    ::RCF::getInterfaceName( (InterfaceT *) NULL),          \
                    std::move(clientTransportUniquePtr),                    \
                    targetName)                                             \
        {                                                                   \
        }                                                                   \
                                                                            \
        RcfClient(                                                          \
            const ::RCF::ClientStub &       clientStub,                     \
            const std::string &             targetName = "") :              \
                I_RcfClient(                                                \
                    ::RCF::getInterfaceName( (InterfaceT *) NULL),          \
                    clientStub,                                             \
                    targetName)                                             \
        {                                                                   \
        }                                                                   \
                                                                            \
        RcfClient(const RcfClient &                 rhs) :                  \
                I_RcfClient(                                                \
                    ::RCF::getInterfaceName( (InterfaceT *) NULL),          \
                    rhs)                                                    \
        {                                                                   \
        }                                                                   \
                                                                            \
        RcfClient & operator=(const RcfClient &     rhs)                    \
        {                                                                   \
            I_RcfClient::operator =(rhs);                                   \
            return *this;                                                   \
        }                                                                   \
                                                                            \
        /* Move constructors. */                                            \
                                                                            \
        RcfClient(::RCF::I_RcfClient &&             rhs) :                  \
            I_RcfClient(                                                    \
                ::RCF::getInterfaceName( (InterfaceT *) NULL),              \
                std::move(rhs))                                             \
        {                                                                   \
        }                                                                   \
                                                                            \
        RcfClient(RcfClient &&                      rhs) :                  \
            I_RcfClient(                                                    \
                ::RCF::getInterfaceName( (InterfaceT *) NULL),              \
                std::move(rhs))                                             \
        {                                                                   \
        }                                                                   \
                                                                            \
        /* Move assignment. */                                              \
                                                                            \
        RcfClient & operator=(I_RcfClient &&        rhs)                    \
        {                                                                   \
            I_RcfClient::operator =(std::move(rhs));                        \
            return *this;                                                   \
        }                                                                   \
                                                                            \
                                                                            \
        RcfClient & operator=(RcfClient &&          rhs)                    \
        {                                                                   \
            I_RcfClient::operator =(std::move(rhs));                        \
            return *this;                                                   \
        }                                                                   \
                                                                            \
        /* Destructor. */                                                   \
                                                                            \
        ~RcfClient()                                                        \
        {                                                                   \
        }                                                                   \
                                                                            \
    private:                                                                \
                                                                            \
        template<typename N, typename T>                                    \
        void callMethod(                                                    \
            const N &,                                                      \
            ::RCF::RcfSession &,                                            \
            const T &)                                                      \
        {                                                                   \
            ::RCF::Exception e(RCF::RcfError_FnId, N::value);               \
            RCF_THROW(e);                                                   \
        }                                                                   \
                                                                            \
        const char * getFunctionName(...)                                   \
        {                                                                   \
            RCF_ASSERT_ALWAYS("getFunctionName() - invalid function id");   \
            return "";                                                      \
        }                                                                   \
                                                                            \
        const char * getArity(...)                                          \
        {                                                                   \
            return "";                                                      \
        }                                                                   \
                                                                            \
        typedef RcfClient< InterfaceT > ThisT;                              \
        typedef ::RCF::Dummy<ThisT>     DummyThisT;                         \
                                                                            \
        friend class ::RCF::StubAccess;                                     \
        friend class ::RCF::ServerBinding;                                  \
                                                                            \
        friend ::RCF::default_ RCF_make_next_dispatch_id_func(              \
            DummyThisT *,                                                   \
            ThisT *,                                                        \
            ...);                                                           \
    public:                                                                 \
        typedef InterfaceT              Interface;
        


#define RCF_END( InterfaceT )                                               \
    };

#define RCF_METHOD_PLACEHOLDER()                                            \
    RCF_METHOD_PLACEHOLDER_(RCF_MAKE_UNIQUE_ID(PlaceHolder, V0))

#define RCF_METHOD_PLACEHOLDER_(id)                                         \
    public:                                                                 \
        RCF_MAKE_NEXT_DISPATCH_ID(id)                                       \
    private:

// Generated file with all the RCF_METHOD macros.
#include "RcfMethodGen.hpp"

// Macro machinery for compile-time method ID's.

// RCF_MAKE_UNIQUE_ID

static_assert( sizeof(RCF::defined_) != sizeof(RCF::default_), "Internal data structure mismatch.");

#define RCF_PP_CAT_I(a,b)                       a##b
#define RCF_PP_CAT(a,b)                         RCF_PP_CAT_I(a,b)
#define RCF_PP_CAT_4(arg1, arg2, arg3, arg4)    RCF_PP_CAT( arg1, RCF_PP_CAT( arg2, RCF_PP_CAT(arg3, arg4) ) )

#define RCF_MAKE_UNIQUE_ID(func, sig)           RCF_PP_CAT_4(rcf_unique_id_, func, sig, __LINE__)

#define RCF_MAKE_NEXT_DISPATCH_ID(next_dispatch_id)                                                     \
    RCF_ADVANCE_STATIC_ID(next_dispatch_id, RCF_make_next_dispatch_id_func, DummyThisT, ThisT, friend)

#if RCF_MAX_METHOD_COUNT <= 35

#define RCF_ADVANCE_STATIC_ID(next_static_id, helper_func, T1, T2, friend_or_not)                                                                 \
    typedef                                                                                                                                       \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 0> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 1> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 2> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 3> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 4> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 5> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 6> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 7> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 8> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 9> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<10> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<11> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<12> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<13> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<14> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<15> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<16> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<17> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<18> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<19> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<20> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<21> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<22> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<23> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<24> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<25> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<26> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<27> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<28> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<29> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<30> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<31> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<32> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<33> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<34> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::Int<35>,                                                                                                                         \
    RCF::Int<34> >::type,                                                                                                                 \
    RCF::Int<33> >::type,                                                                                                                 \
    RCF::Int<32> >::type,                                                                                                                 \
    RCF::Int<31> >::type,                                                                                                                 \
    RCF::Int<30> >::type,                                                                                                                 \
    RCF::Int<29> >::type,                                                                                                                 \
    RCF::Int<28> >::type,                                                                                                                 \
    RCF::Int<27> >::type,                                                                                                                 \
    RCF::Int<26> >::type,                                                                                                                 \
    RCF::Int<25> >::type,                                                                                                                 \
    RCF::Int<24> >::type,                                                                                                                 \
    RCF::Int<23> >::type,                                                                                                                 \
    RCF::Int<22> >::type,                                                                                                                 \
    RCF::Int<21> >::type,                                                                                                                 \
    RCF::Int<20> >::type,                                                                                                                 \
    RCF::Int<19> >::type,                                                                                                                 \
    RCF::Int<18> >::type,                                                                                                                 \
    RCF::Int<17> >::type,                                                                                                                 \
    RCF::Int<16> >::type,                                                                                                                 \
    RCF::Int<15> >::type,                                                                                                                 \
    RCF::Int<14> >::type,                                                                                                                 \
    RCF::Int<13> >::type,                                                                                                                 \
    RCF::Int<12> >::type,                                                                                                                 \
    RCF::Int<11> >::type,                                                                                                                 \
    RCF::Int<10> >::type,                                                                                                                 \
    RCF::Int< 9> >::type,                                                                                                                 \
    RCF::Int< 8> >::type,                                                                                                                 \
    RCF::Int< 7> >::type,                                                                                                                 \
    RCF::Int< 6> >::type,                                                                                                                 \
    RCF::Int< 5> >::type,                                                                                                                 \
    RCF::Int< 4> >::type,                                                                                                                 \
    RCF::Int< 3> >::type,                                                                                                                 \
    RCF::Int< 2> >::type,                                                                                                                 \
    RCF::Int< 1> >::type,                                                                                                                 \
    RCF::Int< 0> >::type next_static_id;                                                                                                  \
    friend_or_not RCF::defined_ helper_func(T1 *, T2 *, next_static_id *);


#define RCF_CURRENT_STATIC_ID(current_static_id, helper_func, T1, T2)                                                                             \
    typedef                                                                                                                                       \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 0> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 1> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 2> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 3> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 4> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 5> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 6> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 7> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 8> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 9> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<10> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<11> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<12> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<13> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<14> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<15> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<16> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<17> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<18> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<19> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<20> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<21> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<22> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<23> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<24> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<25> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<26> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<27> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<28> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<29> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<30> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<31> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<32> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<33> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<34> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::Int<34>,                                                                                                                         \
    RCF::Int<33> >::type,                                                                                                                 \
    RCF::Int<32> >::type,                                                                                                                 \
    RCF::Int<31> >::type,                                                                                                                 \
    RCF::Int<30> >::type,                                                                                                                 \
    RCF::Int<29> >::type,                                                                                                                 \
    RCF::Int<28> >::type,                                                                                                                 \
    RCF::Int<27> >::type,                                                                                                                 \
    RCF::Int<26> >::type,                                                                                                                 \
    RCF::Int<25> >::type,                                                                                                                 \
    RCF::Int<24> >::type,                                                                                                                 \
    RCF::Int<23> >::type,                                                                                                                 \
    RCF::Int<22> >::type,                                                                                                                 \
    RCF::Int<21> >::type,                                                                                                                 \
    RCF::Int<20> >::type,                                                                                                                 \
    RCF::Int<19> >::type,                                                                                                                 \
    RCF::Int<18> >::type,                                                                                                                 \
    RCF::Int<17> >::type,                                                                                                                 \
    RCF::Int<16> >::type,                                                                                                                 \
    RCF::Int<15> >::type,                                                                                                                 \
    RCF::Int<14> >::type,                                                                                                                 \
    RCF::Int<13> >::type,                                                                                                                 \
    RCF::Int<12> >::type,                                                                                                                 \
    RCF::Int<11> >::type,                                                                                                                 \
    RCF::Int<10> >::type,                                                                                                                 \
    RCF::Int< 9> >::type,                                                                                                                 \
    RCF::Int< 8> >::type,                                                                                                                 \
    RCF::Int< 7> >::type,                                                                                                                 \
    RCF::Int< 6> >::type,                                                                                                                 \
    RCF::Int< 5> >::type,                                                                                                                 \
    RCF::Int< 4> >::type,                                                                                                                 \
    RCF::Int< 3> >::type,                                                                                                                 \
    RCF::Int< 2> >::type,                                                                                                                 \
    RCF::Int< 1> >::type,                                                                                                                 \
    RCF::Int< 0> >::type,                                                                                                                 \
    RCF::Int<-1> >::type current_static_id;


#elif RCF_MAX_METHOD_COUNT <= 100

#define RCF_ADVANCE_STATIC_ID(next_static_id, helper_func, T1, T2, friend_or_not)                                                                 \
    typedef                                                                                                                                       \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 0> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 1> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 2> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 3> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 4> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 5> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 6> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 7> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 8> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 9> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<10> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<11> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<12> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<13> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<14> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<15> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<16> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<17> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<18> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<19> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<20> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<21> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<22> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<23> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<24> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<25> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<26> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<27> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<28> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<29> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<30> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<31> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<32> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<33> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<34> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<35> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<36> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<37> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<38> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<39> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<40> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<41> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<42> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<43> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<44> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<45> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<46> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<47> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<48> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<49> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<50> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<51> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<52> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<53> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<54> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<55> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<56> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<57> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<58> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<59> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<60> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<61> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<62> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<63> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<64> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<65> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<66> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<67> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<68> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<69> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<70> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<71> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<72> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<73> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<74> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<75> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<76> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<77> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<78> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<79> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<80> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<81> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<82> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<83> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<84> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<85> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<86> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<87> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<88> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<89> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<90> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<91> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<92> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<93> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<94> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<95> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<96> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<97> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<98> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<99> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::Int<100>,                                                                                                                        \
    RCF::Int<99> >::type,                                                                                                                 \
    RCF::Int<98> >::type,                                                                                                                 \
    RCF::Int<97> >::type,                                                                                                                 \
    RCF::Int<96> >::type,                                                                                                                 \
    RCF::Int<95> >::type,                                                                                                                 \
    RCF::Int<94> >::type,                                                                                                                 \
    RCF::Int<93> >::type,                                                                                                                 \
    RCF::Int<92> >::type,                                                                                                                 \
    RCF::Int<91> >::type,                                                                                                                 \
    RCF::Int<90> >::type,                                                                                                                 \
    RCF::Int<89> >::type,                                                                                                                 \
    RCF::Int<88> >::type,                                                                                                                 \
    RCF::Int<87> >::type,                                                                                                                 \
    RCF::Int<86> >::type,                                                                                                                 \
    RCF::Int<85> >::type,                                                                                                                 \
    RCF::Int<84> >::type,                                                                                                                 \
    RCF::Int<83> >::type,                                                                                                                 \
    RCF::Int<82> >::type,                                                                                                                 \
    RCF::Int<81> >::type,                                                                                                                 \
    RCF::Int<80> >::type,                                                                                                                 \
    RCF::Int<79> >::type,                                                                                                                 \
    RCF::Int<78> >::type,                                                                                                                 \
    RCF::Int<77> >::type,                                                                                                                 \
    RCF::Int<76> >::type,                                                                                                                 \
    RCF::Int<75> >::type,                                                                                                                 \
    RCF::Int<74> >::type,                                                                                                                 \
    RCF::Int<73> >::type,                                                                                                                 \
    RCF::Int<72> >::type,                                                                                                                 \
    RCF::Int<71> >::type,                                                                                                                 \
    RCF::Int<70> >::type,                                                                                                                 \
    RCF::Int<69> >::type,                                                                                                                 \
    RCF::Int<68> >::type,                                                                                                                 \
    RCF::Int<67> >::type,                                                                                                                 \
    RCF::Int<66> >::type,                                                                                                                 \
    RCF::Int<65> >::type,                                                                                                                 \
    RCF::Int<64> >::type,                                                                                                                 \
    RCF::Int<63> >::type,                                                                                                                 \
    RCF::Int<62> >::type,                                                                                                                 \
    RCF::Int<61> >::type,                                                                                                                 \
    RCF::Int<60> >::type,                                                                                                                 \
    RCF::Int<59> >::type,                                                                                                                 \
    RCF::Int<58> >::type,                                                                                                                 \
    RCF::Int<57> >::type,                                                                                                                 \
    RCF::Int<56> >::type,                                                                                                                 \
    RCF::Int<55> >::type,                                                                                                                 \
    RCF::Int<54> >::type,                                                                                                                 \
    RCF::Int<53> >::type,                                                                                                                 \
    RCF::Int<52> >::type,                                                                                                                 \
    RCF::Int<51> >::type,                                                                                                                 \
    RCF::Int<50> >::type,                                                                                                                 \
    RCF::Int<49> >::type,                                                                                                                 \
    RCF::Int<48> >::type,                                                                                                                 \
    RCF::Int<47> >::type,                                                                                                                 \
    RCF::Int<46> >::type,                                                                                                                 \
    RCF::Int<45> >::type,                                                                                                                 \
    RCF::Int<44> >::type,                                                                                                                 \
    RCF::Int<43> >::type,                                                                                                                 \
    RCF::Int<42> >::type,                                                                                                                 \
    RCF::Int<41> >::type,                                                                                                                 \
    RCF::Int<40> >::type,                                                                                                                 \
    RCF::Int<39> >::type,                                                                                                                 \
    RCF::Int<38> >::type,                                                                                                                 \
    RCF::Int<37> >::type,                                                                                                                 \
    RCF::Int<36> >::type,                                                                                                                 \
    RCF::Int<35> >::type,                                                                                                                 \
    RCF::Int<34> >::type,                                                                                                                 \
    RCF::Int<33> >::type,                                                                                                                 \
    RCF::Int<32> >::type,                                                                                                                 \
    RCF::Int<31> >::type,                                                                                                                 \
    RCF::Int<30> >::type,                                                                                                                 \
    RCF::Int<29> >::type,                                                                                                                 \
    RCF::Int<28> >::type,                                                                                                                 \
    RCF::Int<27> >::type,                                                                                                                 \
    RCF::Int<26> >::type,                                                                                                                 \
    RCF::Int<25> >::type,                                                                                                                 \
    RCF::Int<24> >::type,                                                                                                                 \
    RCF::Int<23> >::type,                                                                                                                 \
    RCF::Int<22> >::type,                                                                                                                 \
    RCF::Int<21> >::type,                                                                                                                 \
    RCF::Int<20> >::type,                                                                                                                 \
    RCF::Int<19> >::type,                                                                                                                 \
    RCF::Int<18> >::type,                                                                                                                 \
    RCF::Int<17> >::type,                                                                                                                 \
    RCF::Int<16> >::type,                                                                                                                 \
    RCF::Int<15> >::type,                                                                                                                 \
    RCF::Int<14> >::type,                                                                                                                 \
    RCF::Int<13> >::type,                                                                                                                 \
    RCF::Int<12> >::type,                                                                                                                 \
    RCF::Int<11> >::type,                                                                                                                 \
    RCF::Int<10> >::type,                                                                                                                 \
    RCF::Int< 9> >::type,                                                                                                                 \
    RCF::Int< 8> >::type,                                                                                                                 \
    RCF::Int< 7> >::type,                                                                                                                 \
    RCF::Int< 6> >::type,                                                                                                                 \
    RCF::Int< 5> >::type,                                                                                                                 \
    RCF::Int< 4> >::type,                                                                                                                 \
    RCF::Int< 3> >::type,                                                                                                                 \
    RCF::Int< 2> >::type,                                                                                                                 \
    RCF::Int< 1> >::type,                                                                                                                 \
    RCF::Int< 0> >::type next_static_id;                                                                                                  \
    friend_or_not RCF::defined_ helper_func(T1 *, T2 *, next_static_id *);


#define RCF_CURRENT_STATIC_ID(current_static_id, helper_func, T1, T2)                                                                             \
    typedef                                                                                                                                       \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 0> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 1> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 2> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 3> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 4> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 5> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 6> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 7> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 8> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 9> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<10> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<11> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<12> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<13> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<14> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<15> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<16> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<17> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<18> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<19> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<20> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<21> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<22> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<23> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<24> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<25> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<26> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<27> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<28> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<29> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<30> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<31> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<32> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<33> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<34> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<35> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<36> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<37> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<38> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<39> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<40> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<41> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<42> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<43> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<44> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<45> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<46> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<47> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<48> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<49> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<50> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<51> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<52> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<53> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<54> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<55> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<56> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<57> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<58> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<59> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<60> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<61> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<62> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<63> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<64> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<65> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<66> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<67> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<68> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<69> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<70> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<71> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<72> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<73> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<74> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<75> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<76> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<77> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<78> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<79> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<80> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<81> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<82> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<83> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<84> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<85> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<86> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<87> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<88> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<89> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<90> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<91> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<92> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<93> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<94> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<95> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<96> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<97> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<98> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<99> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::Int<99>,                                                                                                                         \
    RCF::Int<98> >::type,                                                                                                                 \
    RCF::Int<97> >::type,                                                                                                                 \
    RCF::Int<96> >::type,                                                                                                                 \
    RCF::Int<95> >::type,                                                                                                                 \
    RCF::Int<94> >::type,                                                                                                                 \
    RCF::Int<93> >::type,                                                                                                                 \
    RCF::Int<92> >::type,                                                                                                                 \
    RCF::Int<91> >::type,                                                                                                                 \
    RCF::Int<90> >::type,                                                                                                                 \
    RCF::Int<89> >::type,                                                                                                                 \
    RCF::Int<88> >::type,                                                                                                                 \
    RCF::Int<87> >::type,                                                                                                                 \
    RCF::Int<86> >::type,                                                                                                                 \
    RCF::Int<85> >::type,                                                                                                                 \
    RCF::Int<84> >::type,                                                                                                                 \
    RCF::Int<83> >::type,                                                                                                                 \
    RCF::Int<82> >::type,                                                                                                                 \
    RCF::Int<81> >::type,                                                                                                                 \
    RCF::Int<80> >::type,                                                                                                                 \
    RCF::Int<79> >::type,                                                                                                                 \
    RCF::Int<78> >::type,                                                                                                                 \
    RCF::Int<77> >::type,                                                                                                                 \
    RCF::Int<76> >::type,                                                                                                                 \
    RCF::Int<75> >::type,                                                                                                                 \
    RCF::Int<74> >::type,                                                                                                                 \
    RCF::Int<73> >::type,                                                                                                                 \
    RCF::Int<72> >::type,                                                                                                                 \
    RCF::Int<71> >::type,                                                                                                                 \
    RCF::Int<70> >::type,                                                                                                                 \
    RCF::Int<69> >::type,                                                                                                                 \
    RCF::Int<68> >::type,                                                                                                                 \
    RCF::Int<67> >::type,                                                                                                                 \
    RCF::Int<66> >::type,                                                                                                                 \
    RCF::Int<65> >::type,                                                                                                                 \
    RCF::Int<64> >::type,                                                                                                                 \
    RCF::Int<63> >::type,                                                                                                                 \
    RCF::Int<62> >::type,                                                                                                                 \
    RCF::Int<61> >::type,                                                                                                                 \
    RCF::Int<60> >::type,                                                                                                                 \
    RCF::Int<59> >::type,                                                                                                                 \
    RCF::Int<58> >::type,                                                                                                                 \
    RCF::Int<57> >::type,                                                                                                                 \
    RCF::Int<56> >::type,                                                                                                                 \
    RCF::Int<55> >::type,                                                                                                                 \
    RCF::Int<54> >::type,                                                                                                                 \
    RCF::Int<53> >::type,                                                                                                                 \
    RCF::Int<52> >::type,                                                                                                                 \
    RCF::Int<51> >::type,                                                                                                                 \
    RCF::Int<50> >::type,                                                                                                                 \
    RCF::Int<49> >::type,                                                                                                                 \
    RCF::Int<48> >::type,                                                                                                                 \
    RCF::Int<47> >::type,                                                                                                                 \
    RCF::Int<46> >::type,                                                                                                                 \
    RCF::Int<45> >::type,                                                                                                                 \
    RCF::Int<44> >::type,                                                                                                                 \
    RCF::Int<43> >::type,                                                                                                                 \
    RCF::Int<42> >::type,                                                                                                                 \
    RCF::Int<41> >::type,                                                                                                                 \
    RCF::Int<40> >::type,                                                                                                                 \
    RCF::Int<39> >::type,                                                                                                                 \
    RCF::Int<38> >::type,                                                                                                                 \
    RCF::Int<37> >::type,                                                                                                                 \
    RCF::Int<36> >::type,                                                                                                                 \
    RCF::Int<35> >::type,                                                                                                                 \
    RCF::Int<34> >::type,                                                                                                                 \
    RCF::Int<33> >::type,                                                                                                                 \
    RCF::Int<32> >::type,                                                                                                                 \
    RCF::Int<31> >::type,                                                                                                                 \
    RCF::Int<30> >::type,                                                                                                                 \
    RCF::Int<29> >::type,                                                                                                                 \
    RCF::Int<28> >::type,                                                                                                                 \
    RCF::Int<27> >::type,                                                                                                                 \
    RCF::Int<26> >::type,                                                                                                                 \
    RCF::Int<25> >::type,                                                                                                                 \
    RCF::Int<24> >::type,                                                                                                                 \
    RCF::Int<23> >::type,                                                                                                                 \
    RCF::Int<22> >::type,                                                                                                                 \
    RCF::Int<21> >::type,                                                                                                                 \
    RCF::Int<20> >::type,                                                                                                                 \
    RCF::Int<19> >::type,                                                                                                                 \
    RCF::Int<18> >::type,                                                                                                                 \
    RCF::Int<17> >::type,                                                                                                                 \
    RCF::Int<16> >::type,                                                                                                                 \
    RCF::Int<15> >::type,                                                                                                                 \
    RCF::Int<14> >::type,                                                                                                                 \
    RCF::Int<13> >::type,                                                                                                                 \
    RCF::Int<12> >::type,                                                                                                                 \
    RCF::Int<11> >::type,                                                                                                                 \
    RCF::Int<10> >::type,                                                                                                                 \
    RCF::Int< 9> >::type,                                                                                                                 \
    RCF::Int< 8> >::type,                                                                                                                 \
    RCF::Int< 7> >::type,                                                                                                                 \
    RCF::Int< 6> >::type,                                                                                                                 \
    RCF::Int< 5> >::type,                                                                                                                 \
    RCF::Int< 4> >::type,                                                                                                                 \
    RCF::Int< 3> >::type,                                                                                                                 \
    RCF::Int< 2> >::type,                                                                                                                 \
    RCF::Int< 1> >::type,                                                                                                                 \
    RCF::Int< 0> >::type,                                                                                                                 \
    RCF::Int<-1> >::type current_static_id;

#elif RCF_MAX_METHOD_COUNT <= 200

#define RCF_ADVANCE_STATIC_ID(next_static_id, helper_func, T1, T2, friend_or_not)                                          \
    typedef                                                                                                                \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  0> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  1> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  2> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  3> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  4> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  5> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  6> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  7> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  8> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  9> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 10> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 11> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 12> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 13> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 14> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 15> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 16> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 17> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 18> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 19> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 20> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 21> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 22> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 23> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 24> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 25> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 26> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 27> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 28> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 29> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 30> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 31> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 32> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 33> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 34> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 35> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 36> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 37> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 38> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 39> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 40> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 41> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 42> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 43> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 44> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 45> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 46> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 47> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 48> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 49> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 50> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 51> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 52> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 53> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 54> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 55> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 56> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 57> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 58> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 59> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 60> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 61> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 62> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 63> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 64> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 65> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 66> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 67> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 68> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 69> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 70> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 71> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 72> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 73> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 74> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 75> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 76> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 77> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 78> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 79> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 80> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 81> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 82> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 83> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 84> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 85> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 86> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 87> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 88> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 89> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 90> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 91> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 92> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 93> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 94> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 95> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 96> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 97> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 98> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 99> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<100>*) 0)) == sizeof(RCF::defined_)) >,          \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<101> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<102> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<103> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<104> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<105> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<106> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<107> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<108> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<109> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<110> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<111> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<112> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<113> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<114> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<115> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<116> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<117> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<118> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<119> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<120> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<121> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<122> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<123> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<124> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<125> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<126> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<127> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<128> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<129> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<130> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<131> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<132> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<133> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<134> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<135> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<136> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<137> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<138> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<139> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<140> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<141> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<142> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<143> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<144> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<145> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<146> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<147> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<148> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<149> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<150> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<151> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<152> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<153> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<154> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<155> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<156> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<157> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<158> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<159> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<160> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<161> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<162> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<163> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<164> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<165> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<166> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<167> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<168> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<169> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<170> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<171> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<172> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<173> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<174> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<175> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<176> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<177> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<178> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<179> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<180> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<181> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<182> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<183> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<184> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<185> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<186> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<187> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<188> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<189> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<190> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<191> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<192> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<193> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<194> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<195> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<196> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<197> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<198> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<199> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::Int<200>,                                                                                                                         \
    RCF::Int<199> >::type,                                                                                                                 \
    RCF::Int<198> >::type,                                                                                                                 \
    RCF::Int<197> >::type,                                                                                                                 \
    RCF::Int<196> >::type,                                                                                                                 \
    RCF::Int<195> >::type,                                                                                                                 \
    RCF::Int<194> >::type,                                                                                                                 \
    RCF::Int<193> >::type,                                                                                                                 \
    RCF::Int<192> >::type,                                                                                                                 \
    RCF::Int<191> >::type,                                                                                                                 \
    RCF::Int<190> >::type,                                                                                                                 \
    RCF::Int<189> >::type,                                                                                                                 \
    RCF::Int<188> >::type,                                                                                                                 \
    RCF::Int<187> >::type,                                                                                                                 \
    RCF::Int<186> >::type,                                                                                                                 \
    RCF::Int<185> >::type,                                                                                                                 \
    RCF::Int<184> >::type,                                                                                                                 \
    RCF::Int<183> >::type,                                                                                                                 \
    RCF::Int<182> >::type,                                                                                                                 \
    RCF::Int<181> >::type,                                                                                                                 \
    RCF::Int<180> >::type,                                                                                                                 \
    RCF::Int<179> >::type,                                                                                                                 \
    RCF::Int<178> >::type,                                                                                                                 \
    RCF::Int<177> >::type,                                                                                                                 \
    RCF::Int<176> >::type,                                                                                                                 \
    RCF::Int<175> >::type,                                                                                                                 \
    RCF::Int<174> >::type,                                                                                                                 \
    RCF::Int<173> >::type,                                                                                                                 \
    RCF::Int<172> >::type,                                                                                                                 \
    RCF::Int<171> >::type,                                                                                                                 \
    RCF::Int<170> >::type,                                                                                                                 \
    RCF::Int<169> >::type,                                                                                                                 \
    RCF::Int<168> >::type,                                                                                                                 \
    RCF::Int<167> >::type,                                                                                                                 \
    RCF::Int<166> >::type,                                                                                                                 \
    RCF::Int<165> >::type,                                                                                                                 \
    RCF::Int<164> >::type,                                                                                                                 \
    RCF::Int<163> >::type,                                                                                                                 \
    RCF::Int<162> >::type,                                                                                                                 \
    RCF::Int<161> >::type,                                                                                                                 \
    RCF::Int<160> >::type,                                                                                                                 \
    RCF::Int<159> >::type,                                                                                                                 \
    RCF::Int<158> >::type,                                                                                                                 \
    RCF::Int<157> >::type,                                                                                                                 \
    RCF::Int<156> >::type,                                                                                                                 \
    RCF::Int<155> >::type,                                                                                                                 \
    RCF::Int<154> >::type,                                                                                                                 \
    RCF::Int<153> >::type,                                                                                                                 \
    RCF::Int<152> >::type,                                                                                                                 \
    RCF::Int<151> >::type,                                                                                                                 \
    RCF::Int<150> >::type,                                                                                                                 \
    RCF::Int<149> >::type,                                                                                                                 \
    RCF::Int<148> >::type,                                                                                                                 \
    RCF::Int<147> >::type,                                                                                                                 \
    RCF::Int<146> >::type,                                                                                                                 \
    RCF::Int<145> >::type,                                                                                                                 \
    RCF::Int<144> >::type,                                                                                                                 \
    RCF::Int<143> >::type,                                                                                                                 \
    RCF::Int<142> >::type,                                                                                                                 \
    RCF::Int<141> >::type,                                                                                                                 \
    RCF::Int<140> >::type,                                                                                                                 \
    RCF::Int<139> >::type,                                                                                                                 \
    RCF::Int<138> >::type,                                                                                                                 \
    RCF::Int<137> >::type,                                                                                                                 \
    RCF::Int<136> >::type,                                                                                                                 \
    RCF::Int<135> >::type,                                                                                                                 \
    RCF::Int<134> >::type,                                                                                                                 \
    RCF::Int<133> >::type,                                                                                                                 \
    RCF::Int<132> >::type,                                                                                                                 \
    RCF::Int<131> >::type,                                                                                                                 \
    RCF::Int<130> >::type,                                                                                                                 \
    RCF::Int<129> >::type,                                                                                                                 \
    RCF::Int<128> >::type,                                                                                                                 \
    RCF::Int<127> >::type,                                                                                                                 \
    RCF::Int<126> >::type,                                                                                                                 \
    RCF::Int<125> >::type,                                                                                                                 \
    RCF::Int<124> >::type,                                                                                                                 \
    RCF::Int<123> >::type,                                                                                                                 \
    RCF::Int<122> >::type,                                                                                                                 \
    RCF::Int<121> >::type,                                                                                                                 \
    RCF::Int<120> >::type,                                                                                                                 \
    RCF::Int<119> >::type,                                                                                                                 \
    RCF::Int<118> >::type,                                                                                                                 \
    RCF::Int<117> >::type,                                                                                                                 \
    RCF::Int<116> >::type,                                                                                                                 \
    RCF::Int<115> >::type,                                                                                                                 \
    RCF::Int<114> >::type,                                                                                                                 \
    RCF::Int<113> >::type,                                                                                                                 \
    RCF::Int<112> >::type,                                                                                                                 \
    RCF::Int<111> >::type,                                                                                                                 \
    RCF::Int<110> >::type,                                                                                                                 \
    RCF::Int<109> >::type,                                                                                                                 \
    RCF::Int<108> >::type,                                                                                                                 \
    RCF::Int<107> >::type,                                                                                                                 \
    RCF::Int<106> >::type,                                                                                                                 \
    RCF::Int<105> >::type,                                                                                                                 \
    RCF::Int<104> >::type,                                                                                                                 \
    RCF::Int<103> >::type,                                                                                                                 \
    RCF::Int<102> >::type,                                                                                                                 \
    RCF::Int<101> >::type,                                                                                                                 \
    RCF::Int<100> >::type,                                                                                                                 \
    RCF::Int< 99> >::type,                                                                                                                 \
    RCF::Int< 98> >::type,                                                                                                                 \
    RCF::Int< 97> >::type,                                                                                                                 \
    RCF::Int< 96> >::type,                                                                                                                 \
    RCF::Int< 95> >::type,                                                                                                                 \
    RCF::Int< 94> >::type,                                                                                                                 \
    RCF::Int< 93> >::type,                                                                                                                 \
    RCF::Int< 92> >::type,                                                                                                                 \
    RCF::Int< 91> >::type,                                                                                                                 \
    RCF::Int< 90> >::type,                                                                                                                 \
    RCF::Int< 89> >::type,                                                                                                                 \
    RCF::Int< 88> >::type,                                                                                                                 \
    RCF::Int< 87> >::type,                                                                                                                 \
    RCF::Int< 86> >::type,                                                                                                                 \
    RCF::Int< 85> >::type,                                                                                                                 \
    RCF::Int< 84> >::type,                                                                                                                 \
    RCF::Int< 83> >::type,                                                                                                                 \
    RCF::Int< 82> >::type,                                                                                                                 \
    RCF::Int< 81> >::type,                                                                                                                 \
    RCF::Int< 80> >::type,                                                                                                                 \
    RCF::Int< 79> >::type,                                                                                                                 \
    RCF::Int< 78> >::type,                                                                                                                 \
    RCF::Int< 77> >::type,                                                                                                                 \
    RCF::Int< 76> >::type,                                                                                                                 \
    RCF::Int< 75> >::type,                                                                                                                 \
    RCF::Int< 74> >::type,                                                                                                                 \
    RCF::Int< 73> >::type,                                                                                                                 \
    RCF::Int< 72> >::type,                                                                                                                 \
    RCF::Int< 71> >::type,                                                                                                                 \
    RCF::Int< 70> >::type,                                                                                                                 \
    RCF::Int< 69> >::type,                                                                                                                 \
    RCF::Int< 68> >::type,                                                                                                                 \
    RCF::Int< 67> >::type,                                                                                                                 \
    RCF::Int< 66> >::type,                                                                                                                 \
    RCF::Int< 65> >::type,                                                                                                                 \
    RCF::Int< 64> >::type,                                                                                                                 \
    RCF::Int< 63> >::type,                                                                                                                 \
    RCF::Int< 62> >::type,                                                                                                                 \
    RCF::Int< 61> >::type,                                                                                                                 \
    RCF::Int< 60> >::type,                                                                                                                 \
    RCF::Int< 59> >::type,                                                                                                                 \
    RCF::Int< 58> >::type,                                                                                                                 \
    RCF::Int< 57> >::type,                                                                                                                 \
    RCF::Int< 56> >::type,                                                                                                                 \
    RCF::Int< 55> >::type,                                                                                                                 \
    RCF::Int< 54> >::type,                                                                                                                 \
    RCF::Int< 53> >::type,                                                                                                                 \
    RCF::Int< 52> >::type,                                                                                                                 \
    RCF::Int< 51> >::type,                                                                                                                 \
    RCF::Int< 50> >::type,                                                                                                                 \
    RCF::Int< 49> >::type,                                                                                                                 \
    RCF::Int< 48> >::type,                                                                                                                 \
    RCF::Int< 47> >::type,                                                                                                                 \
    RCF::Int< 46> >::type,                                                                                                                 \
    RCF::Int< 45> >::type,                                                                                                                 \
    RCF::Int< 44> >::type,                                                                                                                 \
    RCF::Int< 43> >::type,                                                                                                                 \
    RCF::Int< 42> >::type,                                                                                                                 \
    RCF::Int< 41> >::type,                                                                                                                 \
    RCF::Int< 40> >::type,                                                                                                                 \
    RCF::Int< 39> >::type,                                                                                                                 \
    RCF::Int< 38> >::type,                                                                                                                 \
    RCF::Int< 37> >::type,                                                                                                                 \
    RCF::Int< 36> >::type,                                                                                                                 \
    RCF::Int< 35> >::type,                                                                                                                 \
    RCF::Int< 34> >::type,                                                                                                                 \
    RCF::Int< 33> >::type,                                                                                                                 \
    RCF::Int< 32> >::type,                                                                                                                 \
    RCF::Int< 31> >::type,                                                                                                                 \
    RCF::Int< 30> >::type,                                                                                                                 \
    RCF::Int< 29> >::type,                                                                                                                 \
    RCF::Int< 28> >::type,                                                                                                                 \
    RCF::Int< 27> >::type,                                                                                                                 \
    RCF::Int< 26> >::type,                                                                                                                 \
    RCF::Int< 25> >::type,                                                                                                                 \
    RCF::Int< 24> >::type,                                                                                                                 \
    RCF::Int< 23> >::type,                                                                                                                 \
    RCF::Int< 22> >::type,                                                                                                                 \
    RCF::Int< 21> >::type,                                                                                                                 \
    RCF::Int< 20> >::type,                                                                                                                 \
    RCF::Int< 19> >::type,                                                                                                                 \
    RCF::Int< 18> >::type,                                                                                                                 \
    RCF::Int< 17> >::type,                                                                                                                 \
    RCF::Int< 16> >::type,                                                                                                                 \
    RCF::Int< 15> >::type,                                                                                                                 \
    RCF::Int< 14> >::type,                                                                                                                 \
    RCF::Int< 13> >::type,                                                                                                                 \
    RCF::Int< 12> >::type,                                                                                                                 \
    RCF::Int< 11> >::type,                                                                                                                 \
    RCF::Int< 10> >::type,                                                                                                                 \
    RCF::Int<  9> >::type,                                                                                                                 \
    RCF::Int<  8> >::type,                                                                                                                 \
    RCF::Int<  7> >::type,                                                                                                                 \
    RCF::Int<  6> >::type,                                                                                                                 \
    RCF::Int<  5> >::type,                                                                                                                 \
    RCF::Int<  4> >::type,                                                                                                                 \
    RCF::Int<  3> >::type,                                                                                                                 \
    RCF::Int<  2> >::type,                                                                                                                 \
    RCF::Int<  1> >::type,                                                                                                                 \
    RCF::Int<  0> >::type next_static_id;                                                                                                  \
    friend_or_not RCF::defined_ helper_func(T1 *, T2 *, next_static_id *);


#define RCF_CURRENT_STATIC_ID(current_static_id, helper_func, T1, T2)                                                      \
    typedef                                                                                                                \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  0> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  1> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  2> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  3> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  4> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  5> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  6> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  7> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  8> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<  9> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 10> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 11> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 12> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 13> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 14> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 15> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 16> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 17> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 18> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 19> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 20> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 21> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 22> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 23> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 24> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 25> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 26> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 27> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 28> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 29> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 30> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 31> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 32> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 33> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 34> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 35> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 36> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 37> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 38> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 39> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 40> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 41> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 42> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 43> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 44> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 45> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 46> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 47> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 48> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 49> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 50> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 51> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 52> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 53> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 54> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 55> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 56> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 57> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 58> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 59> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 60> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 61> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 62> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 63> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 64> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 65> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 66> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 67> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 68> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 69> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 70> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 71> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 72> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 73> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 74> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 75> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 76> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 77> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 78> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 79> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 80> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 81> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 82> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 83> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 84> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 85> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 86> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 87> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 88> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 89> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 90> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 91> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 92> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 93> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 94> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 95> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 96> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 97> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 98> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int< 99> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<100> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<101> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<102> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<103> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<104> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<105> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<106> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<107> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<108> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<109> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<110> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<111> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<112> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<113> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<114> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<115> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<116> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<117> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<118> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<119> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<120> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<121> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<122> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<123> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<124> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<125> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<126> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<127> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<128> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<129> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<130> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<131> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<132> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<133> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<134> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<135> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<136> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<137> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<138> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<139> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<140> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<141> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<142> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<143> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<144> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<145> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<146> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<147> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<148> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<149> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<150> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<151> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<152> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<153> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<154> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<155> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<156> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<157> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<158> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<159> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<160> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<161> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<162> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<163> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<164> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<165> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<166> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<167> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<168> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<169> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<170> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<171> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<172> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<173> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<174> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<175> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<176> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<177> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<178> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<179> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<180> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<181> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<182> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<183> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<184> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<185> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<186> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<187> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<188> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<189> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<190> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<191> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<192> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<193> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<194> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<195> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<196> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<197> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<198> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::If< RCF::Bool< (sizeof(helper_func((T1 *) 0, (T2 *) 0, (RCF::Int<199> *) 0)) == sizeof(RCF::defined_)) >,         \
    RCF::Int<199>,                                                                                                                         \
    RCF::Int<198> >::type,                                                                                                                 \
    RCF::Int<197> >::type,                                                                                                                 \
    RCF::Int<196> >::type,                                                                                                                 \
    RCF::Int<195> >::type,                                                                                                                 \
    RCF::Int<194> >::type,                                                                                                                 \
    RCF::Int<193> >::type,                                                                                                                 \
    RCF::Int<192> >::type,                                                                                                                 \
    RCF::Int<191> >::type,                                                                                                                 \
    RCF::Int<190> >::type,                                                                                                                 \
    RCF::Int<189> >::type,                                                                                                                 \
    RCF::Int<188> >::type,                                                                                                                 \
    RCF::Int<187> >::type,                                                                                                                 \
    RCF::Int<186> >::type,                                                                                                                 \
    RCF::Int<185> >::type,                                                                                                                 \
    RCF::Int<184> >::type,                                                                                                                 \
    RCF::Int<183> >::type,                                                                                                                 \
    RCF::Int<182> >::type,                                                                                                                 \
    RCF::Int<181> >::type,                                                                                                                 \
    RCF::Int<180> >::type,                                                                                                                 \
    RCF::Int<179> >::type,                                                                                                                 \
    RCF::Int<178> >::type,                                                                                                                 \
    RCF::Int<177> >::type,                                                                                                                 \
    RCF::Int<176> >::type,                                                                                                                 \
    RCF::Int<175> >::type,                                                                                                                 \
    RCF::Int<174> >::type,                                                                                                                 \
    RCF::Int<173> >::type,                                                                                                                 \
    RCF::Int<172> >::type,                                                                                                                 \
    RCF::Int<171> >::type,                                                                                                                 \
    RCF::Int<170> >::type,                                                                                                                 \
    RCF::Int<169> >::type,                                                                                                                 \
    RCF::Int<168> >::type,                                                                                                                 \
    RCF::Int<167> >::type,                                                                                                                 \
    RCF::Int<166> >::type,                                                                                                                 \
    RCF::Int<165> >::type,                                                                                                                 \
    RCF::Int<164> >::type,                                                                                                                 \
    RCF::Int<163> >::type,                                                                                                                 \
    RCF::Int<162> >::type,                                                                                                                 \
    RCF::Int<161> >::type,                                                                                                                 \
    RCF::Int<160> >::type,                                                                                                                 \
    RCF::Int<159> >::type,                                                                                                                 \
    RCF::Int<158> >::type,                                                                                                                 \
    RCF::Int<157> >::type,                                                                                                                 \
    RCF::Int<156> >::type,                                                                                                                 \
    RCF::Int<155> >::type,                                                                                                                 \
    RCF::Int<154> >::type,                                                                                                                 \
    RCF::Int<153> >::type,                                                                                                                 \
    RCF::Int<152> >::type,                                                                                                                 \
    RCF::Int<151> >::type,                                                                                                                 \
    RCF::Int<150> >::type,                                                                                                                 \
    RCF::Int<149> >::type,                                                                                                                 \
    RCF::Int<148> >::type,                                                                                                                 \
    RCF::Int<147> >::type,                                                                                                                 \
    RCF::Int<146> >::type,                                                                                                                 \
    RCF::Int<145> >::type,                                                                                                                 \
    RCF::Int<144> >::type,                                                                                                                 \
    RCF::Int<143> >::type,                                                                                                                 \
    RCF::Int<142> >::type,                                                                                                                 \
    RCF::Int<141> >::type,                                                                                                                 \
    RCF::Int<140> >::type,                                                                                                                 \
    RCF::Int<139> >::type,                                                                                                                 \
    RCF::Int<138> >::type,                                                                                                                 \
    RCF::Int<137> >::type,                                                                                                                 \
    RCF::Int<136> >::type,                                                                                                                 \
    RCF::Int<135> >::type,                                                                                                                 \
    RCF::Int<134> >::type,                                                                                                                 \
    RCF::Int<133> >::type,                                                                                                                 \
    RCF::Int<132> >::type,                                                                                                                 \
    RCF::Int<131> >::type,                                                                                                                 \
    RCF::Int<130> >::type,                                                                                                                 \
    RCF::Int<129> >::type,                                                                                                                 \
    RCF::Int<128> >::type,                                                                                                                 \
    RCF::Int<127> >::type,                                                                                                                 \
    RCF::Int<126> >::type,                                                                                                                 \
    RCF::Int<125> >::type,                                                                                                                 \
    RCF::Int<124> >::type,                                                                                                                 \
    RCF::Int<123> >::type,                                                                                                                 \
    RCF::Int<122> >::type,                                                                                                                 \
    RCF::Int<121> >::type,                                                                                                                 \
    RCF::Int<120> >::type,                                                                                                                 \
    RCF::Int<119> >::type,                                                                                                                 \
    RCF::Int<118> >::type,                                                                                                                 \
    RCF::Int<117> >::type,                                                                                                                 \
    RCF::Int<116> >::type,                                                                                                                 \
    RCF::Int<115> >::type,                                                                                                                 \
    RCF::Int<114> >::type,                                                                                                                 \
    RCF::Int<113> >::type,                                                                                                                 \
    RCF::Int<112> >::type,                                                                                                                 \
    RCF::Int<111> >::type,                                                                                                                 \
    RCF::Int<110> >::type,                                                                                                                 \
    RCF::Int<109> >::type,                                                                                                                 \
    RCF::Int<108> >::type,                                                                                                                 \
    RCF::Int<107> >::type,                                                                                                                 \
    RCF::Int<106> >::type,                                                                                                                 \
    RCF::Int<105> >::type,                                                                                                                 \
    RCF::Int<104> >::type,                                                                                                                 \
    RCF::Int<103> >::type,                                                                                                                 \
    RCF::Int<102> >::type,                                                                                                                 \
    RCF::Int<101> >::type,                                                                                                                 \
    RCF::Int<100> >::type,                                                                                                                 \
    RCF::Int< 99> >::type,                                                                                                                 \
    RCF::Int< 98> >::type,                                                                                                                 \
    RCF::Int< 97> >::type,                                                                                                                 \
    RCF::Int< 96> >::type,                                                                                                                 \
    RCF::Int< 95> >::type,                                                                                                                 \
    RCF::Int< 94> >::type,                                                                                                                 \
    RCF::Int< 93> >::type,                                                                                                                 \
    RCF::Int< 92> >::type,                                                                                                                 \
    RCF::Int< 91> >::type,                                                                                                                 \
    RCF::Int< 90> >::type,                                                                                                                 \
    RCF::Int< 89> >::type,                                                                                                                 \
    RCF::Int< 88> >::type,                                                                                                                 \
    RCF::Int< 87> >::type,                                                                                                                 \
    RCF::Int< 86> >::type,                                                                                                                 \
    RCF::Int< 85> >::type,                                                                                                                 \
    RCF::Int< 84> >::type,                                                                                                                 \
    RCF::Int< 83> >::type,                                                                                                                 \
    RCF::Int< 82> >::type,                                                                                                                 \
    RCF::Int< 81> >::type,                                                                                                                 \
    RCF::Int< 80> >::type,                                                                                                                 \
    RCF::Int< 79> >::type,                                                                                                                 \
    RCF::Int< 78> >::type,                                                                                                                 \
    RCF::Int< 77> >::type,                                                                                                                 \
    RCF::Int< 76> >::type,                                                                                                                 \
    RCF::Int< 75> >::type,                                                                                                                 \
    RCF::Int< 74> >::type,                                                                                                                 \
    RCF::Int< 73> >::type,                                                                                                                 \
    RCF::Int< 72> >::type,                                                                                                                 \
    RCF::Int< 71> >::type,                                                                                                                 \
    RCF::Int< 70> >::type,                                                                                                                 \
    RCF::Int< 69> >::type,                                                                                                                 \
    RCF::Int< 68> >::type,                                                                                                                 \
    RCF::Int< 67> >::type,                                                                                                                 \
    RCF::Int< 66> >::type,                                                                                                                 \
    RCF::Int< 65> >::type,                                                                                                                 \
    RCF::Int< 64> >::type,                                                                                                                 \
    RCF::Int< 63> >::type,                                                                                                                 \
    RCF::Int< 62> >::type,                                                                                                                 \
    RCF::Int< 61> >::type,                                                                                                                 \
    RCF::Int< 60> >::type,                                                                                                                 \
    RCF::Int< 59> >::type,                                                                                                                 \
    RCF::Int< 58> >::type,                                                                                                                 \
    RCF::Int< 57> >::type,                                                                                                                 \
    RCF::Int< 56> >::type,                                                                                                                 \
    RCF::Int< 55> >::type,                                                                                                                 \
    RCF::Int< 54> >::type,                                                                                                                 \
    RCF::Int< 53> >::type,                                                                                                                 \
    RCF::Int< 52> >::type,                                                                                                                 \
    RCF::Int< 51> >::type,                                                                                                                 \
    RCF::Int< 50> >::type,                                                                                                                 \
    RCF::Int< 49> >::type,                                                                                                                 \
    RCF::Int< 48> >::type,                                                                                                                 \
    RCF::Int< 47> >::type,                                                                                                                 \
    RCF::Int< 46> >::type,                                                                                                                 \
    RCF::Int< 45> >::type,                                                                                                                 \
    RCF::Int< 44> >::type,                                                                                                                 \
    RCF::Int< 43> >::type,                                                                                                                 \
    RCF::Int< 42> >::type,                                                                                                                 \
    RCF::Int< 41> >::type,                                                                                                                 \
    RCF::Int< 40> >::type,                                                                                                                 \
    RCF::Int< 39> >::type,                                                                                                                 \
    RCF::Int< 38> >::type,                                                                                                                 \
    RCF::Int< 37> >::type,                                                                                                                 \
    RCF::Int< 36> >::type,                                                                                                                 \
    RCF::Int< 35> >::type,                                                                                                                 \
    RCF::Int< 34> >::type,                                                                                                                 \
    RCF::Int< 33> >::type,                                                                                                                 \
    RCF::Int< 32> >::type,                                                                                                                 \
    RCF::Int< 31> >::type,                                                                                                                 \
    RCF::Int< 30> >::type,                                                                                                                 \
    RCF::Int< 29> >::type,                                                                                                                 \
    RCF::Int< 28> >::type,                                                                                                                 \
    RCF::Int< 27> >::type,                                                                                                                 \
    RCF::Int< 26> >::type,                                                                                                                 \
    RCF::Int< 25> >::type,                                                                                                                 \
    RCF::Int< 24> >::type,                                                                                                                 \
    RCF::Int< 23> >::type,                                                                                                                 \
    RCF::Int< 22> >::type,                                                                                                                 \
    RCF::Int< 21> >::type,                                                                                                                 \
    RCF::Int< 20> >::type,                                                                                                                 \
    RCF::Int< 19> >::type,                                                                                                                 \
    RCF::Int< 18> >::type,                                                                                                                 \
    RCF::Int< 17> >::type,                                                                                                                 \
    RCF::Int< 16> >::type,                                                                                                                 \
    RCF::Int< 15> >::type,                                                                                                                 \
    RCF::Int< 14> >::type,                                                                                                                 \
    RCF::Int< 13> >::type,                                                                                                                 \
    RCF::Int< 12> >::type,                                                                                                                 \
    RCF::Int< 11> >::type,                                                                                                                 \
    RCF::Int< 10> >::type,                                                                                                                 \
    RCF::Int<  9> >::type,                                                                                                                 \
    RCF::Int<  8> >::type,                                                                                                                 \
    RCF::Int<  7> >::type,                                                                                                                 \
    RCF::Int<  6> >::type,                                                                                                                 \
    RCF::Int<  5> >::type,                                                                                                                 \
    RCF::Int<  4> >::type,                                                                                                                 \
    RCF::Int<  3> >::type,                                                                                                                 \
    RCF::Int<  2> >::type,                                                                                                                 \
    RCF::Int<  1> >::type,                                                                                                                 \
    RCF::Int<  0> >::type,                                                                                                                 \
    RCF::Int< -1> >::type current_static_id;


#else

#error RCF_MAX_METHOD_COUNT > 200 is currently not implemented.

#endif // RCF_MAX_METHOD_COUNT

#endif // ! INCLUDE_RCF_IDL_HPP
