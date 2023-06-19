
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

#ifndef INCLUDE_RCF_TOOLS_HPP
#define INCLUDE_RCF_TOOLS_HPP

#include <algorithm>
#include <functional>
#include <memory>
#include <string.h>

#include <RCF/Config.hpp>
#include <RCF/Export.hpp>

// Auto linking on VC++
#ifdef _MSC_VER
#pragma comment(lib, "ws2_32.lib")
#pragma comment(lib, "mswsock.lib")
#pragma comment(lib, "advapi32.lib")
#pragma comment(lib, "user32.lib")
#pragma comment(lib, "crypt32.lib")
#pragma comment(lib, "rpcrt4.lib")
#endif

namespace RCF {

    // Logging
    static const int LogNameRcf = 1;
    static const int LogLevel_1 = 1; // Error and exceptions.
    static const int LogLevel_2 = 2; // Larger scale setup/teardown.
    static const int LogLevel_3 = 3; // Messages sent and received (RCF level), RCF client and session lifetime.
    static const int LogLevel_4 = 4; // Messages sent and received (network level), network client and session lifetime.

#define RCF_LOG_1() UTIL_LOG(RCF::LogNameRcf, RCF::LogLevel_1)
#define RCF_LOG_2() UTIL_LOG(RCF::LogNameRcf, RCF::LogLevel_2)
#define RCF_LOG_3() UTIL_LOG(RCF::LogNameRcf, RCF::LogLevel_3)
#define RCF_LOG_4() UTIL_LOG(RCF::LogNameRcf, RCF::LogLevel_4)

    // Asserts
#ifndef NDEBUG

    RCF_EXPORT void doAssert(const char * szFile, int line, const char * szFunc, const char * szAssertion);

#define RCF_ASSERT(x) if (x) ; else RCF::doAssert(__FILE__, __LINE__, __FUNCTION__, #x);
#define RCF_ASSERT_ALWAYS(x) RCF::doAssert(__FILE__, __LINE__, __FUNCTION__, x);

#else

#define RCF_ASSERT(x)
#define RCF_ASSERT_ALWAYS(x)

#endif

    // Throw mechanism
    class Exception;
    RCF_EXPORT void rcfThrow(const char * szFile, int line, const char * szFunc, const Exception & e);

#define RCF_THROW(e)            RCF::rcfThrow(__FILE__, __LINE__, __FUNCTION__, e)
#define RCF_VERIFY(cond, e)     if (cond); else RCF_THROW(e)

    // Null deleter, for use with for shared_ptr
    class NullDeleter
    {
    public:
        template<typename T>
        void operator()(T)
        {}
    };

    // Catch handler.
    RCF_EXPORT void rcfDtorCatchHandler(const std::exception & e);

// destructor try/catch blocks
#define RCF_DTOR_BEGIN                              \
    try {

#define RCF_DTOR_END                                \
    }                                               \
    catch (const std::exception &e)                 \
    {                                               \
        RCF::rcfDtorCatchHandler(e);                \
    }

    struct Void {};

    template<typename Container, typename Element>
    void eraseRemove(Container & container, const Element & element)
    {
        container.erase(
            std::remove(
                container.begin(),
                container.end(),
                element),
            container.end());
    }

    RCF_EXPORT std::uint64_t fileSize(const std::string & path);

    // For some reason C++11 doesn't have operator< for weak_ptr.
    template<typename T>
    inline bool operator<(
        const std::weak_ptr<T> & lhs,
        const std::weak_ptr<T> & rhs)
    {
        std::owner_less< std::weak_ptr<T> > cmp;
        return cmp(lhs, rhs);
    }
    
    template<typename T>
    inline bool operator==(
        const std::weak_ptr<T> & lhs, 
        const std::weak_ptr<T> & rhs)
    {
        return ! (lhs < rhs) && ! (rhs < lhs);
    }

    template<typename T>
    inline bool operator!=(
        const std::weak_ptr<T> & lhs, 
        const std::weak_ptr<T> & rhs)
    {
        return ! (lhs == rhs);
    }

    class Noncopyable
    {
    protected:
        Noncopyable() {}
        ~Noncopyable() {}
        Noncopyable(const Noncopyable&) = delete;
        Noncopyable& operator=(const Noncopyable&) = delete;
    };

    class ScopeGuard
    {
    public:
        ScopeGuard(std::function<void()> func);
        ~ScopeGuard();

        void dismiss();

    private:
        bool                    m_dismissed;
        std::function<void()>   m_func;
    };

    RCF_EXPORT void trim(std::string& s);
    RCF_EXPORT void trimLeft(std::string& s);
    RCF_EXPORT void trimRight(std::string& s);
    RCF_EXPORT bool iequals(const std::string& lhs, const std::string& rhs);
    RCF_EXPORT bool istartsWith(const std::string& s, const std::string& startsWith);

    template<typename TPtr>
    std::string getTypeName(const TPtr & tPtr)
    {
        if ( tPtr )
        {
            auto& t = *tPtr;
            return typeid(t).name();
        }
        return "";
    }
    
} // namespace RCF

namespace SF
{
    typedef RCF::Noncopyable Noncopyable;
}

// Eliminate unused variable warnings, e.g. for scoped lock objects
#define RCF_UNUSED_VARIABLE(x) ((void) x)

// Macros in Windows platform headers make it awkward to use std::min/std::max.
#define RCF_MIN (std::min)
#define RCF_MAX (std::max)

//****************************************************************************
// Helper macros to generate serialization code for fundamental types

#define SF_FOR_EACH_FUNDAMENTAL_TYPE_(arg)  \
    arg(char)                               \
    arg(int)                                \
    arg(bool)                               \
    arg(float)                              \
    arg(double)                             \
    arg(short)                              \
    arg(long)                               \
    arg(unsigned short)                     \
    arg(signed char)                        \
    arg(unsigned char)                      \
    arg(unsigned int)                       \
    arg(unsigned long)                      \
    arg(long double)                        \
    //arg(wchar_t)

#ifdef _MSC_VER

#define SF_FOR_EACH_FUNDAMENTAL_TYPE(arg)   \
    SF_FOR_EACH_FUNDAMENTAL_TYPE_(arg)      \
    arg(__int64)                            \
    arg(unsigned __int64)

#else

#define SF_FOR_EACH_FUNDAMENTAL_TYPE(arg)   \
    SF_FOR_EACH_FUNDAMENTAL_TYPE_(arg)      \
    arg(long long)                          \
    arg(unsigned long long)

#endif

#endif // ! INCLUDE_RCF_TOOLS_HPP
