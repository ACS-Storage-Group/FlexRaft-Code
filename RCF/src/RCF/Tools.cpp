
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

#include <RCF/Tools.hpp>

#include <RCF/Exception.hpp>
#include <RCF/InitDeinit.hpp>
#include <RCF/Log.hpp>
#include <RCF/ThreadLibrary.hpp>
#include <RCF/BsdSockets.hpp>

#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>

#if defined(_MSC_VER)
#include <crtdbg.h>
#endif

namespace RCF {

    void rcfThrow(const char * szFile, int line, const char * szFunc, const Exception & e)
    {
        if (RCF::LogManager::instance().isEnabled(LogNameRcf, LogLevel_1))
        {
            RCF::LogEntry entry(LogNameRcf, LogLevel_1, szFile, line, szFunc);

            entry
                << "RCF exception thrown. Error message: "
                << e.getErrorMessage();
        }

        e.throwSelf();
    }

#ifndef NDEBUG

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable: 4995) // 'sprintf': name was marked as #pragma deprecated
#pragma warning(disable: 4996) // 'sprintf': This function or variable may be unsafe.
#endif

    void doAssert(const char * szFile, int line, const char * szFunc, const char * szAssertion)
    {
        char assertMsgBuffer[512] = { 0 };
        sprintf(assertMsgBuffer, "%s(%d): %s: Assert failed. Expression: %s.\n", szFile, line, szFunc, szAssertion);

#ifdef RCF_WINDOWS
        OutputDebugStringA(assertMsgBuffer);
#endif
        
        fprintf(stdout, "%s", assertMsgBuffer);
        std::string assertMsg(assertMsgBuffer);

        RCF_LOG_1()(assertMsg) << "Failed assertion!";

#ifdef RCF_WINDOWS
        int ret = _CrtDbgReport(_CRT_ASSERT, szFile, line, NULL, NULL, assertMsgBuffer, NULL, NULL);
        if ( ret == 1 )
        {
            // __debugbreak() is more likely to give a proper call stack.
            //DebugBreak();
            __debugbreak();
        }
#else
        assert(0 && "Assert failed.");
#endif

    }

#if defined(_MSC_VER)
#pragma warning(pop)
#endif

#endif

    // Generate a timeout value for the given ending time.
    // Returns zero if endTime <= current time <= endTime+10% of timer resolution,
    // otherwise returns a nonzero duration in ms.
    // Timer resolution as above (49 days).
    std::uint32_t generateTimeoutMs(unsigned int endTimeMs)
    {
        // 90% of the timer interval
        std::uint32_t currentTimeMs = getCurrentTimeMs();
        std::uint32_t timeoutMs = endTimeMs - currentTimeMs;
        return (timeoutMs <= MaxTimeoutMs) ? timeoutMs : 0;
    }

#ifdef RCF_WINDOWS

    std::uint64_t fileSize(const std::string & path)
    {
        struct _stat fileInfo = {0};
        int ret = _stat(path.c_str(), &fileInfo);
        if ( ret == -1 )
        {
            int err = Platform::OS::BsdSockets::GetLastError();
            RCF_THROW(RCF::Exception(RcfError_FileOpen, path, Platform::OS::GetErrorString(err)));
        }        
        return fileInfo.st_size;
    }

#else

    std::uint64_t fileSize(const std::string & path)
    {
        struct stat fileInfo = { 0 };
        int ret = stat(path.c_str(), &fileInfo);
        if ( ret == -1 )
        {
            int err = Platform::OS::BsdSockets::GetLastError();
            RCF_THROW(RCF::Exception(RcfError_FileOpen, path, Platform::OS::GetErrorString(err)));
        }
        return fileInfo.st_size;
    }

#endif

    void rcfDtorCatchHandler(const std::exception & e)
    {
        if (!std::uncaught_exception())
        {
            throw;
        }
        else
        {
            RCF_LOG_1()(e.what());
        }
    }

    ScopeGuard::ScopeGuard(std::function<void()> func) :
        m_func(func),
        m_dismissed(false)
    {
    }

    ScopeGuard::~ScopeGuard()
    {
        RCF_DTOR_BEGIN
            if ( !m_dismissed )
            {
                m_func();
            }
        RCF_DTOR_END
    }

    void ScopeGuard::dismiss()
    {
        m_dismissed = true;
    }

    void trim(std::string& s)
    {
        trimLeft(s);
        trimRight(s);
    }

    void trimLeft(std::string& s)
    {
        std::size_t pos = 0;
        while ( pos < s.size() && isspace(s[pos]) )
        {
            ++pos;
        }
        s.erase(0, pos);
    }

    void trimRight(std::string& s)
    {
        std::size_t pos = s.size() - 1;
        while ( pos < s.size() && isspace(s[pos]) )
        {
            --pos;
        }
        s.erase(pos+1);
    }

    bool iequals(const std::string& lhs, const std::string& rhs)
    {
        if ( lhs.size() != rhs.size() )
        {
            return false;
        }
        for ( std::size_t pos = 0; pos < lhs.size(); ++pos )
        {
            if ( tolower(lhs[pos]) != tolower(rhs[pos]) )
            {
                return false;
            }
        }
        return true;
    }

    bool istartsWith(const std::string& s, const std::string& startsWith)
    {
        if ( s.size() < startsWith.size() )
        {
            return false;
        }
        for ( std::size_t pos = 0; pos < startsWith.size(); ++pos )
        {
            if ( tolower(s[pos]) != tolower(startsWith[pos]) )
            {
                return false;
            }
        }
        return true;
    }

} // namespace RCF
