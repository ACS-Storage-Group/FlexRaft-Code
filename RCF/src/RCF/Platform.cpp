
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

#include <cstdint>

#include <RCF/Config.hpp>
#include <RCF/InitDeinit.hpp>
#include <RCF/ThreadLibrary.hpp>

#ifdef NDEBUG
// To test timer roll over, we can set the tick counter to start at a negative value.
//static const std::uint32_t OffsetMs = 15*1000;
static const std::uint32_t OffsetMs = 0;
#else
static const std::uint32_t OffsetMs = 0;
#endif

namespace RCF 
{
    ThreadId getCurrentThreadId()
    {
        return std::this_thread::get_id();
    }
}

#ifdef RCF_WINDOWS

// Windows implementation, using GetTickCount().
#include <Windows.h>

namespace RCF {

    std::uint32_t getCurrentTimeMs()
    {
        static std::uint32_t BaseTickCountMs = GetTickCount();
        std::uint32_t timeMs = GetTickCount();
        return  timeMs - BaseTickCountMs - OffsetMs;
    }

} // namespace RCF

#else

// Non-Windows implementation, using gettimeofday().
#include <sys/time.h>

namespace RCF {


#ifdef RCF_USE_CLOCK_MONOTONIC

    std::uint32_t getCurrentTimeMs()
    {
        static struct timespec start = {0};
        static bool init = false;
        if (!init)
        {
            init = true;
            clock_gettime(CLOCK_MONOTONIC, &start);
        }

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);

        long seconds =  now.tv_sec - start.tv_sec;
        long nanoseconds = now.tv_nsec - start.tv_nsec;
        std::uint64_t timeMs = std::uint64_t(seconds)*1000 + nanoseconds/1000000;
        timeMs = timeMs & 0xFFFFFFFF;
        return static_cast<std::uint32_t>(timeMs) - OffsetMs;
    }
    
#else

    std::uint32_t getCurrentTimeMs()
    {
        static struct timeval start = {0};
        static bool init = false;
        if (!init)
        {
            init = true;
            gettimeofday(&start, NULL);
        }

        struct timeval now;
        gettimeofday(&now, NULL);

        long seconds =  now.tv_sec - start.tv_sec;
        long microseconds = now.tv_usec - start.tv_usec;
        std::uint64_t timeMs = std::uint64_t(seconds)*1000 + microseconds/1000;
        timeMs = timeMs & 0xFFFFFFFF;
        return static_cast<std::uint32_t>(timeMs) - OffsetMs;
    }
    
#endif

} // namespace RCF

#endif
