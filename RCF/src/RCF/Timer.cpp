
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

#include <RCF/Timer.hpp>

#include <RCF/ThreadLibrary.hpp>
#include <RCF/Tools.hpp>

namespace RCF {

    Timer::Timer() 
    { 
        restart(); 
    }

    Timer::Timer(std::uint32_t startTimeMs)
    {
        restart(startTimeMs);
    }

    bool Timer::elapsed(std::uint32_t durationMs) 
    { 
        std::uint32_t nowMs = RCF::getCurrentTimeMs();
        return nowMs - mStartTimeMs >= durationMs;
    }

    void Timer::restart()
    {
        mStartTimeMs = RCF::getCurrentTimeMs();
    }

    void Timer::restart(std::uint32_t startTimeMs)
    {
        mStartTimeMs = startTimeMs;
    }

    std::uint32_t Timer::getStartTimeMs()
    {
        return mStartTimeMs;
    }

    std::uint32_t Timer::getDurationMs()
    {
        std::uint32_t nowMs = RCF::getCurrentTimeMs();
        return nowMs - mStartTimeMs;
    }

} // namespace RCF
