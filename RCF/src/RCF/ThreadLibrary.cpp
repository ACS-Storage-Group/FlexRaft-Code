
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

#include <RCF/ThreadLibrary.hpp>

#include <RCF/Config.hpp>
#include <RCF/Export.hpp>
#include <RCF/InitDeinit.hpp>
#include <RCF/ThreadLocalData.hpp>

namespace RCF {

    Mutex & getRootMutex()
    {
        static Mutex * gpRootMutex = new Mutex;
        return *gpRootMutex;
    }
#ifdef RCF_WINDOWS

    void sleepMs(std::uint32_t msec)
    {
        ::Sleep(msec);
    }

#else

    void sleepMs(std::uint32_t msec)
    {
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(msec * 1ms);
    }

#endif

}
