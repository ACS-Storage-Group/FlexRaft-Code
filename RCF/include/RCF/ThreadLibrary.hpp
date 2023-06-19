
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

#ifndef INCLUDE_RCF_THREADLIBRARY_HPP
#define INCLUDE_RCF_THREADLIBRARY_HPP

#include <mutex>
#include <shared_mutex>
#include <thread>

#include <cstdint>
#include <condition_variable>

#include <RCF/Export.hpp>

namespace RCF
{


    typedef std::thread                 Thread;
    typedef std::mutex                  Mutex;
    typedef std::unique_lock<std::mutex> Lock;
    typedef std::condition_variable     Condition;
    typedef std::recursive_mutex                    RecursiveMutex;
    typedef std::unique_lock<std::recursive_mutex>  RecursiveLock;

    typedef std::thread::id             ThreadId;
    typedef std::shared_timed_mutex ReadWriteMutex;
    typedef std::shared_lock<std::shared_timed_mutex> ReadLock;
    typedef std::unique_lock<std::shared_timed_mutex> WriteLock;

    typedef std::shared_ptr<RecursiveLock>    RecursiveLockPtr;
    typedef std::shared_ptr<RecursiveMutex>   RecursiveMutexPtr;

    typedef std::shared_ptr<Thread>           ThreadPtr;
    typedef std::shared_ptr<ReadWriteMutex>   ReadWriteMutexPtr;
    typedef std::shared_ptr<Mutex>            MutexPtr;
    typedef std::shared_ptr<Lock>             LockPtr;
    typedef std::shared_ptr<Condition>        ConditionPtr;


    RCF_EXPORT ThreadId getCurrentThreadId();

    // Time in ms since ca 1970, modulo 65536 s (turns over every ~18.2 hrs).
    RCF_EXPORT std::uint32_t getCurrentTimeMs();

    // Generate a timeout value for the given ending time.
    // Returns zero if endTime <= current time <= endTime+10%of timer resolution, otherwise returns a nonzero duration in ms.
    // Timer resolution as above (18.2 hrs).
    static const unsigned int MaxTimeoutMs = (((unsigned int)-1)/10)*9;
    RCF_EXPORT std::uint32_t generateTimeoutMs(unsigned int endTimeMs);

    RCF_EXPORT Mutex & getRootMutex();

    RCF_EXPORT void sleepMs(std::uint32_t msec);
}

#endif // ! INCLUDE_RCF_THREADLIBRARY_HPP
