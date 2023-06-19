
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

#ifndef INCLUDE_RCF_THREADMANAGER_HPP
#define INCLUDE_RCF_THREADMANAGER_HPP

#include <vector>

#include <cstdint>
#include <functional>
#include <map>
#include <memory>

#include <RCF/AsioFwd.hpp>
#include <RCF/Export.hpp>
#include <RCF/ThreadLibrary.hpp>
#include <RCF/Timer.hpp>

namespace RCF {

    class                                               RcfServer;
    typedef std::function<void(int)>                    Task;
    class                                               TaskEntry;
    typedef std::function<void()>                       StopFunctor;

    class                                               ThreadPool;
    typedef std::shared_ptr<ThreadPool>                 ThreadPoolPtr;

    class                                               AsioMuxer;

    typedef std::shared_ptr<ThreadPool>                 ThreadPoolPtr;
    class                                               ShouldStop;

    class RCF_EXPORT ThreadInfo
    {
    public:
        ThreadInfo(ThreadPool & threadPool);
        void touch();
        void notifyBusy();
        ThreadPool & getThreadPool();

    private:
        friend class ThreadPool;
        friend class ShouldStop;

        ThreadPool &    mThreadPool;
        bool            mBusy = false;
        bool            mStopFlag = false;
        bool            mAlreadyRemovedFromThreadPool = false;
        RCF::Timer      mTouchTimer;
    };

    typedef std::shared_ptr<ThreadInfo> ThreadInfoPtr;

    enum MuxerType
    {
        Mt_None,
        Mt_Asio
    };

    static const MuxerType DefaultMuxerType = Mt_Asio;

    /// Represents a server-side thread pool.
    class RCF_EXPORT ThreadPool : 
        public std::enable_shared_from_this<ThreadPool>
    {
    public:

        typedef std::function<void()> ThreadInitFunctor;
        typedef std::function<void()> ThreadDeinitFunctor;

        // *** SWIG BEGIN ***

        ThreadPool(std::size_t fixedThreadCount);
        ThreadPool(std::size_t threadMinCount, std::size_t threadMaxCount);

        /// Sets the minimum number of threads in the thread pool.
        void            setThreadMinCount(std::size_t threadMinCount);

        /// Returns the minimum number of threads in the thread pool.
        std::size_t     getThreadMinCount() const;

        /// Sets the maximum number of threads in the thread pool.
        void            setThreadMaxCount(std::size_t threadMaxCount);

        /// Returns the maximum number of threads in the thread pool.
        std::size_t     getThreadMaxCount() const;

        /// Sets the thread idle timeout value, in milliseconds. After a thread has
        /// been idle for this time, it will be shut down, unless the thread count
        /// is already at the minimum value for the thread pool.
        void            setThreadIdleTimeoutMs(std::uint32_t threadIdleTimeoutMs);

        /// Returns the thread idle timeout value, in milliseconds.
        std::uint32_t   getThreadIdleTimeoutMs() const;

        /// If this setting is true, clients will receive an error message right
        /// away, if all threads in the thread pool are busy. Otherwise, the client
        /// will wait for a thread in the thread pool to become free.
        void            setReserveLastThread(bool reserveLastThread);
        bool            getReserveLastThread() const;

        /// Sets the thread name of the thread pool threads, as visible in a debugger.
        void            setThreadName(const std::string & threadName);

        /// Returns the thread name of the thread pool threads.
        std::string     getThreadName() const;


        // *** SWIG END ***

        ~ThreadPool();
        
        void            start();
        void            stop();
        bool            isStarted();

        /// Sets a function which will be called once by each thread when it starts.
        void            addThreadInitFunctor(
                            ThreadInitFunctor threadInitFunctor);

        /// Sets a function which will be called once by each thread when it is about to stop.
        void            addThreadDeinitFunctor(
                            ThreadDeinitFunctor threadDeinitFunctor);

        AsioIoService * getIoService();

        void            notifyBusy();

        std::size_t     getThreadCount();

        void            setTask(Task task);
        void            setStopFunctor(StopFunctor stopFunctor);

        void            enableMuxerType(MuxerType muxerType);
        void            resetMuxers();


        bool            shouldStop() const;

    private:

        void            onInit();
        void            onDeinit();
        void            setMyThreadName();

        bool            launchThread(std::size_t howManyThreads = 1);

        void            notifyReady();

        void            repeatTask(
                            RCF::ThreadInfoPtr threadInfoPtr,
                            int timeoutMs);

        void            cycle(int timeoutMs, ShouldStop & shouldStop);

        friend class                        TaskEntry;
        friend class                        RcfServer;

        mutable Mutex                       mInitDeinitMutex;
        std::vector<ThreadInitFunctor>      mThreadInitFunctors;
        std::vector<ThreadDeinitFunctor>    mThreadDeinitFunctors;
        std::string                         mThreadName;
        std::shared_ptr<AsioMuxer>          mAsioIoServicePtr;

        bool                                mStarted;
        std::size_t                         mThreadMinCount;
        std::size_t                         mThreadMaxCount;
        bool                                mReserveLastThread;
        std::uint32_t                       mThreadIdleTimeoutMs;

        Task                                mTask;
        StopFunctor                         mStopFunctor;

        bool                                mStopFlag;

        typedef std::map<ThreadInfoPtr, ThreadPtr> ThreadMap;

        Mutex                               mThreadsMutex;
        ThreadMap                           mThreads;
        std::size_t                         mBusyCount;
        Condition                           mAllThreadsStopped;
    };    

    class ThreadTouchGuard
    {
    public:
        ThreadTouchGuard();
        ~ThreadTouchGuard();
    private:
        ThreadInfoPtr mThreadInfoPtr;
    };

    class ShouldStop
    {
    public:

        ShouldStop(
            ThreadInfoPtr threadInfoPtr);

        bool operator()() const;

    private:
        friend class ThreadPool;

        ThreadInfoPtr mThreadInfoPtr;
    };

    RCF_EXPORT void setWin32ThreadName(const std::string & threadName);

} // namespace RCF

#endif // ! INCLUDE_RCF_THREADMANAGER_HPP
