
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

#ifndef INCLUDE_RCF_FUTURE_HPP
#define INCLUDE_RCF_FUTURE_HPP

#include <RCF/ClientStub.hpp>
#include <RCF/Marshal.hpp>
#include <iostream>

namespace RCF {

    class I_Future
    {
    public:
        virtual ~I_Future() {}
        virtual void setClientStub(ClientStub *pClientStub) = 0;
    };

    template<typename T>
    class FutureConverter;

    /// Provides the ability for remote calls to be executed asynchronously.

    /// The Future class provides the user with a mechanism to access the return values of an asynchronous 
    /// remote call. Future instances can be used as parameters or return values in a remote call. If any 
    /// Future instances are used in a remote call invocation, the remote call is performed asynchronously.

    /// There are several ways of waiting for an asynchronous remote call to complete. You can poll a Future 
    /// instance using ready(), or wait for a specified time interval using wait(). You can also cancel the call 
    /// at any time using cancel(). Once a remote call completes, the result is accessed by dereferencing
    /// the Future instance using operator*().

    /// The Future class is internally reference counted, and has shallow copy semantics.

    template<typename T>
    class Future
    {
    public:

        /// Constructs a new Future instance.
        Future() : mStatePtr(new State())
        {}

        Future(T *pt) : mStatePtr( new State(pt))
        {}

        Future(T *pt, ClientStub *pClientStub) : mStatePtr( new State(pt))
        {
            pClientStub->enrol(mStatePtr.get());
        }

        /// Constructs a new Future instance, holding a copy of t.
        Future(const T &t) : mStatePtr( new State(t))
        {}

        operator T&() 
        { 
            return mStatePtr->operator T&();
        }

        /// Dereferences this Future instance. If the remote call is still in progress, this function will block until the remote call completes.
        T& operator*()
        {
            return mStatePtr->operator T&();
        }

        Future &operator=(const Future &rhs)
        {
            mStatePtr = rhs.mStatePtr;
            return *this;
        }
    
        Future &operator=(const FutureConverter<T> &rhs)
        {
            rhs.assignTo(*this);
            return *this;
        }

        Future(const FutureConverter<T> &rhs) : mStatePtr( new State())
        {
            rhs.assignTo(*this);
        }

        /// Tests whether the result of an asynchronous call is ready.
        bool ready()
        {
            return mStatePtr->ready();
        }

        /// Waits for up to timeoutMs ms, for the result of an asynchronous call to become ready.
        void wait(std::uint32_t timeoutMs = 0)
        {
            mStatePtr->wait(timeoutMs);
        }

        /// Cancels an asynchronous call.
        void cancel()
        {
            mStatePtr->cancel();
        }

        /// Clears this Future instance.
        void clear()
        {
            mStatePtr->clear();
        }

        ClientStub & getClientStub()
        {
            return mStatePtr->getClientStub();
        }

        // Retrieves the exception, if any, returned by an asynchronous call.
        std::unique_ptr<Exception> getAsyncException()
        {
            // std::cout << mStatePtr << std::endl;
            return mStatePtr->getClientStub().getAsyncException();
        }

    private:

        template<typename U>
        friend class FutureConverter;

        class State : public I_Future, Noncopyable
        {
        public:
            State() : 
                mpt(), 
                mtPtr( new T() ), 
                mpClientStub()
            {}

            State(T *pt) : 
                mpt(pt), 
                mpClientStub()
            {}

            State(const T &t) : 
                mpt(), 
                mtPtr( new T(t) ), 
                mpClientStub()
            {}

            ~State()
            {
                RCF_DTOR_BEGIN
                unregisterFromCandidates();                            
                RCF_DTOR_END
            }

            operator T&()
            {
                // If a call has been made, check that it has completed, and
                // that there was no exception.

                if (mpClientStub)
                {
                    if (!mpClientStub->ready())
                    {
                        mpClientStub->waitForReady();
                    }

                    std::unique_ptr<Exception> ePtr = 
                        mpClientStub->getAsyncException();

                    if (ePtr.get())
                    {
                        ePtr->throwSelf();
                    }
                }

                T *pt = mpt ? mpt : mtPtr.get();
                {
                    Lock lock(gCandidatesMutex());
                    gCandidates().add(pt, this);
                }
                
                return *pt;
            }

            void setClientStub(ClientStub *pClientStub)
            {
                mpClientStub = pClientStub;
            }

            void setClientStub(ClientStub *pClientStub, T * pt)
            {
                unregisterFromCandidates();

                mpClientStub = pClientStub;
                mpt = pt;
                mtPtr.reset();
            }

        private:

            T *                     mpt;
            std::unique_ptr<T>      mtPtr;
            RCF::ClientStub *       mpClientStub;

        public:

            bool ready()
            {
                return mpClientStub->ready();
            }

            void wait(std::uint32_t timeoutMs = 0)
            {
                mpClientStub->waitForReady(timeoutMs);
            }

            void cancel()
            {
                mpClientStub->cancel();                
            }

            ClientStub & getClientStub()
            {
                // std::cout << mpClientStub << std::endl;
                return *mpClientStub;
            }

            void unregisterFromCandidates()
            {
                T *pt = mpt ? mpt : mtPtr.get();
                Lock lock(gCandidatesMutex());
                I_Future * pFuture = gCandidates().find(pt);
                if (pFuture)
                {
                    gCandidates().erase(pt);
                }
            }

        };

        std::shared_ptr<State> mStatePtr;
    };

    class LogEntryExit
    {
    public:
        LogEntryExit(ClientStub & clientStub);
        ~LogEntryExit();

    private:
        ClientStub & mClientStub;
        const std::string & mMsg;
    };

    // Base class for FutureConverter<>.
    class RCF_EXPORT FutureConverterBase
    {
    protected:

        FutureConverterBase(
            ClientStub &clientStub, 
            int fnId,
            RemoteCallMode rcs,
            const char * szFunc,
            const char * szArity);

        FutureConverterBase(const FutureConverterBase& rhs);
        FutureConverterBase &operator=(const FutureConverterBase &rhs);

        void call() const;
        void callSync() const;
        void callAsync() const;

        ClientStub *            mpClientStub;
        int                     mFnId;
        RemoteCallMode     mRcs;
        const char *            mSzFunc;
        const char *            mSzArity;
        mutable bool            mOwn;
    };

    /// Utility class used by RCF to determine whether a remote call should be performed synchronously or asynchronously.

    /// RCF remote methods return FutureConverter<> instances. If the FutureConverter<> instance is converted
    /// to a Future, then the remote call is performed asynchronously. Otherwise it is performed synchronously.
    template<typename T>
    class FutureConverter : public FutureConverterBase
    {
    public:
        FutureConverter(
            T &t, 
            ClientStub &clientStub, 
            int fnId,
            RemoteCallMode rcs,
            const char * szFunc = "",
            const char * szArity = "") :
                FutureConverterBase(clientStub, fnId, rcs, szFunc, szArity),
                mpT(&t)
        {
        }

        FutureConverter(const FutureConverter &rhs) :
            FutureConverterBase(rhs),
            mpT(rhs.mpT)
        {
        }

        FutureConverter &operator=(const FutureConverter &rhs)
        {
            FutureConverterBase::operator=(rhs);
            mpT = rhs.mpT;
            return *this;
        }

        T get()
        {
            return operator T();
        }

        // Conversion to T kicks off a sync call.
        operator T() const
        {
            mOwn = false;
            call();
            T t = *mpT;
            mpClientStub->clearParameters();
            return t;
        }

        // Assignment to Future<> kicks off an async call.
        void assignTo(Future<T> &future) const
        {
            mOwn = false;
            mpClientStub->setAsync(true);
            future.mStatePtr->setClientStub(mpClientStub, mpT);
            call();
        }

        // Void or ignored return value, kicks off a sync call.
        ~FutureConverter() RCF_DTOR_THROWS
        {
            if(mOwn)
            {
                call();

                if (!mpClientStub->getAsync())
                {
                    mpClientStub->clearParameters();
                }
            }
        }

    private:
        T *                     mpT;
    };

    template<typename T, typename U>
    bool operator==(const FutureConverter<T> & fi, const U & u)
    {
        return fi.operator T() == u;
    }

    template<typename T, typename U>
    bool operator==(const U & u, const FutureConverter<T> & fi)
    {
        return u == fi.operator T();
    }


}

#endif // INCLUDE_RCF_FUTURE_HPP
