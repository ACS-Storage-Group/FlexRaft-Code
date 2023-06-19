
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

#ifndef INCLUDE_RCF_RECURSIONLIMITER_HPP
#define INCLUDE_RCF_RECURSIONLIMITER_HPP

#include <type_traits>

#include <RCF/TypeTraits.hpp>
#include <RCF/Tools.hpp>

namespace RCF {

    class ByteBuffer;

    template<typename T1,typename T2>
    class RecursionState
    {
    public:
        RecursionState() :
            mRecursing(),
            mBreak(),
            mT1(),
            mT2()
        {}

        void assign(const T1 &t1)
        {
            mT1 = t1;

            clearArg(t1);
        }

        void assign(const T1 &t1, const T2 &t2)
        {
            mT1 = t1;
            mT2 = t2;

            clearArg(t1);
            clearArg(t2);
        }

        void clear()
        {
            mRecursing = false;
            mBreak = true;
            mT1 = T1();
            mT2 = T2();
        }

        bool    mRecursing;
        bool    mBreak;
        T1      mT1;
        T2      mT2;

    private:
        void clearArg_(const ByteBuffer &byteBuffer, std::true_type *)
        {
            const_cast<ByteBuffer &>(byteBuffer).clear();
        }

        template<typename T>
        void clearArg_(const T &, std::false_type *)
        {}

        template<typename T>
        void clearArg(const T &t)
        {
            typedef typename std::is_same<T, ByteBuffer>::type type;
            clearArg_(t, (type*) 0);
        }
    };

    // Utility for avoiding recursive function calls on the stack, by unwinding
    // the relevant part of the stack and then reissuing the requested function
    // call.

    template<typename StateT, typename X, typename Pfn>
    void applyRecursionLimiter(
        StateT &        state, 
        Pfn             pfn,
        X &             x)
    {
        state.mBreak = false;

        if (state.mRecursing)
        {
            state.mRecursing = false;
        }
        else
        {
            ScopeGuard guard([&]() { state.clear(); });

            while (!state.mRecursing && !state.mBreak)
            {
                state.mRecursing = true;
                ((&x)->*pfn)();
            }
        }
    }


    template<typename StateT, typename X, typename Pfn, typename T1>
    void applyRecursionLimiter(
        StateT &        state, 
        Pfn             pfn,
        X &             x, 
        const T1 &      t1)
    {
        state.mBreak = false;

        state.assign(t1);
        if (state.mRecursing)
        {
            state.mRecursing = false;
        }
        else
        {
            ScopeGuard guard([&]() { state.clear(); });

            while (!state.mRecursing && !state.mBreak)
            {
                state.mRecursing = true;
                ((&x)->*pfn)(state.mT1);
            }
        }
    }

    template<typename StateT, typename X, typename Pfn, typename T1, typename T2>
    void applyRecursionLimiter(
        StateT &        state, 
        Pfn             pfn,
        X &             x, 
        const T1 &      t1, 
        const T2 &      t2)
    {
        state.mBreak = false;

        state.assign(t1, t2);
        if (state.mRecursing)
        {
            state.mRecursing = false;
        }
        else
        {
            ScopeGuard guard([&]() { state.clear(); });

            while (!state.mRecursing && !state.mBreak)
            {
                state.mRecursing = true;
                ((&x)->*pfn)(state.mT1, state.mT2);
            }
        }
    }

} // namespace RCF

#endif // ! INCLUDE_RCF_RECURSIONLIMITER_HPP
