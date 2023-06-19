
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

#ifndef INCLUDE_SF_SFNEW_HPP
#define INCLUDE_SF_SFNEW_HPP

#include <typeinfo>

#include <RCF/Tools.hpp>

#include <RCF/Exception.hpp>
#include <RCF/TypeTraits.hpp>

namespace SF {

    class Archive;

    template<typename T, typename R>
    R sfNewImpl(T*, R*, Archive &, RCF::TrueType *)
    {
        RCF::Exception e(RCF::RcfError_SfNoCtor);
        RCF_THROW(e);
        return NULL;
    }

    template<typename T, typename R>
    R sfNewImpl(T*, R*, Archive &, RCF::FalseType *)
    {
        return new T;
    }

    template<typename T, typename R>
    R sfNew(T*t, R*r, Archive &ar)
    {
        typedef typename std::is_abstract<T>::type type;
        return sfNewImpl(t, r, ar, (type *) NULL);
    }

    template<typename T, unsigned int N, typename R>
    R sfNew(T (*)[N], R*, Archive &)
    {
        RCF::Exception e(RCF::RcfError_SfNoCtor);
        RCF_THROW(e);
        return NULL;
    }


    // SF_CTOR

#define SF_CTOR(type, ctor)                                                 \
    inline type *sfNew(type*, type **, SF::Archive &)                       \
    {                                                                       \
        return new ctor;                                                    \
    }

    // SF_CUSTOM_CTOR

#define SF_CUSTOM_CTOR(type, func)                                          \
    inline type *sfNew(type*, type **, SF::Archive & ar)                    \
    {                                                                       \
        type *pt = NULL;                                                    \
        func(ar, pt);                                                       \
        return pt;                                                          \
    }

    // SF_NO_CTOR

#define SF_NO_CTOR(type)                                                    \
    inline type *sfNew(type*, type **, SF::Archive &)                       \
    {                                                                       \
        RCF::Exception e(RCF::RcfError_SfNoCtor);                           \
        RCF_THROW(e);                                         \
        return NULL;                                                        \
    }

    // SF_NO_CTOR_T1

#define SF_NO_CTOR_T1(type)                                                 \
    template<typename T>                                                    \
    inline type<T> *sfNew(type<T>*, type<T> **, SF::Archive &)              \
    {                                                                       \
        RCF::Exception e(RCF::RcfError_SfNoCtor);                           \
        RCF_THROW(e);                                      \
        return NULL;                                                        \
    }

    // SF_NO_CTOR_T2

#define SF_NO_CTOR_T2(type)                                                 \
    template<typename T, typename U>                                        \
    inline type<T,U> *sfNew(type<T,U>*, type<T,U> **, SF::Archive &)        \
    {                                                                       \
        RCF::Exception e(RCF::RcfError_SfNoCtor);                           \
        RCF_THROW(e);                                    \
        return NULL;                                                        \
    }

} // namespace SF

#endif // ! INCLUDE_SF_SFNEW_HPP
