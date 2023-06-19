
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

#ifndef INCLUDE_RCF_BSAUTOPTR_HPP
#define INCLUDE_RCF_BSAUTOPTR_HPP

#include <memory>

#include <boost/serialization/split_free.hpp>
#include <boost/serialization/nvp.hpp>

// Boost.Serialization code for std::unique_ptr.

namespace boost {
    namespace serialization {

        template<typename Archive, typename T>
        void serialize(Archive &ar, std::unique_ptr<T> &apt, const unsigned int version)
        {
            split_free(ar, apt, version);
        }

        template<typename Archive, typename T>
        void save(Archive &ar, const std::unique_ptr<T> &apt, const unsigned int)
        {
            T *pt = apt.get();
            ar & boost::serialization::make_nvp("Dummy", pt);
        }

        template<typename Archive, typename T>
        void load(Archive &ar, std::unique_ptr<T> &apt, const unsigned int)
        {
            T *pt = NULL;
            ar & boost::serialization::make_nvp("Dummy", pt);
            apt.reset(pt);
        }

    }
}

#endif // ! INCLUDE_RCF_BSAUTOPTR_HPP
