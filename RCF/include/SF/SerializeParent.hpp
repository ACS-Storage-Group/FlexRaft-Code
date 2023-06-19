
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

/// @file

#ifndef INCLUDE_SF_SERIALIZEPARENT_HPP
#define INCLUDE_SF_SERIALIZEPARENT_HPP

#include <SF/Archive.hpp>
#include <SF/SerializePolymorphic.hpp>

namespace SF {

    /// Serializes a base class of a derived class. Call this from 
    /// the serialization function of the derived class, to invoke serialization
    /// code for the base class.
    template<typename Base, typename Derived>
    void serializeParent(Archive & ar, Derived & derived)
    {
        ar & SF::Archive::PARENT & static_cast<Base &>(derived);
    }

} // namespace SF

#endif // ! INCLUDE_SF_SERIALIZEPARENT_HPP
