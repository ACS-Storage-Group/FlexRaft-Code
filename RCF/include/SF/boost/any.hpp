
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

#ifndef INCLUDE_SF_ANY_HPP
#define INCLUDE_SF_ANY_HPP

#include <string>

#include <boost/any.hpp>

#include <SF/string.hpp>
#include <SF/Registry.hpp>
#include <SF/SerializeAny.hpp>

namespace SF {

    class Archive;

    inline void serialize(SF::Archive &ar, boost::any &a)
    {
        if ( ar.isWrite() )
        {
            std::string which =
                SF::Registry::getSingleton().getTypeName(a.type());

            if ( which.empty() && !a.empty() )
            {
                RCF_THROW(RCF::Exception(RCF::RcfError_AnyTypeNotRegistered, a.type().name()));
            }

            ar & which;

            if ( !a.empty() )
            {
                RCF_ASSERT(which.size() > 0);

                SF::I_SerializerAny * serializerAny = SF::Registry::getSingleton().getAnySerializer(which);
                if ( serializerAny )
                {
                    serializerAny->serialize(ar, a);
                }
            }
        }
        else
        {
            std::string which;
            ar & which;
            if ( which.empty() )
            {
                a = boost::any();
            }
            else
            {
                SF::I_SerializerAny * serializerAny = SF::Registry::getSingleton().getAnySerializer(which);
                if ( serializerAny )
                {
                    serializerAny->serialize(ar, a);
                }
            }
        }
    }


} // namespace SF

#endif // ! INCLUDE_SF_ANY_HPP
