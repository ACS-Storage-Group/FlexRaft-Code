
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

#ifndef INCLUDE_SF_QMAP_HPP
#define INCLUDE_SF_QMAP_HPP

#include <QMap>

#include <SF/SerializeStl.hpp>

namespace SF {

    // QMap
    template<typename K, typename T>
    inline void serialize_vc6(Archive &ar, QMap<K,T> &t, const unsigned int)
    {
        typedef typename QMap<K,T>::iterator Iterator;
        typedef typename QMap<K,T>::key_type Key;
        typedef typename QMap<K,T>::mapped_type Value;

        if (ar.isRead())
        {
          t.clear();
          std::uint32_t count = 0;
          ar & count;

          for (std::uint32_t i=0; i<count; i++)
          {
            Key key;
            ar & key;
            Value value;
            ar & value;
            t.insert(key, value);
          }
        }
        else if (ar.isWrite())
        {
          std::uint32_t count = static_cast<std::uint32_t>(t.size());
          ar & count;
          Iterator it = t.begin();
          for (std::uint32_t i=0; i<count; i++)
          {
            ar & it.key();
            ar & it.value();
            it++;
          }
        }
    }

}

#endif // ! INCLUDE_SF_QMAP_HPP
