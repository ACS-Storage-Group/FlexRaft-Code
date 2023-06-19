
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

#ifndef INCLUDE_SF_QBYTEARRAY_HPP
#define INCLUDE_SF_QBYTEARRAY_HPP

#include <QDataStream>
#include <QByteArray>

#include <boost/config.hpp>

#include <SF/Archive.hpp>
#include <SF/Stream.hpp>

namespace SF {

    // QByteArray    
    inline void serializeQByteArray(SF::Archive & ar, QByteArray & qba)
    {
        if (ar.isRead())
        {
            std::uint32_t count = 0;
            ar & count;
                
            qba.resize(count);
                        
            if (count) 
            {
                SF::IStream &is = *ar.getIstream();            

                // Size field is verified, so read everything in one go. 
                if ( is.read((char *)qba.data(), count) != count )
                {
                    RCF::Exception e(RCF::RcfError_SfReadFailure);
                    RCF_THROW(e);
                }
            }
        }
        else if (ar.isWrite())
        {
            std::uint32_t count = static_cast<std::uint32_t >(qba.size());
            ar & count;
            ar.getOstream()->writeRaw(qba.constData(), count);
        }

    }

    // QByteArray
    inline void serialize_vc6(SF::Archive & ar, QByteArray & qba, const unsigned int)
    {
        serializeQByteArray(ar, qba);
    }
    
    
    #define SERIALIZE_QT_OBJECT                         \
      QByteArray data;                                  \
      if (ar.isRead())                                  \
      {                                                 \
        serializeQByteArray(ar, data);                  \
        QDataStream qdsi(data); /*QIODevice::ReadOnly*/ \
        qdsi >> qobj;                                   \
      }                                                 \
      else if (ar.isWrite())                            \
      {                                                 \
        QDataStream qdso(&data, QIODevice::ReadWrite);  \
        qdso << qobj;                                   \
        serializeQByteArray(ar, data);                  \
      }



} // namespace SF

#endif // ! INCLUDE_SF_QBYTEARRAY_HPP
