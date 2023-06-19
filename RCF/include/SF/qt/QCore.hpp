
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

#ifndef INCLUDE_SF_QCORE_HPP
#define INCLUDE_SF_QCORE_HPP

#include <QByteArray>
#include <QDataStream>
#include <QDateTime>
#include <QPair>
#include <QUrl>

#include <boost/config.hpp>

#include <SF/Archive.hpp>
#include <SF/Stream.hpp>

#include <SF/QByteArray.hpp>
#include <SF/QString.hpp>
#include <SF/QList.hpp>
#include <SF/QStringList.hpp>
#include <SF/QMap.hpp>


namespace SF {
                
    inline void serializeQDataStream(SF::Archive & ar, QDataStream & qds)
    {
      if (ar.isRead())
      {         
        if (qds.device()->isWritable())    // QIODevice::WriteOnly
        {
          //qds.resetStatus();
          QByteArray qba;
          serializeQByteArray(ar, qba);
          if (qba.size()) { 
            qds.writeRawData(qba.data(), qba.size());
          }
        }
      }
      else if (ar.isWrite())
      {         
        QIODevice * dev = qds.device();
        if (dev->isReadable())             // QIODevice::ReadOnly
        {
          std::uint32_t count = dev->bytesAvailable();
          if (count == 0)
          {
            ar & count;
          }
          else
          {
            serializeQByteArray(ar, dev->readAll());
          }
          //while (1)
          //{
          //  if (qds.atEnd()) break;
          //  std::uint32_t count = 0;
          //  char * buf = 0;
          //  qds.readBytes(buf, count);
          //  ar & count;
          //  if (buf && count) ar.getOstream()->writeRaw(buf, count);
          //  if (buf) delete[] buf;
          //}
        }  
      }
    }

    // QDateTime    
    inline void serialize(SF::Archive & ar, QDateTime & qdt)
    {
      if (ar.isRead())
      {
        std::int64_t utc_time;
        serializeFundamental(ar, utc_time);
        qdt.setMSecsSinceEpoch(utc_time);
      }
      else if (ar.isWrite())
      {
        std::int64_t utc_time = qdt.toMSecsSinceEpoch();
        serializeFundamental(ar, utc_time);
      }
    }
    
    // QPair
    template<typename T, typename U>
    inline void serialize_vc6(Archive &ar, QPair<T,U> &t, const unsigned int)
    {
      ar & t.first & t.second;
    }
   
    // QUrl
    inline void serialize(SF::Archive & ar, QUrl & qobj)
    {
      SERIALIZE_QT_OBJECT
    }    
        
    
} // namespace SF

#endif // ! INCLUDE_SF_QCORE_HPP
