
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

#ifndef INCLUDE_SF_QHOSTADDRESS_HPP
#define INCLUDE_SF_QHOSTADDRESS_HPP

#include <QHostAddress>

#include <boost/config.hpp>

#include <SF/Archive.hpp>
#include <SF/QByteArray.hpp> 

namespace SF {

    // QHostAddress
    inline void serialize(SF::Archive & ar, QHostAddress & qha)
    {
      if (ar.isRead())
      {
        QByteArray data;
        serializeQByteArray(ar, data);
        QDataStream qdsi(data);   // QIODevice::ReadOnly
        qdsi >> qha;
      }
      else if (ar.isWrite())
      {
        QByteArray data;
        QDataStream qdso(&data, QIODevice::ReadWrite);
        qdso << qha;
        serializeQByteArray(ar, data);
      }
    }    

    //inline void serialize(SF::Archive & ar, QHostAddress & qha)
    //{
    //  if (ar.isRead())
    //  {
    //    QByteArray data;
    //    QDataStream qdsi(&data, QIODevice::ReadWrite);
    //    serializeQDataStream(ar, qdsi);
    //    qdsi >> qha;
    //  }
    //  else if (ar.isWrite())
    //  {
    //    QByteArray data;
    //    QDataStream qdso(&data, QIODevice::ReadWrite);
    //    qdso << qha;
    //    serializeQDataStream(ar, qdso);
    //  }
    //}

} // namespace SF

#endif // ! INCLUDE_SF_QHOSTADDRESS_HPP
