
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

#ifndef INCLUDE_SF_QIMAGE_HPP
#define INCLUDE_SF_QIMAGE_HPP

#include <boost/config.hpp>

#include <SF/Archive.hpp>
#include <SF/Stream.hpp>

#include <QtGui/QImage>
#include <QByteArray>
#include <SF/QByteArray.hpp>


namespace SF {
                
    // QImage
    inline void serialize(SF::Archive & ar, QImage & qobj)
    {
      SERIALIZE_QT_OBJECT
    }    
        
    
} // namespace SF

#endif // ! INCLUDE_SF_QIMAGE_HPP
