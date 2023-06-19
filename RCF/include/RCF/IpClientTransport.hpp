
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

#ifndef INCLUDE_RCF_IPCLIENTTRANSPORT_HPP
#define INCLUDE_RCF_IPCLIENTTRANSPORT_HPP

#include <RCF/Export.hpp>
#include <RCF/IpAddress.hpp>

namespace RCF {

    /// Base class for IP-based client transports. Provides IP-related functionality.
    class RCF_EXPORT IpClientTransport
    {
    public:
        virtual             ~IpClientTransport();
        
        /// Gets the local IP address the client transport should bind to.
        IpAddress           getLocalIp();

        /// Sets the local IP address the client transport should bind to.
        void                setLocalIp(const IpAddress & localIp);

        /// Returns the local IP address the client transport is actually bound to.
        IpAddress           getAssignedLocalIp();

    protected:
        IpAddress           mLocalIp;
        IpAddress           mAssignedLocalIp;
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_IPCLIENTTRANSPORT_HPP
