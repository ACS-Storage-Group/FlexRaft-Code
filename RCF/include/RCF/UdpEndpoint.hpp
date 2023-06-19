
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

#ifndef INCLUDE_RCF_UDPENDPOINT_HPP
#define INCLUDE_RCF_UDPENDPOINT_HPP

#include <memory>
#include <string>

#include <RCF/Endpoint.hpp>
#include <RCF/Export.hpp>
#include <RCF/IpAddress.hpp>

namespace RCF {

    class ServerTransport;
    class ClientTransport;

    /// Represents a UDP endpoint.
    class RCF_EXPORT UdpEndpoint : public Endpoint
    {
    public:

        // *** SWIG BEGIN ***

        /// Constructs a UdpEndpoint from a port number. The IP address defaults to 127.0.0.1 .
        UdpEndpoint(int port);

        /// Constructs a UdpEndpoint from an IP address and port number.
        UdpEndpoint(const std::string &ip, int port);

        std::string         getIp() const;
        int                 getPort() const;
        std::string         asString() const;

        /// Enables shared address binding. Uses SO_REUSEPORT socket option to allow multiple servers (even in different processes), to listen to broadcast messages on the same port.
        void                enableSharedAddressBinding(bool enable = true);

        /// Listen for multicast messages on the given IP address.
        void                listenOnMulticast(const IpAddress & multicastIp);

        /// Listen for multicast messages on the given IP address.
        void                listenOnMulticast(const std::string & multicastIp);

        // *** SWIG END ***

        UdpEndpoint();
        UdpEndpoint(const IpAddress & ipAddress);
        UdpEndpoint(const UdpEndpoint &rhs);
       
        std::unique_ptr<ServerTransport>    createServerTransport() const;
        std::unique_ptr<ClientTransport>    createClientTransport() const;
        EndpointPtr                         clone() const;
       
    private:
        IpAddress           mIp;
        IpAddress           mMulticastIp;
        bool                mEnableSharedAddressBinding;
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_UDPENDPOINT_HPP
