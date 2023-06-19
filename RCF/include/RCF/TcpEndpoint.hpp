
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

#ifndef INCLUDE_RCF_TCPENDPOINT_HPP
#define INCLUDE_RCF_TCPENDPOINT_HPP

#include <string>
#include <memory>

#include <RCF/Endpoint.hpp>
#include <RCF/Export.hpp>
#include <RCF/IpAddress.hpp>

namespace RCF {

    class ServerTransport;
    class ClientTransport;

    /// Represents a TCP endpoint.
    class RCF_EXPORT TcpEndpoint : public Endpoint
    {
    public:

        // *** SWIG BEGIN ***

        /// Constructs a TcpEndpoint from a port number. The IP address defaults to 127.0.0.1 .
        TcpEndpoint(int port);

        /// Constructs a TcpEndpoint from an IP address and port number.
        TcpEndpoint(const std::string &ip, int port);

        /// Gets the IP address of the TcpEndpoint.
        std::string                         getIp() const;

        /// Gets the port number of the TcpEndpoint.
        int                                 getPort() const;

        /// Returns a string representation of the TcpEndpoint.
        std::string                         asString() const;

        // *** SWIG END ***

        TcpEndpoint();
        
        
        TcpEndpoint(const IpAddress & ipAddress);
        TcpEndpoint(const TcpEndpoint &rhs);

        std::unique_ptr<ServerTransport>    createServerTransport() const;
        std::unique_ptr<ClientTransport>    createClientTransport() const;
        EndpointPtr                         clone() const;
        
        IpAddress                           getIpAddress() const;

        bool operator<(const TcpEndpoint &rhs) const
        {
            return mIpAddress < rhs.mIpAddress;
        }

    private:
        IpAddress mIpAddress;
    };

    class RCF_EXPORT TcpEndpointV4 : public TcpEndpoint
    {
    public:
        TcpEndpointV4(const std::string & ip, int port);
    };

    class RCF_EXPORT TcpEndpointV6 : public TcpEndpoint
    {
    public:
        TcpEndpointV6(const std::string & ip, int port);
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_TCPENDPOINT_HPP
