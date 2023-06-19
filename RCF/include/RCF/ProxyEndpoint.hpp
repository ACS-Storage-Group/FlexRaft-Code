
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

#ifndef INCLUDE_RCF_PROXYENDPOINT_HPP
#define INCLUDE_RCF_PROXYENDPOINT_HPP

#include <RCF/Endpoint.hpp>
#include <RCF/Export.hpp>

namespace RCF
{

    /// Represents a proxy endpoint. 

    /// Proxy endpoints are used to facilitate bidirectional server communication. A proxy endpoint allows
    /// one RcfServer (the destination server) to establish TCP connections to another RcfServer (the proxy server), 
    /// so that clients with access to the proxy RcfServer, are then able to make remote calls through to 
    /// the target RcfServer.
    
    /// A RcfServer constructed with a proxy endpoint parameter will serve as the destination server, and will 
    /// maintain a persistent connection to the proxy server. The proxy server is able to request further 
    /// connections to be established, and these connections are then utilized by clients to connect to the destination server.
    ///
    /// A RcfClient constructed with a proxy endpoint parameter, will request the proxy RcfServer
    /// for a connection to the destination server, and then utilizes that connection just as any other RcfClient<> would.
    ///
    /// This allows RCF client connections to be established, in situations where direct TCP connections are not
    /// feasible, e.g. due to firewalls or NAT devices. It also allows a single RcfServer to serve as a gateway
    /// to other RcfServer instances.

    class RCF_EXPORT ProxyEndpoint : public Endpoint
    {
    public:

        /// Constructs a proxy endpoint. proxyServer is the endpoint of the proxy server. proxyEndpointName is the name the proxy endpoint, and must be unique among the proxy endpoints on the proxy server.
        ProxyEndpoint(const Endpoint& proxyServer, const std::string& proxyEndpointName);

        /// Constructs a proxy endpoint. proxyServer is an in-process proxy server. proxyEndpointName is the name of the proxy endpoint, and must be unique among the proxy endpoints on the proxy server.
        ProxyEndpoint(RcfServer& proxyServer, const std::string& proxyEndpointName);

        virtual std::unique_ptr<ServerTransport>    createServerTransport() const;
        virtual std::unique_ptr<ClientTransport>    createClientTransport() const;
        virtual EndpointPtr                         clone() const;
        virtual std::string                         asString() const;

        virtual std::string                         getProxyEndpointName() const;

    private:

        EndpointPtr         mProxyEndpointPtr;
        RcfServer *         mpServer = NULL;
        std::string         mProxyEndpointName;

    };

}

#endif // ! INCLUDE_RCF_PROXYENDPOINT_HPP
