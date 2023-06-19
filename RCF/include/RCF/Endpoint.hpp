
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

/// \file

#ifndef INCLUDE_RCF_ENDPOINT_HPP
#define INCLUDE_RCF_ENDPOINT_HPP

#include <memory>
#include <string>

#include <RCF/Export.hpp>
#include <RCF/RcfFwd.hpp>

namespace SF
{
    class Archive;
}

namespace RCF {

    class ServerTransport;
    class ClientTransport;

    /// Base class for all network endpoint types.
    class RCF_EXPORT Endpoint
    {
    public:

        // *** SWIG BEGIN ***
        
        virtual ~Endpoint() {}
        
        // *** SWIG END ***

        virtual std::unique_ptr<ServerTransport>  createServerTransport() const = 0;
        virtual std::unique_ptr<ClientTransport>  createClientTransport() const = 0;
        virtual EndpointPtr                     clone() const = 0;
        virtual std::string                     asString() const = 0;
        void                                    serialize(SF::Archive &) {}

        // Only implemented by ProxyEndpoint.
        virtual std::string                     getProxyEndpointName() const { return ""; }
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_ENDPOINT_HPP
