
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

#include <RCF/ProxyEndpoint.hpp>

#include <RCF/ServerTransport.hpp>
#include <RCF/ProxyEndpointTransport.hpp>

namespace RCF
{
    ProxyEndpoint::ProxyEndpoint(const Endpoint& ep, const std::string& proxyEndpointName) :
        mProxyEndpointPtr(ep.clone()),
        mpServer(),
        mProxyEndpointName(proxyEndpointName)
    {
    }

    ProxyEndpoint::ProxyEndpoint(RcfServer& server, const std::string& proxyEndpointName) :
        mProxyEndpointPtr(),
        mpServer(&server),
        mProxyEndpointName(proxyEndpointName)
    {
    }

    std::unique_ptr<ServerTransport>  ProxyEndpoint::createServerTransport() const
    {
        RCF_ASSERT(mProxyEndpointPtr);
        return std::unique_ptr<ServerTransport>(
            new ProxyEndpointTransport(*mProxyEndpointPtr, mProxyEndpointName));
    }

    std::unique_ptr<ClientTransport>  ProxyEndpoint::createClientTransport() const
    {
        if ( mpServer )
        {
            ClientTransportUniquePtr transportPtr = mpServer->makeProxyEndpointConnection(mProxyEndpointName);
            return transportPtr;
        }
        else if ( mProxyEndpointPtr )
        {
            return mProxyEndpointPtr->createClientTransport();
        }
        else
        {
            RCF_ASSERT(0);
            return std::unique_ptr<ClientTransport>();
        }
    }

    EndpointPtr ProxyEndpoint::clone() const
    {
        if ( mpServer )
        {
            return EndpointPtr( new ProxyEndpoint(*mpServer, mProxyEndpointName));
        }
        else if ( mProxyEndpointPtr )
        {
            return EndpointPtr(new ProxyEndpoint(*mProxyEndpointPtr, mProxyEndpointName));
        }
        else
        {
            return EndpointPtr();
        }
    }

    std::string ProxyEndpoint::asString() const
    {
        // NB: this is used in ClientStub::disconnect() to identify proxy endpoints.
        std::string desc = "ProxyEndpoint '" + mProxyEndpointName + "'";
        if ( mProxyEndpointPtr )
        {
            desc += " (via " + mProxyEndpointPtr->asString() + ")";
        }
        else
        {
            desc += " (via in-process RcfServer)";
        }
        return desc;
    }

    std::string ProxyEndpoint::getProxyEndpointName() const
    {
        return mProxyEndpointName;
    }

}
