
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

#ifndef INCLUDE_RCF_PROXYENDPOINTTRANSPORT_HPP
#define INCLUDE_RCF_PROXYENDPOINTTRANSPORT_HPP

#include <RCF/Export.hpp>
#include <RCF/Idl.hpp>
#include <RCF/ServerTransport.hpp>

#include <string.h>
#include <vector>

namespace RCF
{

    RCF_BEGIN(I_ProxyEp, "I_ProxyEp")
        
        RCF_METHOD_V2(
            void, 
                SetupProxyEndpoint, 
                    const std::string&,             // Proxy endpoint name
                    const std::string&)             // Proxy endpoint password
        
        RCF_METHOD_V1(
            void, 
                GetConnectionRequests, 
                    std::vector<std::string>&)      // Connection requests
        
        RCF_METHOD_V2(
            void, 
                MakeConnectionAvailable, 
                    const std::string&,             // Proxy endpoint name
                    const std::string&)             // Request ID
    
    RCF_END(I_ProxyEp)


    class ProxyEndpointTransport;
    class TcpServerTransport;

    typedef std::shared_ptr<ProxyEndpointTransport> ProxyEndpointTransportPtr;

    class RCF_EXPORT ProxyEndpointTransport : public ServerTransport, public I_Service
    {
    public:

        ProxyEndpointTransport(const Endpoint& ep, const std::string& proxyEndpointName);
        ~ProxyEndpointTransport();
        
        virtual ServerTransportPtr clone();

        virtual TransportType getTransportType();
        virtual void onServiceAdded(RcfServer &server);
        virtual void onServiceRemoved(RcfServer &server);
        virtual void onServerStart(RcfServer &server);
        virtual void onServerStop(RcfServer &server);

        
    private:

        void runTransport();

        EndpointPtr             mEndpointPtr;
        std::string             mEndpointName;

        RcfServer *             mpServer = NULL;
        TcpServerTransport *    mpTcpTransport = NULL;

        bool                    mStopFlag = false;
        ThreadPtr               mThreadPtr;
        RcfClient<I_ProxyEp>    mConn;
    };

}

#endif // ! INCLUDE_RCF_PROXYENDPOINTTRANSPORT_HPP
