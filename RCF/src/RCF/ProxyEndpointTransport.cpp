
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

#include <RCF/ProxyEndpointTransport.hpp>

#include <RCF/Endpoint.hpp>
#include <RCF/Log.hpp>
#include <RCF/RcfClient.hpp>
#include <RCF/TcpEndpoint.hpp>
#include <RCF/TcpServerTransport.hpp>
#include <RCF/Tools.hpp>

namespace RCF
{

    ProxyEndpointTransport::ProxyEndpointTransport(
        const Endpoint& ep, 
        const std::string& proxyEndpointName) :
            mEndpointPtr(ep.clone()),
            mEndpointName(proxyEndpointName)
    {

    }

    ProxyEndpointTransport::~ProxyEndpointTransport()
    {
    }

    ServerTransportPtr ProxyEndpointTransport::clone()
    {
        return ServerTransportPtr(
            new ProxyEndpointTransport(*mEndpointPtr, mEndpointName));
    }

    TransportType ProxyEndpointTransport::getTransportType()
    {
        return Tt_Proxy;
    }

    void ProxyEndpointTransport::onServiceAdded(RcfServer &server)
    {
        ServerTransport * pTransport = server.queryForTransport(Tt_Tcp);
        if ( !pTransport )
        {
            pTransport = & server.addEndpoint(TcpEndpoint(-1));
        }
        mpTcpTransport = static_cast<TcpServerTransport *>(pTransport);
        
    }

    void ProxyEndpointTransport::onServiceRemoved(RcfServer &server)
    {
        RCF_UNUSED_VARIABLE(server);
    }

    void ProxyEndpointTransport::onServerStart(RcfServer &server)
    {
        mpServer = &server;
        mStopFlag = false;
        mConn = RcfClient<I_ProxyEp>(*mEndpointPtr);
        mThreadPtr.reset(new Thread([&]() { runTransport(); }));
    }

    void ProxyEndpointTransport::onServerStop(RcfServer &server)
    {
        RCF_UNUSED_VARIABLE(server);
        mStopFlag = true;
        mThreadPtr->join();
        mConn = RcfClient<I_ProxyEp>();
    }

    void ProxyEndpointTransport::runTransport()
    {
        auto progressCallback = [&](const RCF::RemoteCallProgressInfo& info, RemoteCallAction& action)
        {
            RCF_UNUSED_VARIABLE(info);
            if ( mStopFlag )
            {
                action = RCF::Rca_Cancel;
            }
        };

        mConn.getClientStub().setRemoteCallProgressCallback(
            progressCallback,
            500
        );

        // TODO: set the long polling interval.
        mConn.getClientStub().setRemoteCallTimeoutMs(0);

        auto progressCb = [&](const RemoteCallProgressInfo&, RemoteCallAction& rca)
        {
            if ( mStopFlag )
            {
                rca = RCF::Rca_Cancel;
            }
        };
        
        mConn.getClientStub().setRemoteCallProgressCallback(progressCb, 500);

        bool setupEndpoint = true;      
        while ( !mStopFlag )
        {
            try
            {
                if ( setupEndpoint )
                {
                    // TODO: implement password.
                    mConn.SetupProxyEndpoint(mEndpointName, "");
                    setupEndpoint = false;
                }
                std::vector<std::string> requests;
                mConn.GetConnectionRequests(requests);
                for ( const std::string& requestId : requests )
                {
                    RcfClient<I_ProxyEp> client(*mEndpointPtr);
                    client.getClientStub().connect();
                    client.MakeConnectionAvailable(mEndpointName, requestId);
                    convertRcfClientToRcfSession(client.getClientStub(), *mpTcpTransport);
                }
            }
            catch ( const RCF::Exception& e )
            {
                RCF_LOG_2() << "Failed to establish proxy endpoint connection to proxy server at " << mEndpointPtr->asString() << ". " << e.getErrorMessage();
                setupEndpoint = true;
            }
            RCF::sleepMs(1000);
        }
    }

} // namespace RCF
