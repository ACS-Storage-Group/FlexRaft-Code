
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

#ifndef INCLUDE_RCF_PROXYENDPOINTSERVICE_HPP
#define INCLUDE_RCF_PROXYENDPOINTSERVICE_HPP

#include <RCF/Export.hpp>
#include <RCF/RemoteCallContext.hpp>
#include <RCF/ServerTransport.hpp>

#include <string.h>
#include <vector>

namespace RCF
{

    class I_ProxyEp;

    typedef RemoteCallContext<void, std::vector<std::string>&>      AmdGetRequests;
    typedef std::shared_ptr<AmdGetRequests>                         AmdGetRequestsPtr;

    class ProxyEndpointEntry
    {
    public:

        ProxyEndpointEntry();
        ProxyEndpointEntry(const std::string& endpointName);

        std::string                                     mName;
        std::vector<std::string>                        mPendingRequests;
        RcfSessionWeakPtr                               mSessionWeakPtr;
        AmdGetRequestsPtr                               mAmdPtr;
    };
    
    class ProxyEndpointService : public I_Service
    {
    public:
        virtual void onServiceAdded(RCF::RcfServer &server);
        virtual void onServiceRemoved(RCF::RcfServer &server);
        virtual void onServerStart(RCF::RcfServer &server);
        virtual void onServerStop(RCF::RcfServer &server);

    private:

        friend class RcfClient<I_ProxyEp>;
        friend class RcfServer;
        friend class RcfSession;
        friend class ProxyEndpointSession;

        void                        enumerateProxyEndpoints(std::vector<std::string>& endpoints);
        ClientTransportUniquePtr    makeProxyEndpointConnection(const std::string& proxyEndpointName);
        void                        setupProxiedConnection(RcfSession& session, ClientTransportPtr proxyTransportPtr);

        void            SetupProxyEndpoint(const std::string& endpointName, const std::string& password);
        //void          CloseProxyEndpoint();
        void            GetConnectionRequests(std::vector<std::string>& requests);
        void            MakeConnectionAvailable(const std::string& endpointName, const std::string& requestId);

        void            onConnectionAvailable(
                            const std::string&          endpointName,
                            const std::string&          requestId,
                            RcfSessionPtr               sessionPtr,
                            ClientTransportUniquePtr    transportPtr);

        void            removeEndpoint(
                            const std::string&          endpointName);        

        RcfServer *                                         mpRcfServer = NULL;

        Mutex                                               mEntriesMutex;
        std::map<std::string, ProxyEndpointEntry>           mEntries;

        Mutex                                               mEndpointConnectionsMutex;
        Condition                                           mEndpointConnectionsCond;
        std::map< 
            std::pair<std::string, std::string>, 
            ClientTransportUniquePtr>                       mEndpointConnections;

        
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_PROXYENDPOINTSERVICE_HPP
