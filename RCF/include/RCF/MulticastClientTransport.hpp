
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

#ifndef INCLUDE_RCF_MULTICASTCLIENTTRANSPORT_HPP
#define INCLUDE_RCF_MULTICASTCLIENTTRANSPORT_HPP

#include <memory>
#include <string>
#include <vector>

#include <RCF/ClientTransport.hpp>
#include <RCF/Export.hpp>
#include <RCF/ThreadLibrary.hpp>

namespace RCF {

    typedef std::shared_ptr< ClientTransportUniquePtr >       ClientTransportUniquePtrPtr;
    typedef std::vector< ClientTransportUniquePtrPtr >        ClientTransportList;

    // Special purpose client transport for sending messages in parallel on multiple sub-transports.
    class RCF_EXPORT MulticastClientTransport : public ClientTransport
    {
    public:

        TransportType getTransportType();

        std::unique_ptr<ClientTransport> clone() const;

        EndpointPtr getEndpointPtr() const;

        void        doSendOnTransports(
            Lock&                           lock,
            ClientTransportList&            transportList,
            const std::vector<ByteBuffer> & data,
            unsigned int                    timeoutMs);

        int         send(
                        ClientTransportCallback &     clientStub, 
                        const std::vector<ByteBuffer> & data, 
                        unsigned int                    timeoutMs);

        int         receive(
                        ClientTransportCallback &     clientStub, 
                        ByteBuffer &                    byteBuffer, 
                        unsigned int                    timeoutMs);

        bool        isConnected();

        void        connect(
                        ClientTransportCallback &     clientStub, 
                        unsigned int                    timeoutMs);

        void        disconnect(
                        unsigned int                    timeoutMs);

        void        addTransport(
                        ClientTransportUniquePtr          clientTransportUniquePtr);

        void        setTransportFilters(
                        const std::vector<FilterPtr> &  filters);

        void        getTransportFilters(
                        std::vector<FilterPtr> &        filters);

        void        setTimer(std::uint32_t timeoutMs, ClientTransportCallback *pClientStub);

        void        dropIdleTransports();
        void        pingAllTransports();

        void        close();

        std::size_t getTransportCount();

    private:

        void        bringInNewTransports();

        Mutex                                           mClientTransportsMutex;
        ClientTransportList                             mClientTransports;
        ClientTransportList                             mClientTransportsTemp;
        ClientTransportList                             mClientTransportsSending;

        Mutex                                           mAddedClientTransportsMutex;
        ClientTransportList                             mAddedClientTransports;

        ClientTransportUniquePtr                          mMulticastTemp;
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_MULTICASTCLIENTTRANSPORT_HPP
