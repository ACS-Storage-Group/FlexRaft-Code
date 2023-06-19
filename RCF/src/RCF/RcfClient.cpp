
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

#include <RCF/RcfClient.hpp>

#include <RCF/ClientStub.hpp>
#include <RCF/ClientTransport.hpp>

namespace RCF {

    I_RcfClient::~I_RcfClient()
    {}

    I_RcfClient::I_RcfClient(const std::string & interfaceName)
    {
        mInterfaceName = interfaceName;
    }

    I_RcfClient::I_RcfClient(
        const std::string &     interfaceName, 
        ServerBindingPtr        serverStubPtr)
    {
        mInterfaceName = interfaceName;
        mServerStubPtr = serverStubPtr;
    }

    I_RcfClient::I_RcfClient(
        const std::string &     interfaceName, 
        const Endpoint &        endpoint, 
        const std::string &     serverBindingName_)
    {
        mInterfaceName = interfaceName;
        std::string serverBindingName = serverBindingName_;
        if ( serverBindingName.empty())
        {
            serverBindingName = mInterfaceName;
        }
        ClientStubPtr clientStubPtr( new ClientStub(mInterfaceName, serverBindingName) );
        clientStubPtr->setEndpoint(endpoint);
        setClientStubPtr(clientStubPtr);
    }

    I_RcfClient::I_RcfClient(
        const std::string &     interfaceName, 
        ClientTransportUniquePtr  clientTransportUniquePtr, 
        const std::string &     serverBindingName_)
    {
        mInterfaceName = interfaceName;
        std::string serverBindingName = serverBindingName_;
        if ( serverBindingName.empty())
        {
            serverBindingName = mInterfaceName;
        }
        ClientStubPtr clientStubPtr( new ClientStub(mInterfaceName, serverBindingName) );
        clientStubPtr->setTransport(std::move(clientTransportUniquePtr));
        setClientStubPtr(clientStubPtr);
    }

    I_RcfClient::I_RcfClient(
        const std::string &     interfaceName, 
        const ClientStub &      clientStub, 
        const std::string &     serverBindingName_)
    {
        mInterfaceName = interfaceName;
        std::string serverBindingName = serverBindingName_;
        if ( serverBindingName.empty())
        {
            serverBindingName = mInterfaceName;
        }
        ClientStubPtr clientStubPtr( new ClientStub(clientStub) );
        clientStubPtr->setInterfaceName(mInterfaceName);
        clientStubPtr->setServerBindingName(serverBindingName);
        setClientStubPtr(clientStubPtr);
    }

    // Copy construction.
    I_RcfClient::I_RcfClient(const std::string & interfaceName, const I_RcfClient & rhs)
    {
        mInterfaceName = interfaceName;
        if (rhs.getClientStubPtr())
        {
            const std::string & serverBindingName = mInterfaceName;
            ClientStubPtr clientStubPtr( new ClientStub(rhs.getClientStub()));
            clientStubPtr->setInterfaceName(mInterfaceName);
            clientStubPtr->setServerBindingName(serverBindingName);
            setClientStubPtr(clientStubPtr);
        }
    }

    // Copy assignment.
    I_RcfClient & I_RcfClient::operator=(const I_RcfClient & rhs)
    {
        if (&rhs != this)
        {
            if (rhs.mClientStubPtr)
            {
                ClientStubPtr clientStubPtr( new ClientStub(rhs.getClientStub()));
                setClientStubPtr(clientStubPtr);
            }
            else
            {
                RCF_ASSERT(!rhs.mServerStubPtr);
                mClientStubPtr = rhs.mClientStubPtr;
            }
        }
        return *this;
    }


    // Move construction.
    I_RcfClient::I_RcfClient(
        const std::string &     interfaceName,
        I_RcfClient &&     rhs)
    {
        mInterfaceName = interfaceName;
        operator=(std::move(rhs));
    }

    // Move assignment.
    I_RcfClient & I_RcfClient::operator=(I_RcfClient && rhs)
    {
        if ( rhs.mClientStubPtr )
        {
            rhs.mServerBindingName = rhs.mClientStubPtr->mServerBindingName;
        }
        
        mClientStubPtr = rhs.mClientStubPtr;
        rhs.mClientStubPtr.reset();

        if ( mClientStubPtr )
        {
            mClientStubPtr->mInterfaceName = mInterfaceName;
            mClientStubPtr->mServerBindingName = mServerBindingName.size() > 0 ? mServerBindingName : mInterfaceName;
        }
        return *this;
    }

    // Trivial swap function.
    void I_RcfClient::swap(I_RcfClient & rhs)
    {
        ClientStubPtr rhsClientStubPtr          = rhs.mClientStubPtr;
        ServerBindingPtr rhsServerStubPtr       = rhs.mServerStubPtr;
        std::string rhsInterfaceName            = rhs.mInterfaceName;
        std::string rhsServerBindingName        = rhs.mServerBindingName;

        rhs.mClientStubPtr                      = mClientStubPtr;
        rhs.mServerStubPtr                      = mServerStubPtr;
        rhs.mInterfaceName                      = mInterfaceName;
        rhs.mServerBindingName                  = mServerBindingName;

        mClientStubPtr                          = rhsClientStubPtr;
        mServerStubPtr                          = rhsServerStubPtr;
        mInterfaceName                          = rhsInterfaceName;
        mServerBindingName                      = rhsServerBindingName;
    }

    void I_RcfClient::setClientStubPtr(ClientStubPtr clientStubPtr)
    {
        mClientStubPtr = clientStubPtr;
    }

    ClientStub & I_RcfClient::getClientStub()
    {
        return *mClientStubPtr;
    }

    const ClientStub & I_RcfClient::getClientStub() const
    {
        return *mClientStubPtr;
    }

    ClientStubPtr I_RcfClient::getClientStubPtr() const
    {
        return mClientStubPtr;
    }

    ServerBindingPtr I_RcfClient::getServerStubPtr() const
    {
        return mServerStubPtr;
    }

    ServerBinding & I_RcfClient::getServerStub()
    {
        return *mServerStubPtr;
    }

    void I_RcfClient::checkClientInitialized()
    {
        if ( !getClientStubPtr() )
        {
            RCF_ASSERT(0 && "Uninitialized RCF client.");
            RCF_THROW(Exception(RcfError_ClientUninitialized));
        }
    }

} // namespace RCF
