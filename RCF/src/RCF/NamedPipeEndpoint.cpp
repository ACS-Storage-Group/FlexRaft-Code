
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

#include <RCF/NamedPipeEndpoint.hpp>

#include <RCF/InitDeinit.hpp>
#include <RCF/MemStream.hpp>
#include <RCF/Tchar.hpp>

#if RCF_FEATURE_NAMEDPIPE==1
#include <RCF/Win32NamedPipeClientTransport.hpp>
#include <RCF/Win32NamedPipeServerTransport.hpp>
#elif RCF_FEATURE_LOCALSOCKET==1
#include <RCF/UnixLocalServerTransport.hpp>
#include <RCF/UnixLocalClientTransport.hpp>
#endif

namespace RCF {

    NamedPipeEndpoint::NamedPipeEndpoint()
    {}

    NamedPipeEndpoint::NamedPipeEndpoint(const tstring & pipeName) :
        mPipeName(pipeName)
    {}

#if RCF_FEATURE_NAMEDPIPE==1

    ServerTransportUniquePtr NamedPipeEndpoint::createServerTransport() const
    {
        return ServerTransportUniquePtr(
            new Win32NamedPipeServerTransport(mPipeName));
    }

    ClientTransportUniquePtr NamedPipeEndpoint::createClientTransport() const
    {            
        return ClientTransportUniquePtr(
            new Win32NamedPipeClientTransport(mPipeName));
    }

#else
        
    ServerTransportUniquePtr NamedPipeEndpoint::createServerTransport() const
    {
        return ServerTransportUniquePtr(new UnixLocalServerTransport(toAstring(mPipeName)));
    }

    ClientTransportUniquePtr NamedPipeEndpoint::createClientTransport() const
    {
        return ClientTransportUniquePtr(new UnixLocalClientTransport(toAstring(mPipeName)));
    }

#endif

    EndpointPtr NamedPipeEndpoint::clone() const
    {
        return EndpointPtr( new NamedPipeEndpoint(*this) );
    }

    std::string NamedPipeEndpoint::asString() const
    {
        MemOstream os;
        os << "pipe://" << RCF::toAstring(mPipeName);
        return os.string();
    }

} // namespace RCF
