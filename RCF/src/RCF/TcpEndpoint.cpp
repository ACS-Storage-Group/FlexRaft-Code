
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

#include <RCF/TcpEndpoint.hpp>

#include <RCF/InitDeinit.hpp>
#include <RCF/SerializationProtocol.hpp>
#include <RCF/TcpServerTransport.hpp>
#include <RCF/TcpClientTransport.hpp>

namespace RCF {

    TcpEndpoint::TcpEndpoint()
    {}

    TcpEndpoint::TcpEndpoint(int port) :
        mIpAddress("127.0.0.1", port)
    {}

    TcpEndpoint::TcpEndpoint(const std::string &ip, int port) :
        mIpAddress(ip, port)
    {}

    TcpEndpoint::TcpEndpoint(const IpAddress & ipAddress) :
        mIpAddress(ipAddress)
    {}

    TcpEndpoint::TcpEndpoint(const TcpEndpoint &rhs) :
        mIpAddress(rhs.mIpAddress)
    {}

    EndpointPtr TcpEndpoint::clone() const
    {
        return EndpointPtr(new TcpEndpoint(*this));
    }

    std::string TcpEndpoint::getIp() const
    {
        return mIpAddress.getIp();
    }

    int TcpEndpoint::getPort() const
    {
        return mIpAddress.getPort();
    }

    std::string TcpEndpoint::asString() const
    {
        MemOstream os;
        std::string ip = getIp();
        if (ip.empty())
        {
            ip = "127.0.0.1";
        }
        os << "tcp://" << ip << ":" << getPort();
        return os.string();
    }

    IpAddress TcpEndpoint::getIpAddress() const
    {
        return mIpAddress;
    }

    std::unique_ptr<ServerTransport> TcpEndpoint::createServerTransport() const
    {
        return std::unique_ptr<ServerTransport>(
            new RCF::TcpServerTransport(mIpAddress));
    }

    std::unique_ptr<ClientTransport> TcpEndpoint::createClientTransport() const
    {
        return std::unique_ptr<ClientTransport>(
            new RCF::TcpClientTransport(mIpAddress));
    }

    TcpEndpointV4::TcpEndpointV4(const std::string & ip, int port) :
        TcpEndpoint(IpAddressV4(ip, port))
    {
    }

    TcpEndpointV6::TcpEndpointV6(const std::string & ip, int port) :
        TcpEndpoint(IpAddressV6(ip, port))
    {
    }

} // namespace RCF
