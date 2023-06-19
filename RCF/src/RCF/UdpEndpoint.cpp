
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

#include <RCF/UdpEndpoint.hpp>

#include <RCF/InitDeinit.hpp>
#include <RCF/SerializationProtocol.hpp>
#include <RCF/UdpClientTransport.hpp>
#include <RCF/UdpServerTransport.hpp>

namespace RCF {

    UdpEndpoint::UdpEndpoint() :
        mEnableSharedAddressBinding(false)
    {}

    UdpEndpoint::UdpEndpoint(int port) :
        mIp("127.0.0.1", port),
        mEnableSharedAddressBinding(false)
    {}

    UdpEndpoint::UdpEndpoint(const std::string &ip, int port) :
        mIp(ip, port),
        mEnableSharedAddressBinding(false)
    {}

    UdpEndpoint::UdpEndpoint(const IpAddress & ipAddress) :
        mIp(ipAddress),
        mEnableSharedAddressBinding(false)
    {}

    UdpEndpoint::UdpEndpoint(const UdpEndpoint &rhs) :
        mIp(rhs.mIp),
        mMulticastIp(rhs.mMulticastIp),
        mEnableSharedAddressBinding(rhs.mEnableSharedAddressBinding)
    {}

    void UdpEndpoint::enableSharedAddressBinding(bool enable)
    {
        mEnableSharedAddressBinding = enable;
    }

    void UdpEndpoint::listenOnMulticast(const IpAddress & multicastIp)
    {
        mMulticastIp = multicastIp;

        if (!mMulticastIp.empty())
        {
            mEnableSharedAddressBinding = true;
        }
    }

    void UdpEndpoint::listenOnMulticast(const std::string & multicastIp)
    {
        return listenOnMulticast(IpAddress(multicastIp));
    }

    EndpointPtr UdpEndpoint::clone() const
    {
        return EndpointPtr(new UdpEndpoint(*this));
    }

    std::string UdpEndpoint::getIp() const
    {
        return mIp.getIp();
    }

    int UdpEndpoint::getPort() const
    {
        return mIp.getPort();
    }

    ServerTransportUniquePtr UdpEndpoint::createServerTransport() const
    {
        std::unique_ptr<UdpServerTransport> udpServerTransportPtr(
            new UdpServerTransport(mIp, mMulticastIp));

        if (mEnableSharedAddressBinding)
        {
            udpServerTransportPtr->enableSharedAddressBinding();
        }

        return ServerTransportUniquePtr(udpServerTransportPtr.release());
    }

    std::unique_ptr<ClientTransport> UdpEndpoint::createClientTransport() const
    {
        return std::unique_ptr<ClientTransport>(
            new UdpClientTransport(mIp));
    }

    std::string UdpEndpoint::asString() const
    {
        MemOstream os;
        std::string ip = getIp();
        if ( ip.empty() )
        {
            ip = "127.0.0.1";
        }
        os << "udp://" << ip << ":" << getPort();
        return os.string();
    }

} // namespace RCF
