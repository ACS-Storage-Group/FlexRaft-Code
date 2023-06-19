
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

#include <RCF/HttpEndpoint.hpp>

#include <RCF/HttpsClientTransport.hpp>
#include <RCF/HttpsServerTransport.hpp>
#include <RCF/HttpsEndpoint.hpp>
#include <RCF/MemStream.hpp>

namespace RCF {

    HttpsEndpoint::HttpsEndpoint(int port) : 
        TcpEndpoint(port)
    {
    }

    HttpsEndpoint::HttpsEndpoint(const std::string & ip, int port) :
        TcpEndpoint(ip, port)
    {
    }

    HttpsEndpoint::HttpsEndpoint(const std::string & ip, int port, const std::string & urlPath) :
        TcpEndpoint(ip, port)
    {
        mUrlPath = urlPath;
    }

    std::string HttpsEndpoint::asString() const
    {
        MemOstream os;
        std::string ip = getIp();
        if (ip.empty())
        {
            ip = "127.0.0.1";
        }
        os << "https://" << ip << ":" << getPort();
        return os.string();
    }

    ServerTransportUniquePtr HttpsEndpoint::createServerTransport() const
    {
        return ServerTransportUniquePtr( new HttpsServerTransport(*this) );
    }

    ClientTransportUniquePtr HttpsEndpoint::createClientTransport() const
    {
        return ClientTransportUniquePtr( new HttpsClientTransport(*this) );
    }

    EndpointPtr HttpsEndpoint::clone() const
    {
        return EndpointPtr( new HttpsEndpoint(*this) );
    }

    std::string HttpsEndpoint::getUrlPath() const 
    {
        return mUrlPath;
    }

} // namespace RCF

