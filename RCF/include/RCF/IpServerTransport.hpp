
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

/// @file

#ifndef INCLUDE_RCF_IPSERVERTRANSPORT_HPP
#define INCLUDE_RCF_IPSERVERTRANSPORT_HPP

#include <vector>

#include <RCF/Export.hpp>
#include <RCF/IpAddress.hpp>
#include <RCF/ThreadLibrary.hpp>

namespace RCF {

    /// Represents an IP rule, in the form of (IP address, number of significant bits).
    /// An IP address matches the rule if the significant bits of the IP address match those of the IP rule.
    typedef std::pair<IpAddress, std::size_t> IpRule;

    /// Base class for IP-based server transports. Provides IP-related functionality.
    class RCF_EXPORT IpServerTransport
    {
    public:
                        IpServerTransport();
        virtual         ~IpServerTransport();

        bool            isIpAllowed(const IpAddress &ip) const;

        /// Sets IP rules to allow connections from certain IP ranges.
        void            setAllowIps(
                            const std::vector<IpRule> &allowIps);

        /// Sets IP rules to deny connections from certain IP ranges.
        void            setDenyIps(
                            const std::vector<IpRule> &denyIps);

        /// Returns the IP rules set for allowing connections.
        std::vector<IpRule> 
                        getAllowIps() const;

        /// Returns the IP rules set for denying connections.
        std::vector<IpRule> 
                        getDenyIps() const;

        /// Returns the port number the IP-based server transport is listening on.
        virtual int     getPort() const = 0;

    private:

        mutable ReadWriteMutex      mReadWriteMutex;
        std::vector<IpRule>         mAllowedIps;
        std::vector<IpRule>         mDisallowedIps;
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_IPSERVERTRANSPORT_HPP
