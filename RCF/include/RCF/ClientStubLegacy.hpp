
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

#ifndef INCLUDE_RCF_CLIENTSTUBLEGACY_HPP
#define INCLUDE_RCF_CLIENTSTUBLEGACY_HPP

#include <RCF/Idl.hpp>
#include <SF/vector.hpp>

namespace RCF {

    //--------------------------------------------------------------------------
    // I_RequestTransportFilters

    RCF_BEGIN(I_RequestTransportFilters, "")

        RCF_METHOD_R1(
            std::int32_t,
                RequestTransportFilters,
                    const std::vector<std::int32_t> &)

        // No longer supported but still present to improve error message.
        RCF_METHOD_R1(
            std::int32_t,
                QueryForTransportFilters,
                    const std::vector<std::int32_t> &);

    RCF_END(I_RequestTransportFilters)

} // namespace RCF

#endif // ! INCLUDE_RCF_CLIENTSTUBLEGACY_HPP
