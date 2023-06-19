
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

#ifndef INCLUDE_RCF_SCHANNEL_HPP
#define INCLUDE_RCF_SCHANNEL_HPP

#include <RCF/Filter.hpp>
#include <RCF/SspiFilter.hpp>
#include <RCF/Tchar.hpp>

#include <schnlsp.h>

namespace RCF {

    class SchannelServerFilter : public SspiServerFilter
    {
    public:
        SchannelServerFilter(
            SspiCredentialsPtr credentialsPtr,
            RcfServer & server,
            ULONG contextRequirements);

        int getFilterId() const;
    };

    class SchannelFilterFactory : public FilterFactory
    {
    public:
        SchannelFilterFactory();
        FilterPtr                           createFilter(RcfServer & server);
        int                                 getFilterId();

    private:
        Mutex                               mCredentialsMutex;
        SspiCredentialsPtr                  mCredentialsPtr;
    };

    class SchannelClientFilter : public SspiClientFilter
    {
    public:
        SchannelClientFilter(ClientStub * pClientStub);
        int getFilterId() const;
    };

    typedef SchannelClientFilter        SchannelFilter;  

} // namespace RCF

#endif // ! INCLUDE_RCF_SCHANNEL_HPP
