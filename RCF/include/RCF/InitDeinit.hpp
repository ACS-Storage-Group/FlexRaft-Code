
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

#ifndef INCLUDE_RCF_INITDEINIT_HPP
#define INCLUDE_RCF_INITDEINIT_HPP

/// \file

#include <RCF/Export.hpp>
#include <RCF/Config.hpp>

namespace RCF {

    /// Returns the number of outstanding RCF::init() calls.
    RCF_EXPORT std::size_t getInitRefCount();

    /// Reference-counted initialization of RCF library. May be called multiple times (see deinit()).
    RCF_EXPORT bool init(RcfConfigT * = nullptr);

    /// Reference-counted deinitialization of RCF library. For actual deinitialization to take place, deinit() must be called as many times as init() has been called.
    RCF_EXPORT bool deinit();

    /// RCF initialization sentry class. Internally reference counted. Constructor calls RCF::init(). Destructor calls RCF::deinit().
    class RCF_EXPORT RcfInit
    {
    public:

        /// Calls RCF::init(). Uses an anonymous default parameter to detect build configuration mismatches.
        RcfInit(RcfConfigT * = nullptr);

        /// Calls RCF::deinit().
        ~RcfInit();

    private:
        bool mIsRootInstance = false;
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_INITDEINIT_HPP
