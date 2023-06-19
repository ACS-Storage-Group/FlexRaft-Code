
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

/// \file

#ifndef INCLUDE_RCF_FILESYSTEM_HPP
#define INCLUDE_RCF_FILESYSTEM_HPP

#include <cstdint>

#include <RCF/Export.hpp>
#include <RCF/StdFileSystem.hpp>

namespace RCF
{
    /// Typedef for standard C++ path type.
    typedef RCF_FILESYSTEM_NS::path Path;

    RCF_EXPORT Path makeCanonical(const Path& p);

    RCF_EXPORT void setLastWriteTime(const Path& p, std::uint64_t writeTime);
    RCF_EXPORT std::uint64_t getLastWriteTime(const Path& p);

}

#endif // ! INCLUDE_RCF_FILESYSTEM_HPP

