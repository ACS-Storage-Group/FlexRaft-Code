
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

#include <RCF/Version.hpp>

#include <RCF/ClientStub.hpp>
#include <RCF/Exception.hpp>
#include <RCF/RcfSession.hpp>
#include <RCF/ThreadLocalData.hpp>

namespace RCF {

    // Runtime versioning.

    const std::uint32_t gRuntimeVersionInherent = 13;

    std::uint32_t gRuntimeVersionDefault = gRuntimeVersionInherent;

    std::uint32_t getMaxSupportedRuntimeVersion()
    {
        return gRuntimeVersionInherent;
    }

    std::uint32_t getRuntimeVersion()
    {
        return gRuntimeVersionDefault;
    }

    void setRuntimeVersion(std::uint32_t version)
    {
        RCF_VERIFY(
            1 <= version && version <= gRuntimeVersionInherent,
            Exception(RcfError_UnsupportedRuntimeVersion, version, gRuntimeVersionInherent));

        gRuntimeVersionDefault = version;
    }

    // Archive versioning.

    std::uint32_t gArchiveVersion = 0;

    std::uint32_t getArchiveVersion()
    {
        return gArchiveVersion;
    }

    void setArchiveVersion(std::uint32_t version)
    {
        gArchiveVersion = version;
    }

} // namespace RCF
