
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

#ifndef INCLUDE_RCF_STDFILESYSTEM_HPP
#define INCLUDE_RCF_STDFILESYSTEM_HPP

// This header allows us to pull in either <filesystem> or <experimental/filesystem>
// , depending on what the compiler supports.

#if defined(_MSC_VER) && _MSC_VER < 1920
    // VS 2017 and older - always use std::experimental::filesystem.
#   define  INCLUDE_STD_FILESYSTEM_EXPERIMENTAL 1
#elif defined(__cpp_lib_filesystem)
#   define INCLUDE_STD_FILESYSTEM_EXPERIMENTAL 0
#elif defined(__cpp_lib_experimental_filesystem)
#   define INCLUDE_STD_FILESYSTEM_EXPERIMENTAL 1
#elif !defined(__has_include)
#   define INCLUDE_STD_FILESYSTEM_EXPERIMENTAL 1
#elif __has_include(<filesystem>)
#   ifdef _MSC_VER
        // Visual C++
#       if __has_include(<yvals_core.h>)
#           include <yvals_core.h>
#           if defined(_HAS_CXX17) && _HAS_CXX17
#               define INCLUDE_STD_FILESYSTEM_EXPERIMENTAL 0
#           endif
#       endif
#       ifndef INCLUDE_STD_FILESYSTEM_EXPERIMENTAL
#            define INCLUDE_STD_FILESYSTEM_EXPERIMENTAL 1
#       endif
#   else
        // non-Visual C++
#       define INCLUDE_STD_FILESYSTEM_EXPERIMENTAL 0
#   endif
#elif __has_include(<experimental/filesystem>)
#   define INCLUDE_STD_FILESYSTEM_EXPERIMENTAL 1
#else
#   error Could not find system header "<filesystem>" or "<experimental/filesystem>"
#endif

// Now include the relevant header.

#if INCLUDE_STD_FILESYSTEM_EXPERIMENTAL

#include <experimental/filesystem>
#define RCF_FILESYSTEM_NS std::experimental::filesystem

#else

#include <filesystem>
#define RCF_FILESYSTEM_NS std::filesystem

#endif



#endif // ! INCLUDE_RCF_STDFILESYSTEM_HPP
