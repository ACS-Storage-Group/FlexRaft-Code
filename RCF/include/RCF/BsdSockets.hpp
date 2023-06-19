
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

#ifndef INCLUDE_UTIL_PLATFORM_OS_BSDSOCKETS_HPP
#define INCLUDE_UTIL_PLATFORM_OS_BSDSOCKETS_HPP

#include <RCF/Config.hpp>

#if defined(RCF_WINDOWS)
#include "Windows/BsdSockets.hpp"
#elif defined(__CYGWIN__)
#include "Unix/BsdSockets.hpp"
#elif defined(__unix__)
#include "Unix/BsdSockets.hpp"
#elif defined(__APPLE__)
#include "Unix/BsdSockets.hpp"
#else
#include "UnknownOS/BsdSockets.hpp"
#endif

#endif // ! INCLUDE_UTIL_PLATFORM_OS_BSDSOCKETS_HPP
