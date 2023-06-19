
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

#ifndef INCLUDE_RCF_BASE64_HPP
#define INCLUDE_RCF_BASE64_HPP

#include <string>

std::string base64_encode(unsigned char const*, unsigned int len);
std::string base64_decode(std::string const& s);

#endif // ! INCLUDE_RCF_BASE64_HPP
