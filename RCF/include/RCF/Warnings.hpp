
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

#ifndef INCLUDE_RCF_WARNINGS_HPP
#define INCLUDE_RCF_WARNINGS_HPP

#if defined(_MSC_VER)
#pragma warning(disable: 4251)  // warning C4251: '': class '' needs to have dll-interface to be used by clients of class ''
#pragma warning(disable: 4275)  // warning C4275: non dll-interface class '' used as base for dll-interface class ''
#pragma warning(disable: 4834)  // warning C4834 : discarding return value of function with 'nodiscard' attribute
#pragma warning(disable: 4127)  // warning C4127: warning C4127: conditional expression is constant
#endif

#endif // ! INCLUDE_RCF_WARNINGS_HPP
