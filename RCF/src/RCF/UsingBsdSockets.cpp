
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

#include <string>

#include <RCF/Config.hpp>
#include <RCF/Exception.hpp>
#include <RCF/InitDeinit.hpp>
#include <RCF/MemStream.hpp>
#include <RCF/Tools.hpp>
#include <RCF/BsdSockets.hpp>

#ifdef RCF_WINDOWS

namespace RCF {

    void initWinsock()
    {
        WORD wVersion = MAKEWORD( 1, 0 );
        WSADATA wsaData;
        int ret = WSAStartup(wVersion, &wsaData);
        int err = Platform::OS::BsdSockets::GetLastError();
        RCF_VERIFY(ret == 0, Exception(RcfError_Socket, "WSAStartup()", osError(err)));
    }

    void deinitWinsock()
    {
        WSACleanup();
    }

} // namespace RCF

#endif


