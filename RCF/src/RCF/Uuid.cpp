
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

#include <RCF/Uuid.hpp>

#include <RCF/Config.hpp>

#ifdef RCF_WINDOWS

#include <Rpc.h>
namespace RCF
{
    std::string generateUuid()
    {
        UUID uuid = { 0 };
        UuidCreate(&uuid);

        unsigned char * str = NULL;
        UuidToStringA(&uuid, &str);

        std::string s((char*)str);

        RpcStringFreeA(&str);

        return s;
    }
}

#else

#include <uuid/uuid.h>
namespace RCF
{
    std::string generateUuid()
    {
        uuid_t uuid;
        uuid_generate_random(uuid);
        char s[37] = { 0 };
        uuid_unparse(uuid, s);
        return std::string(s);
    }
} // namespace RCF

#endif
