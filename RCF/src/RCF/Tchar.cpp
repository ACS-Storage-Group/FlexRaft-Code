
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

#include <RCF/Tchar.hpp>

#include <RCF/ThreadLocalData.hpp>

#include <locale>

namespace RCF
{

    std::wstring stringToWstring(const std::string &s)
    {
        std::wstring_convert<std::codecvt_utf8<wchar_t> > & utf8Converter 
            = getTlsUtf8Converter();

        return utf8Converter.from_bytes(s);
    }

    std::string wstringToString(const std::wstring &ws)
    {
        std::wstring_convert<std::codecvt_utf8<wchar_t> > & utf8Converter
            = getTlsUtf8Converter();

        return utf8Converter.to_bytes(ws);
    }

} // namespace RCF

