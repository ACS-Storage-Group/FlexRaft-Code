
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

#ifndef INCLUDE_SF_STRING_HPP
#define INCLUDE_SF_STRING_HPP

#include <algorithm>
#include <string>
#include <type_traits>

#include <RCF/Tools.hpp>
#include <SF/Archive.hpp>
#include <SF/Stream.hpp>
#include <RCF/Tchar.hpp>

namespace SF {

    template<typename C, typename T, typename A>
    inline void serializeString(SF::Archive & ar, std::basic_string<C,T,A> & s)
    {
        if (ar.isRead())
        {
            std::uint32_t count = 0;
            ar & count;

            SF::IStream &is = *ar.getIstream();

            s.resize(0);

            std::size_t minSerializedLength = sizeof(C);
            if (ar.verifyAgainstArchiveSize(count*minSerializedLength))
            {
                if (count > s.capacity())
                {
                    s.reserve(count);
                }
            }

            std::uint32_t charsRemaining = count;
            const std::uint32_t BufferSize = 512;
            C buffer[BufferSize];
            while (charsRemaining)
            {
                std::uint32_t charsToRead = RCF_MIN(BufferSize, charsRemaining);
                std::uint32_t bytesToRead = charsToRead*sizeof(C);

                if ( is.read((char *)buffer, bytesToRead) != bytesToRead )
                {
                    RCF::Exception e(RCF::RcfError_SfReadFailure);
                    RCF_THROW(e);
                }

                s.append(buffer, charsToRead);
                charsRemaining -= charsToRead;
            }
        }
        else if (ar.isWrite())
        {
            std::uint32_t count = static_cast<std::uint32_t >(s.length());
            ar & count;
            ar.getOstream()->writeRaw(
                (char *) s.c_str(),
                count*sizeof(C));
        }

    }

    RCF_EXPORT bool getCurrentNativeWstringSerialization();

    template<typename C, typename T, typename A>
    inline void serializeEncodedString(SF::Archive & ar, std::basic_string<C,T,A> & ws)
    {
        if (ar.getRuntimeVersion() < 8)
        {
            serializeString(ar, ws);
            return;
        }

        RCF_ASSERT(ar.getRuntimeVersion() >= 8);

        if (ar.isRead())
        {
            int encodingType = 0;
            ar & encodingType;
            if (encodingType == 8)
            {
                // UTF-8 serialization.
                std::string s;
                ar & s;
                ws = RCF::stringToWstring(s);
            }
            else
            {
                // Native wchar_t serialization.

                if ( encodingType != sizeof(wchar_t) )
                {
                    RCF::Exception e(RCF::RcfError_WcharSizeMismatch, sizeof(wchar_t), encodingType);
                    RCF_THROW(e);
                }

                serializeString(ar, ws);
            }

        }
        else if (ar.isWrite())
        {
            // Default behavior is to convert to UTF-8. However there is also a setting on ClientStub that allows
            // user to instead select native wchar_t serialization.

            int encodingType = 8;
            bool useNativeWstringSerialization = getCurrentNativeWstringSerialization();
            if ( useNativeWstringSerialization )
            {
                encodingType = sizeof(wchar_t);
            }

            ar & encodingType;

            if (encodingType == 8)
            {
                // UTF-8 serialization.
                std::string s = RCF::wstringToString(ws);
                ar & s;
            }
            else
            {
                // Native wchar_t serialization.

                if ( encodingType != sizeof(wchar_t) )
                {
                    RCF::Exception e(RCF::RcfError_WcharSizeMismatch, sizeof(wchar_t), encodingType);
                    RCF_THROW(e);
                }

                serializeString(ar, ws);
            }
        }
    }

    template<typename C, typename T, typename A>
    inline void serializeEncodedStringOrNot(SF::Archive & ar, std::basic_string<C, T, A> & t, std::true_type *)
    {
        serializeEncodedString(ar, t);
    }

    template<typename C, typename T, typename A>
    inline void serializeEncodedStringOrNot(SF::Archive & ar, std::basic_string<C, T, A> & t, std::false_type *)
    {
        serializeString(ar, t);
    }

    // std::basic_string (with wstring support)
    template<typename C, typename T, typename A>
    inline void serialize_vc6(SF::Archive & ar, std::basic_string<C,T,A> & t, const unsigned int)
    {
        typedef typename std::is_same<C, wchar_t>::type type;
        serializeEncodedStringOrNot(ar, t, (type *) NULL);
    }

} // namespace SF

#endif // ! INCLUDE_SF_STRING_HPP
