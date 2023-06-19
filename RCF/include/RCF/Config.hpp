
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

#ifndef INCLUDE_RCF_CONFIG_HPP
#define INCLUDE_RCF_CONFIG_HPP

#include <RCF/TypeTraits.hpp>

#ifndef RCF_MAX_METHOD_COUNT
#define RCF_MAX_METHOD_COUNT 100
#endif

// C++11 requires us to mark some destructors as throwing.
#define RCF_DTOR_THROWS noexcept(false)

#ifdef _WIN32
#define RCF_WINDOWS
#endif

#ifdef RCF_USE_CONSTEXPR
#define RCF_CONSTEXPR constexpr
#else
#define RCF_CONSTEXPR
#endif

//------------------------------------------------------------------------------
// Determine which features to compile.

#ifndef RCF_PRO
#define RCF_PRO 1
#endif

#ifndef RCF_FEATURE_LEGACY
#define RCF_FEATURE_LEGACY          0
#endif

// RCF_FEATURE_LOCALSOCKET not supported on Windows platforms.
#if defined(RCF_FEATURE_LOCALSOCKET) && defined(RCF_WINDOWS)
#undef RCF_FEATURE_LOCALSOCKET
#define RCF_FEATURE_LOCALSOCKET     0
#endif

// RCF_FEATURE_NAMEDPIPE not supported on non-Windows platforms.
#if defined(RCF_FEATURE_NAMEDPIPE) && !defined(RCF_WINDOWS)
#undef RCF_FEATURE_NAMEDPIPE
#define RCF_FEATURE_NAMEDPIPE       0
#endif

// RCF_FEATURE_SSPI not supported on non-Windows platforms.
#if defined(RCF_FEATURE_SSPI) && !defined(RCF_WINDOWS)
#undef RCF_FEATURE_SSPI
#define RCF_FEATURE_SSPI            0
#endif


// For any features left undefined, define to default value.

// Zlib feature.
#ifndef RCF_FEATURE_ZLIB
#ifdef RCF_USE_ZLIB
#define RCF_FEATURE_ZLIB            1
#else
#define RCF_FEATURE_ZLIB            0
#endif
#endif

// OpenSSL feature.
#ifndef RCF_FEATURE_OPENSSL
#ifdef RCF_USE_OPENSSL
#define RCF_FEATURE_OPENSSL         1
#else
#define RCF_FEATURE_OPENSSL         0
#endif
#endif

// SSPI feature.
#ifndef RCF_FEATURE_SSPI
#if defined(RCF_WINDOWS) && RCF_PRO == 1
#define RCF_FEATURE_SSPI                1
#else
#define RCF_FEATURE_SSPI                0
#endif
#endif

// File transfer feature. Off by default as it requires C++17. 
#ifndef RCF_FEATURE_FILETRANSFER
#define RCF_FEATURE_FILETRANSFER    0
#endif

// Server feature
#ifndef RCF_FEATURE_SERVER
#define RCF_FEATURE_SERVER          1
#endif

// Server feature
#ifndef RCF_FEATURE_PROXYENDPOINT
#define RCF_FEATURE_PROXYENDPOINT          1
#endif

// Publish/subscribe feature.
#ifndef RCF_FEATURE_PUBSUB
#define RCF_FEATURE_PUBSUB          1
#endif

// HTTP/HTTPS feature.
#ifndef RCF_FEATURE_HTTP
#define RCF_FEATURE_HTTP            1
#endif

// UDP feature
#ifndef RCF_FEATURE_UDP
#define RCF_FEATURE_UDP             1
#endif

// Win32 named pipes feature.
#ifndef RCF_FEATURE_NAMEDPIPE
#ifdef RCF_WINDOWS
#define RCF_FEATURE_NAMEDPIPE           1
#else
#define RCF_FEATURE_NAMEDPIPE           0
#endif
#endif

// Unix local sockets feature.
#ifndef RCF_FEATURE_LOCALSOCKET
#ifdef RCF_WINDOWS
#define RCF_FEATURE_LOCALSOCKET         0
#else
#define RCF_FEATURE_LOCALSOCKET         1
#endif
#endif

// TCP feature.
#ifndef RCF_FEATURE_TCP
#define RCF_FEATURE_TCP             1
#endif

// IPv6 feature
#ifndef RCF_FEATURE_IPV6
#define RCF_FEATURE_IPV6            1
#endif

// Protocol Buffers feature
#ifndef RCF_FEATURE_PROTOBUF
#ifdef RCF_USE_PROTOBUF
#define RCF_FEATURE_PROTOBUF    1
#else
#define RCF_FEATURE_PROTOBUF    0
#endif
#endif

// Custom allocator feature.
#ifndef RCF_FEATURE_CUSTOM_ALLOCATOR
#ifdef RCF_USE_CUSTOM_ALLOCATOR
#define RCF_FEATURE_CUSTOM_ALLOCATOR    1
#else
#define RCF_FEATURE_CUSTOM_ALLOCATOR    0
#endif
#endif


// RCF_FEATURE_SF / RCF_FEATURE_BOOST_SERIALIZATION.
// For backward compatibility we need to interpret RCF_USE_SF_SERIALIZATION / RCF_USE_BOOST_SERIALIZATION correctly.

#ifndef RCF_FEATURE_SF
#ifdef RCF_USE_SF_SERIALIZATION
#define RCF_FEATURE_SF                      1
#elif !defined(RCF_USE_SF_SERIALIZATION) && defined(RCF_USE_BOOST_SERIALIZATION)
#define RCF_FEATURE_SF                      0
#else
#define RCF_FEATURE_SF                      1
#endif
#endif

#ifndef RCF_FEATURE_BOOST_SERIALIZATION
#ifdef RCF_USE_BOOST_SERIALIZATION
#define RCF_FEATURE_BOOST_SERIALIZATION     1
#else
#define RCF_FEATURE_BOOST_SERIALIZATION     0
#endif
#endif

//------------------------------------------------------------------------------

// Detect TR1 availability.
#ifndef RCF_USE_TR1

    // MSVC
    #if defined(_MSC_VER) && (_MSC_VER >= 1600 || (_MSC_VER == 1500 && _MSC_FULL_VER >= 150030729))
    #define RCF_USE_TR1
    #define RCF_TR1_HEADER(x) <x>
    #endif

    // GCC
    #if defined(__GNUC__) && (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 3))
    #define RCF_USE_TR1
    #define RCF_TR1_HEADER(x) <tr1/x>
    #endif

#endif // RCF_USE_TR1

// Detect hash_map/hash_set availability.
#ifndef RCF_USE_HASH_MAP

    #if (defined(_MSC_VER) && _MSC_VER >= 1310) || (defined(__GNUC__) && __GNUC__ == 3)
        #define RCF_USE_HASH_MAP
        #if defined(_MSC_VER)
            #define RCF_HASH_MAP_HEADER(x) <x>
            #define RCF_HASH_MAP_NS stdext
        #elif defined(__GNUC__)
            #define RCF_HASH_MAP_HEADER(x) <ext/x>
            #define RCF_HASH_MAP_NS __gnu_cxx
        #endif
    #endif

#endif // RCF_USE_HASH_MAP

namespace RCF {

    typedef Int<RCF_FEATURE_SF>                         RcfConfig_SF;
    typedef Int<RCF_FEATURE_BOOST_SERIALIZATION>        RcfConfig_BSer;
    typedef Int<RCF_FEATURE_FILETRANSFER>               RcfConfig_FileTransfer;
    typedef Int<RCF_FEATURE_PROTOBUF>                   RcfConfig_Protobufs;

    template<int N1, int N2, int N3, int N4>
    struct RcfConfigurationDetectMismatches
    {
    };

    typedef RcfConfigurationDetectMismatches<
        RcfConfig_SF::value, 
        RcfConfig_BSer::value, 
        RcfConfig_FileTransfer::value,
        RcfConfig_Protobufs::value>                     RcfConfigT;

} // namespace RCF

#ifdef _MSC_VER
#ifndef _CPPRTTI
  // RTTI support not detected!
  // If you're using Visual C++, set "Enable Run-Time Type Info" to true,
  // in the Project Properties | C/C++ | Language options,
#error RCF requires Run-Time Type Info support to be enabled.
#endif
#endif

// Borrowed from Boost, to determine which current function macro to use.
#if defined(__GNUC__) || (defined(__MWERKS__) && (__MWERKS__ >= 0x3000)) || (defined(__ICC) && (__ICC >= 600)) || defined(__ghs__)
# define RCF_CURRENT_FUNCTION __PRETTY_FUNCTION__
#elif defined(__DMC__) && (__DMC__ >= 0x810)
# define RCF_CURRENT_FUNCTION __PRETTY_FUNCTION__
#elif defined(__FUNCSIG__)
# define RCF_CURRENT_FUNCTION __FUNCSIG__
#elif (defined(__INTEL_COMPILER) && (__INTEL_COMPILER >= 600)) || (defined(__IBMCPP__) && (__IBMCPP__ >= 500))
# define RCF_CURRENT_FUNCTION __FUNCTION__
#elif defined(__BORLANDC__) && (__BORLANDC__ >= 0x550)
# define RCF_CURRENT_FUNCTION __FUNC__
#elif defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901)
# define RCF_CURRENT_FUNCTION __func__
#elif defined(__cplusplus) && (__cplusplus >= 201103)
# define RCF_CURRENT_FUNCTION __func__
#else
# define RCF_CURRENT_FUNCTION "(unknown)"
#endif

// Need these defines so asio doesn't pull in any Boost code.
#ifndef RCF_USE_BOOST_ASIO

#ifndef ASIO_STANDALONE
#define ASIO_STANDALONE
#endif

#ifndef ASIO_HAS_STD_CHRONO
#define ASIO_HAS_STD_CHRONO
#endif

#endif // ! RCF_USE_BOOST_ASIO

#endif // ! INCLUDE_RCF_CONFIG_HPP
