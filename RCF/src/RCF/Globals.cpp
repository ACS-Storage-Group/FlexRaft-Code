
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

#include <RCF/Globals.hpp>

#include <RCF/DynamicLib.hpp>
#include <RCF/Enums.hpp>
#include <RCF/Tools.hpp>

namespace RCF {

    Globals * gpGlobals = NULL;

    Globals & globals()
    {
        RCF_ASSERT(gpGlobals);
        return *gpGlobals;
    }

    Globals::Globals() :
        mpZlibDll(NULL),
        mpOpenSslDll(NULL),
        mpOpenSslCryptoDll(NULL),
        mSimultaneousPublishLimit(100)
    {

#ifdef RCF_WINDOWS

        mZlibDllName            = "zlib.dll";
        mOpenSslDllName         = "libssl.dll";
        mOpenSslCryptoDllName   = "libcrypto.dll";

#else

        mZlibDllName            = "libz.so";
        mOpenSslDllName         = "libssl.so";
        mOpenSslCryptoDllName   = "libcrypto.so";

#endif

        mFileStreamDefaultDownloadDirectory = "RCF-Downloads";
    }

    Globals::~Globals()
    {
        releaseZlibDll();
        releaseOpenSslCryptoDll();
        releaseOpenSslDll();
    }

    void Globals::setDefaultSslImplementation(SslImplementation sslImplementation)
    {
        mDefaultSslImplementation = sslImplementation;
    }

    SslImplementation Globals::getDefaultSslImplementation()
    {
        return mDefaultSslImplementation;
    }

    void Globals::setDefaultConnectTimeoutMs(unsigned int connectTimeoutMs)
    {
        mClientConnectTimeoutMs = connectTimeoutMs;
    }

    unsigned int Globals::getDefaultConnectTimeoutMs()
    {
        return mClientConnectTimeoutMs;
    }

    void Globals::setDefaultRemoteCallTimeoutMs(unsigned int remoteCallTimeoutMs)
    {
        mClientRemoteCallTimeoutMs = remoteCallTimeoutMs;
    }

    unsigned int Globals::getDefaultRemoteCallTimeoutMs()
    {
        return mClientRemoteCallTimeoutMs;
    }

    void Globals::setZlibDllName(const std::string & dllName)
    {
        mZlibDllName = dllName;
    }

    std::string Globals::getZlibDllName() const
    {
        return mZlibDllName;
    }

    void Globals::setOpenSslDllName(const std::string & dllName)
    {
        mOpenSslDllName = dllName;
    }

    std::string Globals::getOpenSslDllName() const
    {
        return mOpenSslDllName;
    }

    void Globals::setOpenSslCryptoDllName(const std::string & dllName)
    {
        mOpenSslCryptoDllName = dllName;
    }

    std::string Globals::getOpenSslCryptoDllName() const
    {
        return mOpenSslCryptoDllName;
    }

#if RCF_FEATURE_ZLIB==0

    void Globals::releaseZlibDll()
    {
        RCF_ASSERT(!mpZlibDll);
    }

#endif

#if RCF_FEATURE_OPENSSL==0

    void Globals::releaseOpenSslDll()
    {
        RCF_ASSERT(!mpOpenSslDll);
    }

    void Globals::releaseOpenSslCryptoDll()
    {
        RCF_ASSERT(!mpOpenSslCryptoDll);
    }

#endif

    void Globals::setSimultaneousPublishLimit(std::size_t simultaneousPublishLimit)
    {
        mSimultaneousPublishLimit = simultaneousPublishLimit;
    }

    std::size_t Globals::getSimultaneousPublishLimit() const
    {
        return mSimultaneousPublishLimit;
    }

    void Globals::setFileStreamDefaultDownloadDirectory(const std::string & downloadDirectory)
    {
        mFileStreamDefaultDownloadDirectory = downloadDirectory;
    }

    std::string Globals::getFileStreamDefaultDownloadDirectory()
    {
        return mFileStreamDefaultDownloadDirectory;
    }

} // namespace RCF
