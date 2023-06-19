
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

/// \file

#ifndef INCLUDE_RCF_GLOBALS_HPP
#define INCLUDE_RCF_GLOBALS_HPP

#include <string>

#include <RCF/Enums.hpp>
#include <RCF/Export.hpp>

namespace RCF {

    class ZlibDll;
    class OpenSslDll;
    class OpenSslCryptoDll;
    class RcfSession;

    enum SslImplementation;

    /// Contains global RCF settings.
    class RCF_EXPORT Globals
    {

    public:
        Globals();
        ~Globals();

        /// Sets the default SSL implementation to use (OpenSSL or Schannel). The default is Schannel on Windows, and OpenSSL on other platforms.
        void                setDefaultSslImplementation(SslImplementation sslImplementation);

        /// Gets the default SSL implementation.
        SslImplementation   getDefaultSslImplementation();

        /// Sets the default connect timeout for all RCF clients.
        void                setDefaultConnectTimeoutMs(unsigned int connectTimeoutMs);

        /// Gets the default connect timeout for all RCF clients.
        unsigned int        getDefaultConnectTimeoutMs();

        /// Sets the default remote call timeout for all RCF clients.
        void                setDefaultRemoteCallTimeoutMs(unsigned int remoteCallTimeoutMs);

        /// Gets the default remote call timeout for all RCF clients.
        unsigned int        getDefaultRemoteCallTimeoutMs();

        /// Sets the name of the Zlib library, when loading ZLib dynamically at runtime.
        void                setZlibDllName(const std::string & dllName);

        /// Gets the name of the Zlib library, when loading ZLib dynamically at runtime.
        std::string         getZlibDllName() const;

        /// Sets the name of the OpenSSL library, when loading OpenSSL dynamically at runtime.
        void                setOpenSslDllName(const std::string & dllName);

        /// Gets the name of the OpenSSL library, when loading OpenSSL dynamically at runtime.
        std::string         getOpenSslDllName() const;

        /// Sets the name of the OpenSSL Crypto library, when loading OpenSSL dynamically at runtime.
        void                setOpenSslCryptoDllName(const std::string & dllName);

        /// Gets the name of the OpenSSL Crypto library, when loading OpenSSL dynamically at runtime.
        std::string         getOpenSslCryptoDllName() const;

        // Sets the simultaneous publish limit. 
        // The publishing service issues messages asynchronously across all subscriber connections, and this setting controls how many asynchronous writes it can perform concurrently.
        void                setSimultaneousPublishLimit(std::size_t simultaneousPublishLimit);

        // Gets the simultaneous publish limit.
        std::size_t         getSimultaneousPublishLimit() const;

        // Default download directory for FileStream objects (deprecated).
        void                setFileStreamDefaultDownloadDirectory(const std::string & downloadDirectory);
        std::string         getFileStreamDefaultDownloadDirectory();


        ZlibDll &           getZlibDll();
        OpenSslDll &        getOpenSslDll();
        OpenSslCryptoDll &  getOpenSslCryptoDll();

    private:

        void                releaseZlibDll();
        void                releaseOpenSslDll();
        void                releaseOpenSslCryptoDll();

        ZlibDll *           mpZlibDll;
        OpenSslDll *        mpOpenSslDll;
        OpenSslCryptoDll *  mpOpenSslCryptoDll;

        std::string         mZlibDllName;
        std::string         mOpenSslDllName;
        std::string         mOpenSslCryptoDllName;

        std::size_t         mSimultaneousPublishLimit;

        std::string         mFileStreamDefaultDownloadDirectory;

#if defined(RCF_WINDOWS) && RCF_FEATURE_OPENSSL==0
        // Windows builds without OpenSSL.
        SslImplementation   mDefaultSslImplementation = Si_Schannel;
#else
        // All other builds.
        SslImplementation   mDefaultSslImplementation = Si_OpenSsl;
#endif

        unsigned int        mClientConnectTimeoutMs = 1000 * 2;
        unsigned int        mClientRemoteCallTimeoutMs = 1000 * 10;
    };

    /// \addtogroup Functions
    /// @{

    /// Exposes RCF global settings.
    RCF_EXPORT Globals & globals();

    /// @}

} // namespace RCF

#endif // ! INCLUDE_RCF_GLOBALS_HPP
