
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

#ifndef INCLUDE_RCF_SSPICREDENTIALS_HPP
#define INCLUDE_RCF_SSPICREDENTIALS_HPP

#include <RCF/Enums.hpp>
#include <RCF/Tchar.hpp>

#ifndef SECURITY_WIN32
#define SECURITY_WIN32
#endif

#include <windows.h>
#include <security.h>
#include <WinCrypt.h>
#include <tchar.h>

#ifdef _UNICODE

#define INIT_SEC_INTERFACE_NAME       "InitSecurityInterfaceW"
typedef unsigned short UTCHAR;

#else

#define INIT_SEC_INTERFACE_NAME       "InitSecurityInterfaceA"
typedef unsigned char UTCHAR;

#endif

namespace RCF {

    class SspiCredentials;
    typedef std::shared_ptr<SspiCredentials> SspiCredentialsPtr;

    class SspiCredentials
    {
    public:

        // For Kerberos/NTLM.
        SspiCredentials(
            SspiRole sspiRole,
            const tstring& packageName,
            const tstring& packageList,
            const tstring &userName = RCF_T(""),
            const tstring &password = RCF_T(""));

        // For Schannel.
        SspiCredentials(
            SspiRole sspiRole,
            bool enableManualCertValidation,
            DWORD enabledProtocols,
            CertificatePtr localCertPtr);

        ~SspiCredentials();

        CredHandle                              mCredentials;
        SecPkgInfo *                            mpPkgInfo = NULL;
        SspiRole                                mSspiRole = Sr_Client;
        bool                                    mEnableManualCertValidation = false;
        DWORD                                   mEnabledProtocols = 0;
        Win32CertificatePtr                     mLocalCertPtr;

        tstring                                 mUserName;
        const tstring                           mPackageName;
        const tstring                           mPackageList;

    private:

        void            setupCredentials(
            const tstring &userName,
            const tstring &password,
            const tstring &domain);

        void            setupCredentialsSchannel();

        void            freeCredentials();

        void            setupPkgInfo();


    };  

} // namespace RCF

#endif // ! INCLUDE_RCF_SSPICREDENTIALS_HPP

