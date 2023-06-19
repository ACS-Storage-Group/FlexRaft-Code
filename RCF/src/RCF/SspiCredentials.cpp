
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

#include <RCF/SspiCredentials.hpp>

#include <sspi.h>
#include <schannel.h>

#include <RCF/Enums.hpp>
#include <RCF/Win32Certificate.hpp>

namespace RCF {

    PSecurityFunctionTable getSft();

    // For Kerberos/NTLM.
    SspiCredentials::SspiCredentials(
        SspiRole sspiRole,
        const tstring & packageName,
        const tstring & packageList,
        const tstring & userNameAndDomain,
        const tstring & password) : 
            mSspiRole(sspiRole),
            mPackageName(packageName), 
            mPackageList(packageList)
    {
        memset(&mCredentials, 0, sizeof(mCredentials));

        setupPkgInfo();

        // Split out the domain from the user name, if it is present.
        tstring userName = userNameAndDomain;
        tstring domain = RCF_T("");
        std::size_t pos = userNameAndDomain.find(RCF_T("\\"));
        if ( pos != tstring::npos && pos != userNameAndDomain.size() - 1 )
        {
            domain = userNameAndDomain.substr(0, pos);
            userName = userNameAndDomain.substr(pos + 1);
        }

        setupCredentials(userName, password, domain);
    }

    // For Schannel.
    SspiCredentials::SspiCredentials(
        SspiRole sspiRole,
        bool enableManualCertValidation,
        DWORD enabledProtocols,
        CertificatePtr localCertPtr) : 
            mSspiRole(sspiRole),
            mEnableManualCertValidation(enableManualCertValidation),
            mEnabledProtocols(enabledProtocols)
    {
        memset(&mCredentials, 0, sizeof(mCredentials));

        mLocalCertPtr = std::dynamic_pointer_cast<Win32Certificate>(localCertPtr);

        setupCredentialsSchannel();
    }

    void SspiCredentials::setupPkgInfo()
    {
        RCF_ASSERT(mPackageName.size() > 0);

        SECURITY_STATUS status = getSft()->QuerySecurityPackageInfo(
            (TCHAR*)mPackageName.c_str(),
            &mpPkgInfo);

        if ( status != SEC_E_OK )
        {
            Exception e(RcfError_Sspi, "QuerySecurityPackageInfo()", osError(status));
            RCF_THROW(e);
        }
    }

    SspiCredentials::~SspiCredentials()
    {
        freeCredentials();

        if ( mpPkgInfo )
        {
            getSft()->FreeContextBuffer((void*)mpPkgInfo);
        }
    }

    void SspiCredentials::freeCredentials()
    {
        SECURITY_STATUS status = 0;
        status = getSft()->FreeCredentialsHandle(&mCredentials);

        RCF_VERIFY(
            status == SEC_E_OK || status == SEC_E_INVALID_HANDLE,
            Exception(RcfError_Sspi, "FreeCredentialsHandle()", osError(status)));

        memset(&mCredentials, 0, sizeof(mCredentials));
    }

    void SspiCredentials::setupCredentials(
        const tstring &userName,
        const tstring &password,
        const tstring &domain)
    {

        SEC_WINNT_AUTH_IDENTITY_EX identity     = {0};

        UTCHAR *pDomain = reinterpret_cast<UTCHAR*>(const_cast<TCHAR*>(domain.c_str()));
        unsigned long pDomainLen = static_cast<unsigned long>(domain.length());

        UTCHAR *pUsername = reinterpret_cast<UTCHAR*>(const_cast<TCHAR*>(userName.c_str()));
        unsigned long pUsernameLen = static_cast<unsigned long>(userName.length());

        UTCHAR *pPassword = reinterpret_cast<UTCHAR*>(const_cast<TCHAR*>(password.c_str()));
        unsigned long pPasswordLen = static_cast<unsigned long>(password.length());

        UTCHAR *pPackages = reinterpret_cast<UTCHAR*>(const_cast<TCHAR*>(mPackageList.c_str()));
        unsigned long pPackagesLen = static_cast<unsigned long>(mPackageList.length());

        if (!userName.empty())
        {
            if (!domain.empty())
            {
                identity.Domain                 = pDomain;
                identity.DomainLength           = pDomainLen;
            }
            if (!userName.empty())
            {
                identity.User                   = pUsername;
                identity.UserLength             = pUsernameLen;
            }
            if (!password.empty())
            {
                identity.Password               = pPassword;
                identity.PasswordLength         = pPasswordLen;
            }
        }

#ifdef _UNICODE
        identity.Flags                          = SEC_WINNT_AUTH_IDENTITY_UNICODE;
#else
        identity.Flags                          = SEC_WINNT_AUTH_IDENTITY_ANSI;
#endif

        identity.Version                        = SEC_WINNT_AUTH_IDENTITY_VERSION;
        identity.Length                         = sizeof(identity);
        if (!mPackageList.empty())
        {
            identity.PackageList                = pPackages;
            identity.PackageListLength          = pPackagesLen;
        }
        SEC_WINNT_AUTH_IDENTITY_EX *pIdentity = &identity;

        TimeStamp Expiration                    = {0};

        SECURITY_STATUS status = getSft()->AcquireCredentialsHandle(
            NULL,
            mpPkgInfo->Name,
            (mSspiRole == Sr_Server) ? SECPKG_CRED_INBOUND : SECPKG_CRED_OUTBOUND ,
            NULL,
            pIdentity,
            NULL, NULL,
            &mCredentials,
            &Expiration);

        if (status != SEC_E_OK)
        {
            Exception e(RcfError_Sspi, "AcquireCredentialsHandle()", osError(status));
            RCF_THROW(e);
        }

        if (domain.size() > 0)
        {
            mUserName = domain + _T("\\");
        }
        if (userName.size() > 0)
        {
            mUserName += userName;
        }
    }

    void SspiCredentials::setupCredentialsSchannel()
    {
        SCHANNEL_CRED schannelCred          = {0};       
        schannelCred.dwVersion              = SCHANNEL_CRED_VERSION;
        PCCERT_CONTEXT pCertContext         = NULL;
        if(mLocalCertPtr)
        {
            pCertContext                    = mLocalCertPtr->getWin32Context();
            schannelCred.cCreds             = 1;
            schannelCred.paCred             = &pCertContext;
        }

        schannelCred.grbitEnabledProtocols  = mEnabledProtocols;

        if ( mSspiRole == Sr_Server )
        {
            if ( mEnableManualCertValidation )
            {
                // Server side manual validation.
                schannelCred.dwFlags            = SCH_CRED_MANUAL_CRED_VALIDATION;
            }
            else
            {
                // Server side auto validation.
                schannelCred.dwFlags            = 0;
            }
        }
        else
        {
            if ( mEnableManualCertValidation )
            {
                // Client side manual validation.
                schannelCred.dwFlags            = SCH_CRED_NO_DEFAULT_CREDS | SCH_CRED_MANUAL_CRED_VALIDATION;
            }
            else
            {
                // Client side auto validation.
                schannelCred.dwFlags            = SCH_CRED_NO_DEFAULT_CREDS | SCH_CRED_AUTO_CRED_VALIDATION;
            }
        }

        SECURITY_STATUS status = getSft()->AcquireCredentialsHandle(
            NULL,
            UNISP_NAME,
            (mSspiRole == Sr_Server) ? SECPKG_CRED_INBOUND : SECPKG_CRED_OUTBOUND,
            NULL,
            &schannelCred,
            NULL, 
            NULL,
            &mCredentials,
            NULL);

        if (status != SEC_E_OK)
        {
            Exception e(RcfError_Sspi, "AcquireCredentialsHandle()", osError(status));
            RCF_THROW(e);
        }
    }

} // namespace RCF
