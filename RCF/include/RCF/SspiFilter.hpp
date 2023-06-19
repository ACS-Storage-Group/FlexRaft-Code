
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

#ifndef INCLUDE_RCF_SSPIFILTER_HPP
#define INCLUDE_RCF_SSPIFILTER_HPP

#include <functional>
#include <memory>

#include <RCF/ByteBuffer.hpp>
#include <RCF/Enums.hpp>
#include <RCF/Filter.hpp>
#include <RCF/RcfFwd.hpp>
#include <RCF/RecursionLimiter.hpp>
#include <RCF/ThreadLibrary.hpp>
#include <RCF/Export.hpp>

#include <RCF/Tchar.hpp>

#ifndef SECURITY_WIN32
#define SECURITY_WIN32
#endif

#include <windows.h>
#include <security.h>
#include <schannel.h>
#include <WinCrypt.h>
#include <tchar.h>

namespace RCF {

    typedef RCF::tstring tstring;

    class SspiFilter;
    class RcfSession;
    class ClientStub;

    typedef std::shared_ptr<SspiFilter> SspiFilterPtr;

    /// Allows the server side of a SSPI-based connection to impersonate the client. Only applicable to connections using NTLM, Kerberos or Negotiate transport protocols.
    class RCF_EXPORT SspiImpersonator
    {
    public:
        SspiImpersonator(SspiFilterPtr sspiFilterPtr);

        /// Impersonates the client associated with the RcfSession. Impersonation lasts until revertToSelf() is called, or the Win32NamedPipeImpersonator object is destroyed.
        SspiImpersonator(RcfSession & session);

        /// Destroys the SspiImpersonator object, and ceases any impersonation.
        ~SspiImpersonator();

        bool impersonate();

        /// Ceases impersonation of the named pipe client.
        void revertToSelf() const;

    private:
        SspiFilterPtr mSspiFilterPtr;
    };

    static const ULONG DefaultSspiContextRequirements =
        ISC_REQ_REPLAY_DETECT   |
        ISC_REQ_SEQUENCE_DETECT |
        ISC_REQ_CONFIDENTIALITY |
        ISC_REQ_INTEGRITY       |
        ISC_REQ_DELEGATE        |
        ISC_REQ_MUTUAL_AUTH;

    static const ULONG DefaultSchannelContextRequirements =
        ASC_REQ_SEQUENCE_DETECT
        | ASC_REQ_REPLAY_DETECT
        | ASC_REQ_CONFIDENTIALITY
        | ASC_REQ_EXTENDED_ERROR
        | ASC_REQ_ALLOCATE_MEMORY
        | ASC_REQ_STREAM;

#if defined(SP_PROT_TLS1_3_SERVER) && defined(SP_PROT_TLS1_3_CLIENT)

    static const DWORD DefaultSchannelServerProtocols =
        SP_PROT_TLS1_3_SERVER
        | SP_PROT_TLS1_2_SERVER;

    static const DWORD DefaultSchannelClientProtocols =
        SP_PROT_TLS1_3_CLIENT
        | SP_PROT_TLS1_2_CLIENT
        | SP_PROT_TLS1_1_CLIENT
        | SP_PROT_TLS1_0_CLIENT;

#else

    static const DWORD DefaultSchannelServerProtocols =
        SP_PROT_TLS1_2_SERVER;

    static const DWORD DefaultSchannelClientProtocols =
        SP_PROT_TLS1_2_CLIENT
        | SP_PROT_TLS1_1_CLIENT
        | SP_PROT_TLS1_0_CLIENT;

#endif

    class SchannelClientFilter;
    typedef SchannelClientFilter SchannelFilter;

    class SchannelFilterFactory;

    class Certificate;
    class Win32Certificate;

    class SspiCredentials;
    typedef std::shared_ptr<SspiCredentials> SspiCredentialsPtr;
    
    class RCF_EXPORT SspiFilter : public Filter
    {
    public:

        ~SspiFilter();

        SspiMessageProtection getQop();

        typedef SspiImpersonator Impersonator;

        Win32CertificatePtr getPeerCertificate();

        PCtxtHandle getSecurityContext() const;

    protected:

        friend class SspiImpersonator;

        SspiFilter(
            SspiCredentialsPtr      credentialsPtr,
            ClientStub *            pClientStub,
            SspiMessageProtection   qop,
            ULONG                   contextRequirements,
            SspiRole                clientOrServer,
            SspiType                sspiType,
            const tstring &         packageName = RCF_T(""),
            const tstring &         packageList = RCF_T(""));

        enum Event
        {
            ReadIssued,
            WriteIssued,
            ReadCompleted,
            WriteCompleted
        };

        enum ContextState
        {
            AuthContinue,
            AuthOk,
            AuthOkAck,
            AuthFailed
        };

        enum State
        {
            Ready,
            Reading,
            Writing
        };

        void            freeContext();

        void            init();
        void            deinit();
        void            resetState();

        void            createClientCredentials();

        void            read(
                            const ByteBuffer &byteBuffer, 
                            std::size_t bytesRequested);

        void            write(const std::vector<ByteBuffer> &byteBuffers);

        void            onReadCompleted(const ByteBuffer &byteBuffer);
        void            onWriteCompleted(std::size_t bytesTransferred);

        void            handleEvent(Event event);
        void            readBuffer();
        void            writeBuffer();
        
        void            encryptWriteBuffer();
        bool            decryptReadBuffer();

        void            encryptWriteBufferSchannel();
        bool            decryptReadBufferSchannel();
        
        bool            completeReadBlock();
        bool            completeWriteBlock();
        bool            completeBlock();
        void            resumeUserIo();
        void            resizeReadBuffer(std::size_t newSize);
        void            resizeWriteBuffer(std::size_t newSize);

        void            shiftReadBuffer(bool shiftEntireBuffer = true);
        void            trimReadBuffer();

        bool            shouldRetryWithExtraData(const SecBufferDesc& ibd, const SecBufferDesc& obd);

        virtual void    handleHandshakeEvent() = 0;

    protected:

        ClientStub *                            mpClientStub;

        SspiMessageProtection                   mQop;
        ULONG                                   mContextRequirements;

        SspiCredentialsPtr                      mCredentialsPtr;
        tstring                                 mPackageName;
        tstring                                 mPackageList;

        bool                                    mHaveContext;
        CtxtHandle                              mContext;
        ContextState                            mContextState;
        State                                   mPreState;
        State                                   mPostState;
        Event                                   mEvent;
        const SspiRole                          mClientOrServer;

        ByteBuffer                              mReadByteBufferOrig;
        ByteBuffer                              mWriteByteBufferOrig;
        std::size_t                             mBytesRequestedOrig;

        ByteBuffer                              mReadByteBuffer;
        ReallocBufferPtr                        mReadBufferVectorPtr;
        char *                                  mReadBuffer;
        std::size_t                             mReadBufferPos;
        std::size_t                             mReadBufferLen;

        ByteBuffer                              mWriteByteBuffer;
        ReallocBufferPtr                        mWriteBufferVectorPtr;
        char *                                  mWriteBuffer;
        std::size_t                             mWriteBufferPos;
        std::size_t                             mWriteBufferLen;

        std::vector<ByteBuffer>                 mByteBuffers;
        ByteBuffer                              mTempByteBuffer;

        // Are we doing Schannel or Kerberos/NTLM.
        const SspiType                          mSspiType;

        std::size_t                             mMaxMessageLength;
 
        // Schannel-specific members.
        Win32CertificatePtr                     mLocalCertPtr;
        Win32CertificatePtr                     mRemoteCertPtr;
        CertificateValidationCallback           mCertValidationCallback;
        tstring                                 mAutoCertValidation;
        tstring                                 mTlsSniName;
        const std::size_t                       mReadAheadChunkSize;
        std::size_t                             mRemainingDataPos;

        std::vector<RCF::ByteBuffer>            mMergeBufferList;
        std::vector<char>                       mMergeBuffer;

        bool                                    mProtocolChecked;

        bool                                    mResumeUserIoAfterWrite = false;

    private:
        bool                                    mLimitRecursion;
        RecursionState<ByteBuffer, int>         mRecursionStateRead;
        RecursionState<std::size_t, int>        mRecursionStateWrite;

        void onReadCompleted_(const ByteBuffer &byteBuffer);
        void onWriteCompleted_(std::size_t bytesTransferred);

        friend class SchannelFilterFactory;
    };


    // Server filters.

    class RCF_EXPORT SspiServerFilter : public SspiFilter
    {
    public:
        SspiServerFilter(
            SspiCredentialsPtr credentialsPtr,
            SspiType sspiType);

    private:
        bool doHandshakeSchannel();
        bool doHandshake();
        void handleHandshakeEvent();
    };

    class NtlmServerFilter : public SspiServerFilter
    {
    public:
        NtlmServerFilter(SspiCredentialsPtr credentialsPtr);
        int getFilterId() const;
    };

    class KerberosServerFilter : public SspiServerFilter
    {
    public:
        KerberosServerFilter(SspiCredentialsPtr credentialsPtr);
        int getFilterId() const;
    };

    class NegotiateServerFilter : public SspiServerFilter
    {
    public:
        NegotiateServerFilter(SspiCredentialsPtr credentialsPtr);
        int getFilterId() const;
    };

    // Server filter factories.

    class RCF_EXPORT NtlmFilterFactory : public FilterFactory
    {
    public:
        NtlmFilterFactory();

        FilterPtr   createFilter(RcfServer & server);
        int         getFilterId();

    private:

        Mutex               mCredentialsMutex;
        SspiCredentialsPtr  mCredentialsPtr;
    };

    class KerberosFilterFactory : public FilterFactory
    {
    public:
        KerberosFilterFactory();

        FilterPtr   createFilter(RcfServer & server);
        int         getFilterId();

    private:

        Mutex               mCredentialsMutex;
        SspiCredentialsPtr  mCredentialsPtr;
    };

    class NegotiateFilterFactory : public FilterFactory
    {
    public:
        NegotiateFilterFactory(const tstring &packageList = RCF_T("Kerberos, NTLM"));

        FilterPtr   createFilter(RcfServer & server);
        int         getFilterId();
    
    private:

        Mutex               mCredentialsMutex;
        tstring             mPackageList;
        SspiCredentialsPtr  mCredentialsPtr;
    };

    // Client filters.

    class SspiClientFilter : public SspiFilter
    {
    public:
        SspiClientFilter(
            SspiCredentialsPtr      credentialsPtr,
            ClientStub *            pClientStub,
            SspiMessageProtection   qop,
            ULONG                   contextRequirements,
            SspiType                sspiType,
            const tstring &         packageName,
            const tstring &         packageList) :
                SspiFilter(
                    credentialsPtr,
                    pClientStub,
                    qop, 
                    contextRequirements, 
                    Sr_Client,
                    sspiType,
                    packageName,
                    packageList)
        {
        }

    private:

        bool doHandshakeSchannel();
        bool doHandshake();
        void handleHandshakeEvent();
    };

    class NtlmClientFilter : public SspiClientFilter
    {
    public:
        NtlmClientFilter(
            ClientStub *            pClientStub,
            SspiMessageProtection   qop = Smp_Encryption,
            ULONG                   contextRequirements = DefaultSspiContextRequirements);

        int getFilterId() const;
    };

    class KerberosClientFilter : public SspiClientFilter
    {
    public:
        KerberosClientFilter(
            ClientStub *            pClientStub,
            SspiMessageProtection   qop = Smp_Encryption,
            ULONG                   contextRequirements = DefaultSspiContextRequirements);

        int getFilterId() const;
    };

    class NegotiateClientFilter : public SspiClientFilter
    {
    public:

        NegotiateClientFilter(
            ClientStub *            pClientStub,
            SspiMessageProtection   qop = Smp_None,
            ULONG                   contextRequirements = DefaultSspiContextRequirements);


        int getFilterId() const;
    };

    typedef NtlmClientFilter            NtlmFilter;
    typedef KerberosClientFilter        KerberosFilter;
    typedef NegotiateClientFilter       NegotiateFilter;
    

    // These SSPI-prefixed typedefs make us compatible with code written for RCF 1.0.
    typedef NtlmFilter                  SspiNtlmFilter;
    typedef KerberosFilter              SspiKerberosFilter;
    typedef NegotiateFilter             SspiNegotiateFilter;

    typedef NtlmServerFilter            SspiNtlmServerFilter;
    typedef KerberosServerFilter        SspiKerberosServerFilter;
    typedef NegotiateServerFilter       SspiNegotiateServerFilter;
    typedef NtlmFilterFactory           SspiNtlmFilterFactory;
    typedef KerberosFilterFactory       SspiKerberosFilterFactory;
    typedef NegotiateFilterFactory      SspiNegotiateFilterFactory;
    typedef NtlmClientFilter            SspiNtlmClientFilter;
    typedef KerberosClientFilter        SspiKerberosClientFilter;
    typedef NegotiateClientFilter       SspiNegotiateClientFilter;

    typedef SspiFilter                  SspiFilterBase;
    typedef SspiFilterPtr               SspiFilterBasePtr;

} // namespace RCF

#endif // ! INCLUDE_RCF_SSPIFILTER_HPP
