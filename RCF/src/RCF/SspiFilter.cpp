
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

#include <RCF/SspiFilter.hpp>

#include <RCF/AsioServerTransport.hpp>
#include <RCF/ClientStub.hpp>
#include <RCF/ClientTransport.hpp>
#include <RCF/CurrentSession.hpp>
#include <RCF/InitDeinit.hpp>
#include <RCF/ObjectPool.hpp>
#include <RCF/RcfSession.hpp>
#include <RCF/Schannel.hpp>
#include <RCF/ServerTransport.hpp>
#include <RCF/SspiCredentials.hpp>
#include <RCF/ThreadLocalData.hpp>
#include <RCF/Tools.hpp>
#include <RCF/Win32Certificate.hpp>
#include <RCF/Win32Username.hpp>
#include <RCF/Log.hpp>

#include <tchar.h>

#include <sspi.h>

namespace RCF {

    PSecurityFunctionTable getSft();

    SspiFilter::SspiFilter(
        SspiCredentialsPtr credentialsPtr,
        ClientStub * pClientStub,
        SspiMessageProtection qop,
        ULONG contextRequirements,
        SspiRole clientOrServer,
        SspiType sspiType,
        const tstring & packageName,
        const tstring & packageList) :
            mCredentialsPtr(credentialsPtr),
            mPackageName(packageName),
            mPackageList(packageList),
            mpClientStub(pClientStub),
            mQop(qop),
            mContextRequirements(contextRequirements),
            mClientOrServer(clientOrServer),
            mPreState(Ready),
            mBytesRequestedOrig(),
            mWriteBuffer(),
            mWriteBufferPos(),
            mWriteBufferLen(),
            mReadBuffer(),
            mReadBufferPos(),
            mReadBufferLen(),
            mPostState(Ready),
            mHaveContext(),
            mContext(),
            mContextState(AuthContinue),
            mEvent(ReadIssued),
            mLimitRecursion(clientOrServer == Sr_Client),
            mSspiType(sspiType),
            mMaxMessageLength(),
            mReadAheadChunkSize(sspiType == St_Schannel ? 0x10000 : 4),
            mRemainingDataPos()
    {
        init();
    }

    SspiFilter::~SspiFilter()
    {
        RCF_DTOR_BEGIN
            deinit();
        RCF_DTOR_END
    }

    SspiMessageProtection SspiFilter::getQop()
    {
        return mQop;
    }

    void SspiFilter::freeContext()
    {
        if (mHaveContext)
        {
            SECURITY_STATUS status = 0;
            status = getSft()->DeleteSecurityContext(&mContext);

            RCF_VERIFY(
                status == SEC_E_OK || status == SEC_E_INVALID_HANDLE,
                Exception(RcfError_Sspi, "DeleteSecurityContext()", osError(status)));

            mHaveContext = false;
        }

        memset(&mContext, 0, sizeof(mContext));
    }

    void SspiFilter::resetState()
    {
        init();
    }

    void SspiFilter::deinit()
    {
        freeContext();

        mReadBufferVectorPtr.reset();
        mWriteBufferVectorPtr.reset();

        // We no longer reset the credentials, as I'm not aware of any reason to.
        
        //if ( mClientOrServer == Sr_Client )
        //{
        //    mCredentialsPtr.reset();
        //}
    }

    void SspiFilter::init()
    {
        deinit();

        mPreState = Ready;
        mPostState = Ready;
        mContextState = AuthContinue;
        mEvent = ReadIssued;

        mProtocolChecked = false;

        resizeReadBuffer(0);
        resizeWriteBuffer(0);

        // Server filter must have credentials supplied by the filter factory.
        RCF_ASSERT(mCredentialsPtr || (!mCredentialsPtr && mClientOrServer == Sr_Client));

    }

    void SspiFilter::createClientCredentials()
    {
        if ( !mCredentialsPtr && mClientOrServer == Sr_Client && mpClientStub )
        {
            if ( mSspiType == St_Schannel )
            {
                mCredentialsPtr.reset(new SspiCredentials(
                    Sr_Client,
                    mpClientStub->getCertificateValidationCallback() ? true : false,
                    mpClientStub->getSchannelEnabledProtocols() ? mpClientStub->getSchannelEnabledProtocols() : DefaultSchannelClientProtocols,
                    mpClientStub->getCertificate()));
            }
            else
            {
                mCredentialsPtr.reset( new SspiCredentials(
                    Sr_Client,
                    mPackageName,
                    mPackageList,
                    mpClientStub->getUserName(),
                    mpClientStub->getPassword())
                );
            }
        }
    }

    void SspiFilter::read(
        const ByteBuffer &byteBuffer,
        std::size_t bytesRequested)
    {
        // Client-side - cache the current max message length setting.
        if ( mClientOrServer == Sr_Client )
        {
            ClientStub * pClientStub = getTlsClientStubPtr();
            if (pClientStub)
            {
                ClientTransport & clientTransport = pClientStub->getTransport();
                mMaxMessageLength = clientTransport.getMaxIncomingMessageLength();
            }
        }

        if (byteBuffer.isEmpty() && bytesRequested == 0)
        {
            // If we have some data, issue a read completion, otherwise return
            // our buffers to the pool and issue a zero byte read on the next
            // filter.

            if (mReadBufferPos < mReadBufferLen)
            {
                mpPreFilter->onReadCompleted(byteBuffer);
            }
            else if (mReadBufferPos == mReadBufferLen && mRemainingDataPos > 0)
            {
                mpPreFilter->onReadCompleted(byteBuffer);
            }
            else
            {
                RCF_ASSERT(mReadBufferPos == mReadBufferLen);

                if (mReadBufferVectorPtr)
                {
                    mReadBufferVectorPtr->resize(0);
                }
                if (mWriteBufferVectorPtr)
                {
                    mWriteBufferVectorPtr->resize(0);
                }
                
                mTempByteBuffer.clear();
                mReadByteBuffer.clear();
                mWriteByteBuffer.clear();

                mReadBufferVectorPtr.reset();
                mWriteBufferVectorPtr.reset();

                // Forward the zero-byte read to the next filter.
                mReadByteBufferOrig = byteBuffer;
                mBytesRequestedOrig = bytesRequested;
                mPreState = Reading;

                mpPostFilter->read(ByteBuffer(), 0);
            }
        }
        else
        {
            mReadByteBufferOrig = byteBuffer;
            mBytesRequestedOrig = bytesRequested;
            mPreState = Reading;
            handleEvent(ReadIssued);
        }
    }

    void SspiFilter::write(const std::vector<ByteBuffer> &byteBuffers)
    {
        // Client-side - cache the current max message length setting.
        if ( mClientOrServer == Sr_Client )
        {
            ClientStub * pClientStub = getTlsClientStubPtr();
            if (pClientStub)
            {
                ClientTransport & clientTransport = pClientStub->getTransport();
                mMaxMessageLength = clientTransport.getMaxIncomingMessageLength();
            }
        }

        // TODO: can we pass multiple buffers through to lower layers, and still 
        // have them coalesced at the network send stage?

        // We used to have an upper limit here, but that doesn't work if there is a
        // HTTP frame further down in the filter stack. The HTTP frame needs to get
        // all the data in one go, otherwise we end up with multiple sends which
        // breaks HTTP semantics.
        // So now we merge the buffers, regardless of their sizes.
        std::size_t MaxMergeBufferLen = lengthByteBuffers(byteBuffers);

        // Schannel SSL won't do more than ~16536 bytes in one message.
        // So it can't be used above a HTTP frame filter.
        const std::size_t MaxSchannelLen = 16000;
        if ( mSspiType == St_Schannel )
        {
            MaxMergeBufferLen = MaxSchannelLen;
        }

        ByteBuffer mergeBuffer;
        if (byteBuffers.size() > 1)
        {
            // Check if we have small enough buffers to merge.
            //std::vector<ByteBuffer> mergeBufferList;
            mMergeBufferList.resize(0);
            sliceByteBuffers(mMergeBufferList, byteBuffers, 0, MaxMergeBufferLen);
            if (mMergeBufferList.size() > 1)
            {
                // Allocate merge buffer.
                ReallocBufferPtr mergeVecPtr = getObjectPool().getReallocBufferPtr();
                mergeVecPtr->resize( RCF_MIN(MaxMergeBufferLen, lengthByteBuffers(byteBuffers)) );

                // Copy to merge buffer.
                copyByteBuffers(mMergeBufferList, &(*mergeVecPtr)[0]);
                std::size_t mergeLength = lengthByteBuffers(mMergeBufferList);
                mergeVecPtr->resize(mergeLength);
                mergeBuffer = ByteBuffer(mergeVecPtr);

                mMergeBufferList.resize(0);
            }
        }

        // If we have a merge buffer, use that for the next write operation.
        if (mergeBuffer.getLength() > 0)
        {
            mWriteByteBufferOrig = mergeBuffer;
        }
        else
        {
            mWriteByteBufferOrig = byteBuffers.front();
        }
        
        if ( mSspiType == St_Schannel )
        {
            mWriteByteBufferOrig = ByteBuffer( 
                mWriteByteBufferOrig, 
                0, 
                RCF_MIN(mWriteByteBufferOrig.getLength(), MaxSchannelLen));
        }

        mPreState = Writing;
        handleEvent(WriteIssued);
    }

    void SspiFilter::onReadCompleted_(
        const ByteBuffer &byteBuffer)
    {
        if (mPreState == Reading && mBytesRequestedOrig == 0)
        {
            RCF_ASSERT(byteBuffer.isEmpty());
            mpPreFilter->onReadCompleted(ByteBuffer());
        }
        else
        {
            RCF_ASSERT(
                    byteBuffer.isEmpty()
                ||  mReadBuffer + mReadBufferPos == byteBuffer.getPtr());

            mReadBufferPos += byteBuffer.getLength();

            RCF_ASSERT(mReadBufferPos <= mReadBufferLen);

            if ( mSspiType == St_Schannel && mClientOrServer == Sr_Server && !mProtocolChecked && mReadBufferPos >= 1 )
            {
                // SSL 3.0 starts with \x16. SSL 2.0 starts with \x80.
                unsigned char firstByte = mReadBuffer[0];
                if ( firstByte != '\x16' && firstByte != (unsigned char) '\x80')
                {
                    Exception e(RcfError_NotSslHandshake);
                    std::string msg = e.getErrorMessage();

                    resizeWriteBuffer(msg.length()+1);
                    memcpy(mWriteBuffer, msg.c_str(), msg.length()+1);

                    RcfSession * pSession = getCurrentRcfSessionPtr();
                    if ( pSession )
                    {
                        NetworkSession& nwSession = pSession->getNetworkSession();
                        AsioNetworkSession& asioNwSession = static_cast<AsioNetworkSession&>(nwSession);
                        asioNwSession.setCloseAfterWrite();
                    }
                }
                mProtocolChecked = true;
            }

            const_cast<ByteBuffer &>(byteBuffer).clear();
            handleEvent(ReadCompleted);
        }
    }

    // Recursion limiter can only be used on synchronous filter stacks, and
    // avoids excessive recursion when reading or writing data in small pieces.
    // On asynchronous filter stacks, it would introduce a race condition by setting
    // filter state after invoking downstream async read/write operations.
    void SspiFilter::onReadCompleted(
        const ByteBuffer &byteBuffer)
    {
        if (mLimitRecursion)
        {
            applyRecursionLimiter(
                mRecursionStateRead,
                &SspiFilter::onReadCompleted_,
                *this,
                byteBuffer);
        }
        else
        {
            onReadCompleted_(byteBuffer);
        }
    }

    void SspiFilter::onWriteCompleted_(
        std::size_t bytesTransferred)
    {
        mByteBuffers.resize(0);
        mWriteBufferPos += bytesTransferred;
       
        RCF_ASSERT(mWriteBufferPos <= mWriteBufferLen);

        handleEvent(WriteCompleted);
    }

    void SspiFilter::onWriteCompleted(
        std::size_t bytesTransferred)
    {
        if (mLimitRecursion)
        {
            applyRecursionLimiter(
                mRecursionStateWrite,
                &SspiFilter::onWriteCompleted_,
                *this,
                bytesTransferred);
        }
        else
        {
            onWriteCompleted_(bytesTransferred);
        }
    }

    void SspiFilter::handleEvent(Event event)
    {
        RCF_ASSERT(
            event == ReadIssued || event == WriteIssued ||
            event == ReadCompleted || event == WriteCompleted)(event);

        mTempByteBuffer.clear();

        mEvent = event;
        if ((mEvent == ReadIssued || mEvent == WriteIssued) || completeBlock())
        {
            if (mContextState != AuthOkAck)
            {
                handleHandshakeEvent();
            }
            else
            {
                switch (mEvent)
                {
                case ReadIssued:

                    if (0 < mReadBufferPos && mReadBufferPos < mReadBufferLen)
                    {
                        // read from currently decrypted  block
                        std::size_t bytesAvail = mReadBufferLen - mReadBufferPos;

                        std::size_t bytesToRead =
                            RCF_MIN(bytesAvail, mBytesRequestedOrig);

                        if (mReadByteBufferOrig.getLength() > 0)
                        {
                            memcpy(
                                mReadByteBufferOrig.getPtr(),
                                mReadBuffer+mReadBufferPos,
                                bytesToRead);

                            mTempByteBuffer = ByteBuffer(
                                mReadByteBufferOrig,
                                0,
                                bytesToRead);
                        }
                        else
                        {
                            mTempByteBuffer = ByteBuffer(
                                mReadByteBuffer,
                                mReadBufferPos,
                                bytesToRead);
                        }
                       
                        mReadBufferPos += bytesToRead;
                        mReadByteBufferOrig = ByteBuffer();

                        mRecursionStateRead.clear();
                        mpPreFilter->onReadCompleted(mTempByteBuffer);
                    }
                    else if (mRemainingDataPos)
                    {
                        RCF_ASSERT(mSspiType == St_Schannel);
                        shiftReadBuffer();
                        handleEvent(ReadCompleted);
                    }
                    else
                    {
                        // read in a new block
                        resizeReadBuffer(mReadAheadChunkSize);
                        readBuffer();
                    }
                    break;

                case WriteIssued:

                    (mSspiType == St_Schannel) ?
                        encryptWriteBufferSchannel() :
                        encryptWriteBuffer();

                    writeBuffer();
                    break;

                case ReadCompleted:

                    {
                        bool ok = (mSspiType == St_Schannel) ?
                            decryptReadBufferSchannel() :
                            decryptReadBuffer();

                        if (ok)
                        {
                            handleEvent(ReadIssued);
                        }
                    }
                    
                    break;

                case WriteCompleted:

                    // mResumeUserIoAfterWrite is set if a handshake completes with us (the client) sending the last 
                    // block. In this case, we need to resume sending application data.
                    if (mResumeUserIoAfterWrite)
                    {
                        mResumeUserIoAfterWrite = false;
                        resumeUserIo();
                    }
                    else
                    {
                        std::size_t bytesTransferred =
                            mWriteByteBufferOrig.getLength();

                        mRecursionStateWrite.clear();
                        mWriteByteBufferOrig = ByteBuffer();
                        mpPreFilter->onWriteCompleted(bytesTransferred);
                    }
                   
                    break;

                default:
                    RCF_ASSERT_ALWAYS("");
                }
            }
        }
    }

    void SspiFilter::readBuffer()
    {
        RCF_ASSERT(mReadBufferPos <= mReadBufferLen);
        mPostState = Reading;
        mTempByteBuffer = ByteBuffer(mReadByteBuffer, mReadBufferPos);
        mpPostFilter->read(mTempByteBuffer, mReadBufferLen-mReadBufferPos);
    }

    void SspiFilter::writeBuffer()
    {
        RCF_ASSERT(mWriteBufferPos <= mWriteBufferLen);
        mPostState = Writing;
        mByteBuffers.resize(0);
        mByteBuffers.push_back( ByteBuffer(mWriteByteBuffer, mWriteBufferPos));
        mpPostFilter->write(mByteBuffers);
    }

    bool SspiFilter::completeReadBlock()
    {
        if ( mSspiType == St_Schannel )
        {
            return true;
        }

        RCF_ASSERT(mReadBufferPos <= mReadBufferLen);

        if (mReadBufferPos == mReadBufferLen && mReadBufferLen == 4)
        {
            // Got the 4 byte length field, now read the rest of the block.
            static_assert( sizeof(unsigned int) == 4, "Invalid data type size assumption." );
            static_assert( sizeof(DWORD) == 4, "Invalid data type size assumption." );

            unsigned int len = * (unsigned int *) mReadBuffer;
            bool integrity = (len & (1<<30)) ? true : false;
            bool encryption = (len & (1<<31)) ? true : false;
            len = len & ~(1<<30);
            len = len & ~(1<<31);

            // Check the length against the max message length.
            if (mMaxMessageLength && len > mMaxMessageLength)
            {
                int rcfError = (mClientOrServer == Sr_Server) ? 
                    RcfError_ServerMessageLength_Id :
                    RcfError_ClientMessageLength_Id;

                RCF_THROW(Exception(ErrorMsg(rcfError)));
            }

            * (unsigned int *) mReadBuffer = len;

            RCF_ASSERT( !(integrity && encryption) );

            if ( mClientOrServer == Sr_Server )
            {
                if (integrity)
                {
                    mQop = Smp_Integrity;
                }
                else if (encryption)
                {
                    mQop = Smp_Encryption;
                }
                else
                {
                    mQop = Smp_None;
                }
            }

            resizeReadBuffer(4+len);
            mReadBufferPos = 4;
            readBuffer();
            return false;
        }

        return (mReadBufferPos < mReadBufferLen) ?
            readBuffer(), false :
            true;
    }

    bool SspiFilter::completeWriteBlock()
    {
        RCF_ASSERT(mWriteBufferPos <= mWriteBufferLen);

        return (mWriteBufferPos < mWriteBufferLen) ?
            writeBuffer(), false :
            true;
    }

    bool SspiFilter::completeBlock()
    {
        // check to see if a whole block was read or written
        // if not, issue another read or write
        RCF_ASSERT(
            mPostState == Reading || mPostState == Writing )
            (mPostState);

        return
            mPostState == Reading ?
                completeReadBlock() :
                completeWriteBlock();
    }

    void SspiFilter::resizeReadBuffer(std::size_t newSize)
    {
        mTempByteBuffer.clear();
        mReadByteBuffer.clear();
        if (!mReadBufferVectorPtr)
        {
            mReadBufferVectorPtr = getObjectPool().getReallocBufferPtr();
        }

        std::size_t newSize_ = newSize == 0 ? 1 : newSize;
        mReadBufferVectorPtr->resize(newSize_);
        mReadByteBuffer = ByteBuffer(mReadBufferVectorPtr);
        mReadBuffer = mReadByteBuffer.getPtr();
        mReadBufferPos = 0;
        mReadBufferLen = mReadByteBuffer.getLength();
        mReadBufferLen = (mReadBufferLen == 1) ? 0 : mReadBufferLen;

        RCF_ASSERT(mReadBufferLen == newSize);
    }

    void SspiFilter::resizeWriteBuffer(std::size_t newSize)
    {
        mWriteByteBuffer.clear();
        if (!mWriteBufferVectorPtr)
        {
            mWriteBufferVectorPtr = getObjectPool().getReallocBufferPtr();
        }

        std::size_t newSize_ = newSize == 0 ? 1 : newSize;
        mWriteBufferVectorPtr->resize(newSize_);
        mWriteByteBuffer = ByteBuffer(mWriteBufferVectorPtr);
        mWriteBuffer = mWriteByteBuffer.getPtr();
        mWriteBufferPos = 0;
        mWriteBufferLen = mWriteByteBuffer.getLength();
        mWriteBufferLen = mWriteBufferLen == 1 ? 0 : mWriteBufferLen;
        RCF_ASSERT(mWriteBufferLen == newSize);
    }

    void SspiFilter::shiftReadBuffer(bool shiftEntireBuffer)
    {
        RCF_ASSERT(     0 < mRemainingDataPos 
                    &&  mRemainingDataPos < mReadBufferVectorPtr->size());

        // Do we shift the entire buffer, or only up to mReadBufferPos.
        if ( shiftEntireBuffer )
        {
            mReadBufferPos = mReadBufferVectorPtr->size();
        }
        std::size_t bytesToMove = mReadBufferPos - mRemainingDataPos;
        char * pchFrom = mReadBuffer + mRemainingDataPos;
        char * pchTo = mReadBuffer;
        memmove(pchTo, pchFrom, bytesToMove);
        mReadBufferPos = bytesToMove;
        mRemainingDataPos = 0;
        trimReadBuffer();
    }

    void SspiFilter::trimReadBuffer()
    {
        mReadBufferVectorPtr->resize(mReadBufferPos);
        mReadByteBuffer = ByteBuffer(mReadBufferVectorPtr);
        mReadBuffer = mReadByteBuffer.getPtr();
        mReadBufferLen = mReadByteBuffer.getLength();
        mReadBufferLen = (mReadBufferLen == 1) ? 0 : mReadBufferLen;
    }

    void SspiFilter::encryptWriteBuffer()
    {
        // encrypt the pre buffer to the write buffer

        RCF_ASSERT(mContextState == AuthOkAck);

        if (mQop == Smp_Integrity)
        {
            SecPkgContext_Sizes sizes;
            getSft()->QueryContextAttributes(
                &mContext,
                SECPKG_ATTR_SIZES,
                &sizes);

            DWORD cbPacketLength    = 4;
            DWORD cbMsgLength       = 4;
            DWORD cbMsg             = static_cast<DWORD>(mWriteByteBufferOrig.getLength());
            DWORD cbSignature       = sizes.cbMaxSignature;
            DWORD cbPacket          = cbMsgLength + cbMsg + cbSignature;

            resizeWriteBuffer(cbPacketLength + cbPacket);
            memcpy(mWriteBuffer+cbPacketLength, &cbMsg, cbMsgLength);
            memcpy(
                mWriteBuffer+cbPacketLength+cbMsgLength,
                mWriteByteBufferOrig.getPtr(),
                mWriteByteBufferOrig.getLength());

            char *pMsg              = &mWriteBuffer[4];
            SecBuffer rgsb[2]       = {0,0};
            rgsb[0].cbBuffer        = cbMsg;
            rgsb[0].BufferType      = SECBUFFER_DATA;
            rgsb[0].pvBuffer        = pMsg + cbMsgLength;
            rgsb[1].cbBuffer        = cbSignature;
            rgsb[1].BufferType      = SECBUFFER_TOKEN;
            rgsb[1].pvBuffer        = pMsg + cbMsgLength + cbMsg;
            SecBufferDesc sbd       = {0};
            sbd.ulVersion           = SECBUFFER_VERSION;
            sbd.cBuffers            = sizeof(rgsb)/sizeof(*rgsb);
            sbd.pBuffers            = rgsb;

            SECURITY_STATUS status = getSft()->MakeSignature(
                &mContext,
                0,
                &sbd,
                0);

            RCF_VERIFY(
                status == SEC_E_OK,
                Exception(RcfError_SspiEncrypt, "MakeSignature()", osError(status)));

            cbSignature                 = rgsb[1].cbBuffer;
            cbPacket                    = cbMsgLength + cbMsg + cbSignature;
            resizeWriteBuffer(cbPacketLength + cbPacket);
            DWORD encodedLength         = cbPacket;
            RCF_ASSERT(encodedLength < (1<<30));
            encodedLength               = encodedLength | (1<<30);
            * (DWORD*) mWriteBuffer     = encodedLength;
        }
        else if (mQop == Smp_Encryption)
        {
            SecPkgContext_Sizes sizes;
            getSft()->QueryContextAttributes(
                &mContext,
                SECPKG_ATTR_SIZES,
                &sizes);

            DWORD cbPacketLength    = 4;
            DWORD cbMsgLength       = 4;
            DWORD cbMsg             = static_cast<DWORD>(mWriteByteBufferOrig.getLength());
            DWORD cbTrailer         = sizes.cbSecurityTrailer;
            DWORD cbPacket          = cbMsgLength + cbMsg + cbTrailer;

            resizeWriteBuffer(cbPacketLength + cbPacket);
            memcpy(mWriteBuffer+cbPacketLength, &cbMsg, cbMsgLength);
            memcpy(
                mWriteBuffer+cbPacketLength+cbMsgLength,
                mWriteByteBufferOrig.getPtr(),
                mWriteByteBufferOrig.getLength());

            BYTE *pEncryptedMsg     = ((BYTE *) mWriteBuffer) + 4;
            SecBuffer rgsb[2]       = {0,0};
            rgsb[0].cbBuffer        = cbMsg;
            rgsb[0].BufferType      = SECBUFFER_DATA;
            rgsb[0].pvBuffer        = pEncryptedMsg + cbMsgLength;
            rgsb[1].cbBuffer        = cbTrailer;
            rgsb[1].BufferType      = SECBUFFER_TOKEN;
            rgsb[1].pvBuffer        = pEncryptedMsg + cbMsgLength + cbMsg;
            SecBufferDesc sbd       = {0};
            sbd.ulVersion           = SECBUFFER_VERSION;
            sbd.cBuffers            = sizeof(rgsb)/sizeof(*rgsb);
            sbd.pBuffers            = rgsb;

            SECURITY_STATUS status = getSft()->EncryptMessage(
                &mContext,
                0,
                &sbd,
                0);

            RCF_VERIFY(
                status == SEC_E_OK,
                Exception(RcfError_SspiEncrypt, "EncryptMessage()", osError(status)));

            cbTrailer               = rgsb[1].cbBuffer;
            cbPacket                = cbMsgLength + cbMsg + cbTrailer;
            resizeWriteBuffer(cbPacketLength + cbPacket);
            DWORD encodedLength     = cbPacket;
            RCF_ASSERT(encodedLength < (1<<30));
            encodedLength           = encodedLength | (1<<31);
            * (DWORD*) mWriteBuffer = encodedLength;
        }
        else
        {
             RCF_ASSERT(mQop == None);
            RCF_ASSERT(mWriteByteBufferOrig.getLength() < std::size_t(1) << 31 );

            resizeWriteBuffer(mWriteByteBufferOrig.getLength()+4);
            memcpy(
                mWriteBuffer+4,
                mWriteByteBufferOrig.getPtr(),
                mWriteByteBufferOrig.getLength());

            DWORD dw = static_cast<DWORD>(mWriteByteBufferOrig.getLength());
            *(DWORD*) mWriteBuffer  = dw;
        }

    }

    bool SspiFilter::decryptReadBuffer()
    {
        // decrypt read buffer in place

        RCF_ASSERT(mContextState == AuthOkAck);

        if (mQop == Smp_Integrity)
        {
            BYTE *pMsg              = ((BYTE *) mReadBuffer) + 4;
            DWORD cbPacketLength    = 4;
            DWORD cbPacket          = *(DWORD*) mReadBuffer;

            RCF_VERIFY(
                cbPacket <= mReadBufferLen, 
                Exception(RcfError_SspiLengthField, cbPacket, mReadBufferLen));

            DWORD cbMsgLength       = 4;
            DWORD cbMsg             = *(DWORD*) pMsg;

            RCF_VERIFY(
                cbMsg <= mReadBufferLen, 
                Exception(RcfError_SspiLengthField, cbMsg, mReadBufferLen));

            DWORD cbSignature       = cbPacket - cbMsgLength - cbMsg;
            SecBuffer rgsb[2]       = {0,0};
            rgsb[0].cbBuffer        = cbMsg;
            rgsb[0].BufferType      = SECBUFFER_DATA;
            rgsb[0].pvBuffer        = pMsg + cbMsgLength;
            rgsb[1].cbBuffer        = cbSignature;
            rgsb[1].BufferType      = SECBUFFER_TOKEN;
            rgsb[1].pvBuffer        = pMsg + cbMsgLength + cbMsg;
            SecBufferDesc sbd       = {0};
            sbd.ulVersion           = SECBUFFER_VERSION;
            sbd.cBuffers            = sizeof(rgsb)/sizeof(*rgsb);
            sbd.pBuffers            = rgsb;
            ULONG qop               = 0;
            SECURITY_STATUS status  = getSft()->VerifySignature(
                &mContext,
                &sbd,
                0,
                &qop);

            RCF_VERIFY(
                status == SEC_E_OK,
                Exception(RcfError_SspiDecrypt, "VerifySignature()", osError(status)));

            resizeReadBuffer(cbPacketLength + cbMsgLength + cbMsg);
            mReadBufferPos          = cbPacketLength + cbMsgLength;
        }
        else if (mQop == Smp_Encryption)
        {
            BYTE *pMsg              = ((BYTE *) mReadBuffer) + 4;
            DWORD cbPacketLength    = 4;
            DWORD cbPacket          = *(DWORD*)mReadBuffer;

            RCF_VERIFY(
                cbPacket <= mReadBufferLen, 
                Exception(RcfError_SspiLengthField, cbPacket, mReadBufferLen));

            DWORD cbMsgLength       = 4;
            DWORD cbMsg             = *(DWORD*) pMsg;

            RCF_VERIFY(
                cbMsg <= mReadBufferLen, 
                Exception(RcfError_SspiLengthField, cbMsg, mReadBufferLen));

            DWORD cbTrailer         = (cbPacket - cbMsgLength) - cbMsg;
            SecBuffer rgsb[2]       = {0,0};
            rgsb[0].cbBuffer        = cbMsg;
            rgsb[0].BufferType      = SECBUFFER_DATA;
            rgsb[0].pvBuffer        = pMsg + cbMsgLength;
            rgsb[1].cbBuffer        = cbTrailer;
            rgsb[1].BufferType      = SECBUFFER_TOKEN;
            rgsb[1].pvBuffer        = pMsg + cbMsgLength + cbMsg;
            SecBufferDesc sbd       = {0};
            sbd.ulVersion           = SECBUFFER_VERSION;
            sbd.cBuffers            = sizeof(rgsb)/sizeof(*rgsb);
            sbd.pBuffers            = rgsb;
            ULONG qop               = 0;

            SECURITY_STATUS status  = getSft()->DecryptMessage(
                &mContext,
                &sbd,
                0,
                &qop);

            RCF_VERIFY(
                status == SEC_E_OK,
                Exception(RcfError_SspiDecrypt, "DecryptMessage()", osError(status)));

            resizeReadBuffer(cbPacketLength + cbMsgLength + cbMsg);
            mReadBufferPos          = cbPacketLength + cbMsgLength;
        }
        else
        {
            RCF_ASSERT(mQop == None);
            mReadBufferPos = 4;
        }

        return true;
    }

    void SspiFilter::resumeUserIo()
    {
        RCF_ASSERT( mPreState == Reading || mPreState == Writing )(mPreState);
        handleEvent( mPreState == Reading ? ReadIssued : WriteIssued );
    }

    SspiImpersonator::SspiImpersonator(SspiFilterPtr sspiFilterPtr) :
        mSspiFilterPtr(sspiFilterPtr)
    {
    }

    SspiImpersonator::SspiImpersonator(RcfSession & session)
    {
        std::vector<FilterPtr> filters;
        session.getTransportFilters(filters);
        for (std::size_t i=0; i<filters.size(); ++i)
        {
            int filterId = filters[i]->getFilterId();

            if (    filterId == RcfFilter_SspiNtlm 
                ||  filterId == RcfFilter_SspiKerberos 
                ||  filterId == RcfFilter_SspiNegotiate)
            {
                mSspiFilterPtr = std::static_pointer_cast<SspiFilter>(filters[i]);
            }
        }

        if (!mSspiFilterPtr)
        {
            RCF_THROW( Exception(RcfError_SspiImpersonateNoSspi) );
        }
    }

    SspiImpersonator::~SspiImpersonator()
    {
        RCF_DTOR_BEGIN
            revertToSelf();
        RCF_DTOR_END
    }

    bool SspiImpersonator::impersonate()
    {
        if (mSspiFilterPtr)
        {
            SECURITY_STATUS status = 
                getSft()->ImpersonateSecurityContext(&mSspiFilterPtr->mContext);

            RCF_VERIFY(
                status == SEC_E_OK,
                Exception(RcfError_SspiImpersonation, "ImpersonateSecurityContext()", osError(status)));

            return true;
        }
        else
        {
            return false;
        }
    }

    void SspiImpersonator::revertToSelf() const
    {
        if (mSspiFilterPtr)
        {
            SECURITY_STATUS status = 
                getSft()->RevertSecurityContext(&mSspiFilterPtr->mContext);

            RCF_VERIFY(
                status == SEC_E_OK,
                Exception(RcfError_SspiImpersonation, "RevertSecurityContext()", osError(status)));
        }
    }
   
    bool SspiServerFilter::doHandshake()
    {
        // Use the block in the read buffer to proceed through the handshake procedure.

        RCF_ASSERT(mCredentialsPtr);

        DWORD cbPacket          = mCredentialsPtr->mpPkgInfo->cbMaxToken;
        DWORD cbPacketLength    = 4;

        std::vector<char> vec(cbPacketLength + cbPacket);

        BYTE *pPacket           = (BYTE*) &vec[0];
        SecBuffer ob            = {0};
        ob.BufferType           = SECBUFFER_TOKEN;
        ob.cbBuffer             = cbPacket;
        ob.pvBuffer             = pPacket+cbPacketLength;
        SecBufferDesc obd       = {0};
        obd.cBuffers            = 1;
        obd.ulVersion           = SECBUFFER_VERSION;
        obd.pBuffers            = &ob;

        RCF_ASSERT(
            mReadBufferLen == 0 || mReadBufferLen > 4)
            (mReadBufferLen);

        RCF_ASSERT(mClientOrServer == Sr_Client || (mClientOrServer == Sr_Server && mReadBufferLen > 4));

        SecBufferDesc ibd       = {0};
        SecBuffer ib            = {0};
        if (mReadBufferLen > 4)
        {
            ib.BufferType       = SECBUFFER_TOKEN;
            ib.cbBuffer         = *(DWORD *)mReadBuffer;
            ib.pvBuffer         = mReadBuffer+cbPacketLength;
            ibd.cBuffers        = 1;
            ibd.ulVersion       = SECBUFFER_VERSION;
            ibd.pBuffers        = &ib;
        }

        DWORD   CtxtAttr        = 0;
        TimeStamp Expiration    = {0};
        SECURITY_STATUS status  = getSft()->AcceptSecurityContext(
            &mCredentialsPtr->mCredentials,
            mHaveContext ? &mContext : NULL,
            &ibd,
            mContextRequirements,
            SECURITY_NATIVE_DREP,
            &mContext,
            &obd,
            &CtxtAttr,
            &Expiration);

        switch (status)
        {
        case SEC_E_OK:
        case SEC_I_CONTINUE_NEEDED:
        case SEC_I_COMPLETE_NEEDED:
        case SEC_I_COMPLETE_AND_CONTINUE:
        case SEC_E_INCOMPLETE_MESSAGE:
            mHaveContext = true;
            break;
        default:
            break;
        }

        cbPacket = ob.cbBuffer;

        // We only support NTLM, Kerberos and Negotiate SSP's, so there's never
        // a need to call CompleteAuthToken()
        RCF_ASSERT(
            status != SEC_I_COMPLETE_AND_CONTINUE &&
            status != SEC_I_COMPLETE_NEEDED)
            (status);

        if (status == SEC_I_CONTINUE_NEEDED)
        {
            // authorization ok so far, copy outbound data to write buffer
            mContextState = AuthContinue;
            *(DWORD *) pPacket = cbPacket;
            resizeWriteBuffer(cbPacketLength + cbPacket);
            memcpy(mWriteBuffer, pPacket, cbPacketLength + cbPacket);
        }
        else if (status == SEC_E_OK)
        {
            // authorization ok, send a special block of our own to notify client
            mContextState = AuthOk;
            if (cbPacket > 0)
            {
                *(DWORD *) pPacket = cbPacket;
                resizeWriteBuffer(cbPacketLength + cbPacket);
                memcpy(mWriteBuffer, pPacket, cbPacketLength + cbPacket);
            }
            else
            {
                resizeWriteBuffer(4+4+4);
                *(DWORD*) mWriteBuffer = 8;
                *(DWORD*)(mWriteBuffer + 4) = RcfError_Ok_Id;
                *(DWORD*) (mWriteBuffer+8) = 0;
            }

            // Impersonate client to get their username.
            SspiFilterPtr thisPtr(this, NullDeleter());
            SspiImpersonator impersonator(thisPtr);
            impersonator.impersonate();
            tstring domainAndUsername = RCF::getMyDomain() + RCF_T("\\") + RCF::getMyUserName();
            getCurrentRcfSession().mClientUsername = domainAndUsername;

            RCF_LOG_2() << "SSPI login succeeded. User name: " << domainAndUsername;
        }
        else
        {
            // authorization failed, send a special block of our own to notify client

            RCF_LOG_2() << "SSPI login failed. Error: " + RCF::osError(status);

            mContextState = AuthFailed;
            resizeWriteBuffer(4+4+4);
            *(DWORD*) mWriteBuffer = 8;
            *(DWORD*) (mWriteBuffer+4) = RcfError_SspiAuthFailServer_Id;
            *(DWORD*) (mWriteBuffer+8) = status;

            RcfSession * pSession = getCurrentRcfSessionPtr();
            if ( pSession )
            {
                NetworkSession& nwSession = pSession->getNetworkSession();
                AsioNetworkSession& asioNwSession = static_cast<AsioNetworkSession&>(nwSession);
                asioNwSession.setCloseAfterWrite();
            }
        }

        return true;
    }

    void SspiServerFilter::handleHandshakeEvent()
    {
        // take another step through the handshake process

        switch (mEvent)
        {
        case ReadIssued:
        case WriteIssued:

            // read first block from client
            RCF_ASSERT(mEvent == ReadIssued);
            resizeReadBuffer(mReadAheadChunkSize);
            readBuffer();
            break;

        case ReadCompleted:
           
            // process inbound block and write outbound block
            {
                bool written = (mSspiType == St_Schannel) ? 
                    doHandshakeSchannel() : 
                    doHandshake();

                if (written)
                {
                    writeBuffer();
                }
            }
            break;

        case WriteCompleted:

            switch (mContextState)
            {
            case AuthOk:
                mContextState = AuthOkAck;
                resumeUserIo();
                break;

            case AuthFailed:
                {
                    Exception e(RcfError_SspiAuthFailServer);
                    RCF_THROW(e);
                }
                break;

            default:
                resizeReadBuffer(mReadAheadChunkSize);
                readBuffer();
            }
            break;
        default:
            RCF_ASSERT_ALWAYS("");
        }
    }

    bool SspiClientFilter::doHandshake()
    {
        // Use the block in the read buffer to proceed through the handshake procedure.

        if ( !mCredentialsPtr )
        {
            createClientCredentials();
        }

        RCF_ASSERT(mCredentialsPtr);

        if (mContextState == AuthOk)
        {
            if (mReadBufferLen == 12)
            {
                DWORD rcfErr = *(DWORD*) &mReadBuffer[4];
                DWORD osErr = *(DWORD*) &mReadBuffer[8];
                if (rcfErr == RcfError_Ok_Id )
                {
                    mContextState = AuthOkAck;

                    if (mCredentialsPtr->mUserName.size() > 0)
                    {
                        RCF_LOG_2() << "SSPI login succeeded, using explicit credentials for user: " << mCredentialsPtr->mUserName << " . SSPI message protection level: " << (int)mQop;
                    }
                    else
                    {
                        auto currentUser = getMyDomain() + _T("\\") + getMyUserName();
                        RCF_LOG_2() << "SSPI login succeeded, using implicit credentials for user: " << currentUser << " . SSPI message protection level: " << (int)mQop;
                    }
                    
                    resumeUserIo();
                    return false;
                }
                else
                {
                    RCF_UNUSED_VARIABLE(osErr);
                    RCF_THROW(RemoteException(ErrorMsg(rcfErr)));
                }
            }
            else
            {
                Exception e(RcfError_SspiAuthFailServer);
                RCF_THROW(e);
            }
        }
       
        DWORD cbPacketLength    = 4;
        DWORD cbPacket          = mCredentialsPtr->mpPkgInfo->cbMaxToken;
        std::vector<char> vec(cbPacket + cbPacketLength);

        BYTE *pPacket           = (BYTE*) &vec[0];
        SecBuffer ob            = {0};
        ob.BufferType           = SECBUFFER_TOKEN;
        ob.cbBuffer             = cbPacket;
        ob.pvBuffer             = pPacket + cbPacketLength;
        SecBufferDesc obd       = {0};
        obd.cBuffers            = 1;
        obd.ulVersion           = SECBUFFER_VERSION;
        obd.pBuffers            = &ob;

        RCF_ASSERT(mReadBufferLen == 0 || mReadBufferLen > 4);

        RCF_ASSERT(mClientOrServer == Sr_Client || (mClientOrServer == Sr_Server && mReadBufferLen > 4));

        SecBuffer ib            = {0};
        SecBufferDesc ibd       = {0};

        if (mReadBufferLen > 4)
        {
            ib.BufferType       = SECBUFFER_TOKEN;
            ib.cbBuffer         = *(DWORD *) mReadBuffer;
            ib.pvBuffer         = mReadBuffer + cbPacketLength;
            ibd.cBuffers        = 1;
            ibd.ulVersion       = SECBUFFER_VERSION;
            ibd.pBuffers        = &ib;
        }

        tstring strTarget = mpClientStub->getKerberosSpn();
        const TCHAR *target = strTarget.empty() ? RCF_T("") : strTarget.c_str();

        DWORD CtxtAttr          = 0;
        TimeStamp Expiration    = {0};
        ULONG CtxtReq =  mContextRequirements;

        SECURITY_STATUS status  = getSft()->InitializeSecurityContext(
            &mCredentialsPtr->mCredentials,
            mHaveContext ? &mContext : NULL,
            (TCHAR *) target,
            CtxtReq,
            0,
            SECURITY_NATIVE_DREP,
            (mHaveContext && mReadBufferLen > 4) ? &ibd : NULL,
            0,
            &mContext,
            &obd,
            &CtxtAttr,
            &Expiration);

        switch (status)
        {
        case SEC_E_OK:
        case SEC_I_CONTINUE_NEEDED:
        case SEC_I_COMPLETE_NEEDED:
        case SEC_I_COMPLETE_AND_CONTINUE:
        case SEC_E_INCOMPLETE_MESSAGE:
        case SEC_I_INCOMPLETE_CREDENTIALS:
            mHaveContext = true;
            break;
        default:
            break;
        }

        RCF_ASSERT(
            status != SEC_I_COMPLETE_NEEDED &&
            status != SEC_I_COMPLETE_AND_CONTINUE)
            (status);
       
        cbPacket                = ob.cbBuffer;
        if (cbPacket > 0)
        {
            *(DWORD *)pPacket   = cbPacket;
            mContextState       =
                (status == SEC_E_OK) ?
                    AuthOk :
                    (status == SEC_I_CONTINUE_NEEDED) ?
                        AuthContinue :
                        AuthFailed;

            RCF_VERIFY(
                mContextState != AuthFailed,
                Exception(RcfError_SspiAuthFailClient, osError(status)));

            resizeWriteBuffer(cbPacketLength + cbPacket);
            memcpy(mWriteBuffer, pPacket, cbPacketLength + cbPacket);

            return true;
        }
        else
        {
            mContextState = AuthOkAck;
            resumeUserIo();
            return false;
        }

    }

    void SspiClientFilter::handleHandshakeEvent()
    {
        // take another step through the handshake process

        switch (mEvent)
        {
        case ReadIssued:
        case WriteIssued:
           
            // create first block to send to server
            //resizeReadBuffer(0);
            (mSspiType == St_Schannel) ?
                doHandshakeSchannel() : 
                doHandshake();

            writeBuffer();
            break;

        case ReadCompleted:

            // process a block, and send any emitted output block
            {
            bool written = (mSspiType == St_Schannel) ?
                doHandshakeSchannel() : 
                doHandshake();

            if (written)
            {
                writeBuffer();
            }
            }
            break;

        case WriteCompleted:

            // issue a read for the next block from the server
            resizeReadBuffer(mReadAheadChunkSize);
            readBuffer();
            break;

        default:
            RCF_ASSERT_ALWAYS("");
        }
    }



    //**************************************************************************
    // Server filters.

    SspiServerFilter::SspiServerFilter(
        SspiCredentialsPtr credentialsPtr,
        SspiType sspiType) :
            SspiFilter(credentialsPtr, NULL, SspiMessageProtection::Smp_None, 0, Sr_Server, sspiType)
    {
        RcfSession * pRcfSession = RCF::getTlsRcfSessionPtr();
        if (pRcfSession)
        {
            ServerTransport & serverTransport = 
                pRcfSession->getNetworkSession().getServerTransport();

            mMaxMessageLength = serverTransport.getMaxIncomingMessageLength();
        }
    }

    // NTLM
    NtlmServerFilter::NtlmServerFilter(SspiCredentialsPtr credentialsPtr) :
        SspiServerFilter(credentialsPtr, St_WindowsAuth)
    {}

    int NtlmServerFilter::getFilterId() const
    {
        return RcfFilter_SspiNtlm;
    }

    // Kerberos
    KerberosServerFilter::KerberosServerFilter(SspiCredentialsPtr credentialsPtr) :
        SspiServerFilter(credentialsPtr, St_WindowsAuth)
    {}

    int KerberosServerFilter::getFilterId() const
    {
        return RcfFilter_SspiKerberos;
    }

    // Negotiate
    NegotiateServerFilter::NegotiateServerFilter(SspiCredentialsPtr credentialsPtr) :
            SspiServerFilter(credentialsPtr, St_WindowsAuth)
    {}

    int NegotiateServerFilter::getFilterId() const
    {
        return RcfFilter_SspiNegotiate;
    }

    // Schannel
    int SchannelServerFilter::getFilterId() const
    {
        return RcfFilter_SspiSchannel;
    }

    //**************************************************************************
    // Filter factories

    // NTLM
    NtlmFilterFactory::NtlmFilterFactory()
    {
    }

    FilterPtr NtlmFilterFactory::createFilter(RcfServer &)
    {
        {
            Lock lock(mCredentialsMutex);
            if ( !mCredentialsPtr )
            {
                mCredentialsPtr.reset(new SspiCredentials(Sr_Server, RCF_T("NTLM"), RCF_T("")));
            }
        }

        return FilterPtr( new NtlmServerFilter(mCredentialsPtr) );
    }

    int NtlmFilterFactory::getFilterId()
    {
        return RcfFilter_SspiNtlm;
    }

    // Kerberos

    KerberosFilterFactory::KerberosFilterFactory()
    {
    }

    FilterPtr KerberosFilterFactory::createFilter(RcfServer &)
    {
        {
            Lock lock(mCredentialsMutex);
            if ( !mCredentialsPtr )
            {
                mCredentialsPtr.reset(new SspiCredentials(Sr_Server, RCF_T("Kerberos"), RCF_T("")));
            }
        }

        return FilterPtr( new KerberosServerFilter(mCredentialsPtr) );
    }

    int KerberosFilterFactory::getFilterId()
    {
        return RcfFilter_SspiKerberos;
    }

    // Negotiate
    NegotiateFilterFactory::NegotiateFilterFactory(
        const tstring &packageList) :
            mPackageList(packageList)
    {
    }

    FilterPtr NegotiateFilterFactory::createFilter(RcfServer &)
    {
        {
            Lock lock(mCredentialsMutex);
            if ( !mCredentialsPtr )
            {
                mCredentialsPtr.reset(new SspiCredentials(Sr_Server, RCF_T("Negotiate"), mPackageList));
            }
        }
        return FilterPtr( new NegotiateServerFilter(mCredentialsPtr) );
    }

    int NegotiateFilterFactory::getFilterId()
    {
        return RcfFilter_SspiNegotiate;
    }

    // Schannel
    int SchannelFilterFactory::getFilterId()
    {
        return RcfFilter_SspiSchannel;
    }

    //**************************************************************************
    // Client filters

    // NTLM
    NtlmClientFilter::NtlmClientFilter(
        ClientStub *            pClientStub,
        SspiMessageProtection   qop,
        ULONG                   contextRequirements) :
            SspiClientFilter(
                SspiCredentialsPtr(),
                pClientStub,
                qop, 
                contextRequirements,
                St_WindowsAuth,
                RCF_T("NTLM"), 
                RCF_T(""))
    {}

    int NtlmClientFilter::getFilterId() const
    {
        return RcfFilter_SspiNtlm;
    }

    // Kerberos
    KerberosClientFilter::KerberosClientFilter(
        ClientStub *            pClientStub,
        SspiMessageProtection   qop,
        ULONG                   contextRequirements) :
            SspiClientFilter(
                SspiCredentialsPtr(),
                pClientStub,
                qop, 
                contextRequirements,
                St_WindowsAuth,
                RCF_T("Kerberos"), 
                RCF_T(""))
    {}

    int KerberosClientFilter::getFilterId() const
    {
        return RcfFilter_SspiKerberos;
    }

    // Negotiate
    NegotiateClientFilter::NegotiateClientFilter(
        ClientStub *            pClientStub,
        SspiMessageProtection   qop,
        ULONG                   contextRequirements) :
            SspiClientFilter(
                SspiCredentialsPtr(),
                pClientStub,
                qop, 
                contextRequirements,
                St_WindowsAuth,
                RCF_T("Negotiate"), 
                RCF_T("Kerberos,NTLM"))
    {}

    int NegotiateClientFilter::getFilterId() const
    {
        return RcfFilter_SspiNegotiate;
    }

    // Schannel
    int SchannelClientFilter::getFilterId() const
    {
        return RcfFilter_SspiSchannel;
    }

    //************************************************************************

    // Global init and deinit.

    HINSTANCE               ghProvider          = NULL;      // provider dll's instance
    PSecurityFunctionTable  gpSecurityInterface = NULL;      // security interface table

    PSecurityFunctionTable getSft()
    {
        return gpSecurityInterface;
    }

    void SspiInitialize()
    {
        // load the provider dll
        ghProvider = LoadLibrary ( RCF_T("security.dll") );
        if (ghProvider == NULL)
        {
            int err = GetLastError();
            Exception e(RcfError_SspiInit, "Unable to load security.dll.", osError(err));
            RCF_THROW(e);
        }

        INIT_SECURITY_INTERFACE InitSecurityInterface;

        InitSecurityInterface = reinterpret_cast<INIT_SECURITY_INTERFACE> (
            GetProcAddress(ghProvider, INIT_SEC_INTERFACE_NAME));

        if (InitSecurityInterface == NULL)
        {
            int err = GetLastError();
            Exception e(RcfError_SspiInit, "Unable to load function \"InitSecurityInterface\".", osError(err));
            RCF_THROW(e);
        }

        gpSecurityInterface = InitSecurityInterface();
        if (gpSecurityInterface == NULL)
        {
            int err = GetLastError();
            Exception e(RcfError_SspiInit, "InitSecurityInterface() failed.", err);
            RCF_THROW(e);
        }
    }

    void SspiUninitialize()
    {
        FreeLibrary (ghProvider);
        ghProvider = NULL;
        gpSecurityInterface = NULL;
    }

} // namespace RCF
