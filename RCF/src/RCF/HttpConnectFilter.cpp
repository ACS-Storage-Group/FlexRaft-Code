
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

#include <RCF/HttpConnectFilter.hpp>

#include <RCF/ClientStub.hpp>
#include <RCF/Exception.hpp>
#include <RCF/HttpFrameFilter.hpp>
#include <RCF/ThreadLocalData.hpp>
#include <RCF/Tools.hpp>
#include <RCF/Base64.hpp>

#if RCF_FEATURE_SSPI==0

namespace RCF {

    // A simple wrapper around the SSPI NTLM filter.
    class NtlmWrapper : public Filter
    {
    public:
        NtlmWrapper()
        {
            Exception e(RcfError_NotSupportedInThisBuild, "Windows authentication");
            RCF_THROW(e);
        }

        void onPacketReceived(const std::string& packetReceived)
        {
            Exception e(RcfError_NotSupportedInThisBuild, "Windows authentication");
            RCF_THROW(e);
        }

        std::string getPacketToSend()
        {
            Exception e(RcfError_NotSupportedInThisBuild, "Windows authentication");
            RCF_THROW(e);
            return "";
        }

    private:

        void            read(const ByteBuffer &byteBuffer, std::size_t bytesRequested) {}
        void            write(const std::vector<ByteBuffer> &byteBuffers) {}
        void            onReadCompleted(const ByteBuffer &byteBuffer) {}
        void            onWriteCompleted(std::size_t bytesTransferred) {}

        int             getFilterId() const { return RcfFilter_Unknown; }
    };

} // namespace RCF

#else

#include <RCF/SspiFilter.hpp>

namespace RCF {
    // A simple wrapper around the SSPI NTLM filter.
    class NtlmWrapper : public Filter
    {
    public:
        NtlmWrapper();

        void            onPacketReceived(const std::string& packetReceived);
        std::string     getPacketToSend();

    private:

        // Filter functions are called by the NTLM filter.
        void            read(const ByteBuffer &byteBuffer, std::size_t bytesRequested);
        void            write(const std::vector<ByteBuffer> &byteBuffers);
        void            onReadCompleted(const ByteBuffer &byteBuffer);
        void            onWriteCompleted(std::size_t bytesTransferred);

        int             getFilterId() const;

        std::string             mPacketToSend;
        std::string             mPacketReceived;
        std::size_t             mPacketReceivedPos;
        std::unique_ptr<Filter>   mNtlmFilterPtr;

    };

    // NTLM filter will automatically pick up credentials from ClientStub.
    NtlmWrapper::NtlmWrapper() : 
        mNtlmFilterPtr(new NtlmFilter(getTlsClientStubPtr())), mPacketReceivedPos(0)
    {
        mNtlmFilterPtr->setPostFilter(*this);

        // Issue a write, to kick off the NTLM authentication sequence.
        std::vector<ByteBuffer> buffers;
        buffers.push_back(ByteBuffer(std::string("Dummy")));
        mNtlmFilterPtr->write(buffers);
    }

    void NtlmWrapper::onPacketReceived(const std::string& packetReceived)
    {
        mPacketReceived = packetReceived;
        
        // Insert the length field that the filter is expecting to see.
        DWORD packetLen = (DWORD) packetReceived.length();
        std::string strPacketLen(reinterpret_cast<char *>(&packetLen), 4);
        mPacketReceived.insert(0, strPacketLen);

        mPacketReceivedPos = 0;

        mNtlmFilterPtr->onReadCompleted(ByteBuffer());
    }

    std::string NtlmWrapper::getPacketToSend()
    {
        return mPacketToSend;
    }

    // Filter functions are downstream and are called by the NTLM filter.
    void NtlmWrapper::read(
        const ByteBuffer &byteBuffer,
        std::size_t bytesRequested)
    {
        RCF_UNUSED_VARIABLE(bytesRequested);
        if ( mPacketReceivedPos < mPacketReceived.size() )
        {
            RCF_ASSERT(bytesRequested <= mPacketReceived.size());
            memcpy(byteBuffer.getPtr(), mPacketReceived.c_str() + mPacketReceivedPos, byteBuffer.getLength());
            mPacketReceivedPos += byteBuffer.getLength();
            mNtlmFilterPtr->onReadCompleted(byteBuffer);
        }
    }


    void NtlmWrapper::write(const std::vector<ByteBuffer> &byteBuffers)
    {
        RCF_ASSERT(byteBuffers.size() == 1);
        
        // Remove the length field that the filter puts at the front.
        mPacketToSend = std::string(byteBuffers[0].getPtr() + 4, byteBuffers[0].getLength() - 4);
        
        mNtlmFilterPtr->onWriteCompleted(byteBuffers[0].getLength());
    }

    void NtlmWrapper::onReadCompleted(const ByteBuffer &)
    {
        // Should never be called.
    }

    void NtlmWrapper::onWriteCompleted(std::size_t)
    {
        // Should never be called.
    }

    int NtlmWrapper::getFilterId() const
    {
        return RcfFilter_Unknown;
    }

} // namespace RCF

#endif // RCF_FEATURE_SSPI

namespace RCF {

    HttpConnectFilter::HttpConnectFilter() :
        mServerAddr(),
        mServerPort(0),
        mCurrentAuthType(0)
    {
        resetState();
    }

    HttpConnectFilter::HttpConnectFilter(const std::string serverAddr, int serverPort) :
        mServerAddr(serverAddr),
        mServerPort(serverPort),
        mCurrentAuthType(0)
    {
        resetState();

        mProxyAuthTypes.clear();
        mProxyAuthTypes.push_back(std::make_pair(HttpConnectFilter::AuthType_None, std::string()));
    }

    HttpConnectFilter::~HttpConnectFilter()
    {
    }

    void HttpConnectFilter::resetState()
    {
        mPassThrough = false;
        mOrigWriteBuffers.clear();
        mHttpProxyRequest.clear();
        mHttpProxyResponse.clear();
        mWritePos = 0;
        mReadPos = 0;
        mReadVector.clear(); 
    }

    void HttpConnectFilter::read(
        const ByteBuffer &byteBuffer,
        std::size_t bytesRequested)
    {
        RCF_ASSERT(mPassThrough);
        mpPostFilter->read(byteBuffer, bytesRequested);
    }

    void HttpConnectFilter::sendProxyRequest()
    {
        mWritePos = 0;

        std::vector<ByteBuffer> writeBuffers;

        writeBuffers.push_back(ByteBuffer(
            (char *)mHttpProxyRequest.c_str(),
            mHttpProxyRequest.size()));

        mpPostFilter->write(writeBuffers);
    }

    void HttpConnectFilter::write(const std::vector<ByteBuffer> &byteBuffers)
    {
        if (mPassThrough)
        {
            mpPostFilter->write(byteBuffers);
        }
        else
        {
            ClientStub * pClientStub = getTlsClientStubPtr();
            RCF_ASSERT(pClientStub);
            bool usingProxy = pClientStub->getHttpProxy().size() > 0;
            if (!usingProxy)
            {
                mPassThrough = true;
                write(byteBuffers);
            }
            else
            {
                mOrigWriteBuffers = byteBuffers;

                AuthType authType = mProxyAuthTypes[mCurrentAuthType].first;
                std::string realmValue = mProxyAuthTypes[mCurrentAuthType].second;

                if ( authType == HttpConnectFilter::AuthType_None )
                {
                    MemOstream os;
                    os
                        << "CONNECT " << mServerAddr << ":" << mServerPort << " HTTP/1.1\r\n"
                        << "Host: " << mServerAddr << ":" << mServerPort << "\r\n"
                        << "Proxy-Connection: Keep-Alive\r\n"
                        << "\r\n";

                    mHttpProxyRequest = os.string();
                }
                else if ( authType == HttpConnectFilter::AuthType_Basic )
                {
                    std::string username = toAstring(pClientStub->getHttpProxyUserName());
                    std::string password = toAstring(pClientStub->getHttpProxyPassword());
                    std::string authValue = username + ":" + password;

                    std::string base64Auth = base64_encode(
                        (const unsigned char *) authValue.c_str(), 
                        (unsigned int)          authValue.size());

                    MemOstream os;
                    os
                        << "CONNECT " << mServerAddr << ":" << mServerPort << " HTTP/1.1\r\n"
                        << "Host: " << mServerAddr << ":" << mServerPort << "\r\n"
                        << "Proxy-Connection: Keep-Alive\r\n"
                        << "Proxy-Authorization: Basic " << base64Auth << "\r\n"
                        << "\r\n";

                    mHttpProxyRequest = os.string();
                }
                else if ( authType == HttpConnectFilter::AuthType_Ntlm )
                {
                    mNtlmWrapper.reset( new NtlmWrapper() );
                    std::string authValue = mNtlmWrapper->getPacketToSend();
                    
                    std::string base64Auth = base64_encode(
                        (const unsigned char *)     authValue.c_str(), 
                        (unsigned int)              authValue.size());

                    MemOstream os;
                    os
                        << "CONNECT " << mServerAddr << ":" << mServerPort << " HTTP/1.1\r\n"
                        << "Host: " << mServerAddr << ":" << mServerPort << "\r\n"
                        << "Proxy-Connection: Keep-Alive\r\n"
                        << "Proxy-Authorization: NTLM " << base64Auth << "\r\n"
                        << "\r\n";

                    mHttpProxyRequest = os.string();
                }
                else
                {
                    RCF_ASSERT_ALWAYS("Not implemented yet!");
                }

                sendProxyRequest();
            }
        }
    }

    void HttpConnectFilter::tryNextAuthType()
    {
        bool shouldRetry = false;

        std::size_t curIdx = mCurrentAuthType;
        while ( ++curIdx < mProxyAuthTypes.size() )
        {
            AuthType authType = mProxyAuthTypes[curIdx].first;

            if ( authType == HttpConnectFilter::AuthType_Basic )
            {
                mCurrentAuthType = curIdx;
                shouldRetry = true;
                break;
            }

#if RCF_FEATURE_SSPI==1
            if ( authType == HttpConnectFilter::AuthType_Ntlm )
            {
                mCurrentAuthType = curIdx;
                shouldRetry = true;
                break;
            }
#endif

        }

        if ( shouldRetry )
        {
            Exception e(RcfError_ProxyAuthRetry);
            e.setShouldRetry(true);
            RCF_THROW(e);
        }
        else
        {
            ClientStub * pClientStub = getTlsClientStubPtr();
            if ( pClientStub && pClientStub->getUserName().empty() )
            {
                // No credentials supplied, so the only thing that could have worked in any case is NTLM with logged on credentials.
                std::string realmValue;
                for ( std::size_t i = 0; i < mProxyAuthTypes.size(); ++i )
                {
                    if ( mProxyAuthTypes[i].second.size() > 0 )
                    {
                        realmValue = mProxyAuthTypes[i].second;
                    }
                }
                
                if ( realmValue.size() > 0 )
                {
                    pClientStub->setHttpProxyRealm(toTstring(realmValue));
                }

                Exception e(RcfError_ProxyCredentialsNeeded);
                RCF_THROW(e);
            }
            else
            {
                // Credentials were supplied, but the proxy did not recognize them.
                Exception e(RcfError_ProxyCredentialsInvalid);
                RCF_THROW(e);
            }
        }
    }

    void HttpConnectFilter::doNtlmHandshake()
    {
        // Dig out the NTLM packet and proceed another step through the handshake.

        std::string proxyAuthHeader = "Proxy-Authenticate";

        bool ok = false;
        for ( std::size_t i = 0; i < mHttpMessage.mHeaderList.size(); ++i )
        {
            if ( iequals(proxyAuthHeader, mHttpMessage.mHeaderList[i].first) )
            {
                std::string& authValue = mHttpMessage.mHeaderList[i].second;
                if ( istartsWith(authValue, "NTLM ") )
                {
                    std::string base64Auth = authValue.substr(5);
                    trim(base64Auth);
                    std::string authPacketIn = base64_decode(base64Auth);

                    RCF_ASSERT(mNtlmWrapper.get());
                    mNtlmWrapper->onPacketReceived(authPacketIn);
                    std::string authPacketOut = mNtlmWrapper->getPacketToSend();

                    base64Auth = base64_encode((const unsigned char *)authPacketOut.c_str(), (unsigned int)authPacketOut.size());

                    MemOstream os;
                    os
                        << "CONNECT " << mServerAddr << ":" << mServerPort << " HTTP/1.1\r\n"
                        << "Host: " << mServerAddr << ":" << mServerPort << "\r\n"
                        << "Proxy-Connection: Keep-Alive\r\n"
                        << "Proxy-Authorization: NTLM " << base64Auth << "\r\n"
                        << "\r\n";

                    mHttpProxyRequest = os.string();

                    sendProxyRequest();
                    ok = true;
                    break;
                }
            }
        }

        if ( !ok )
        {
            tryNextAuthType();
        }

    }

    void HttpConnectFilter::doProxyAuthRetry()
    {
        mProxyAuthTypes.resize(1);
        
        std::string proxyAuthHeader = "Proxy-Authenticate";

        for ( std::size_t i = 0; i < mHttpMessage.mHeaderList.size(); ++i )
        {
            if ( iequals(proxyAuthHeader, mHttpMessage.mHeaderList[i].first) )
            {
                AuthType authType = HttpConnectFilter::AuthType_None;
                std::string realmValue;

                const std::string& headerValue = mHttpMessage.mHeaderList[i].second;
                if ( istartsWith(headerValue, "Basic") )
                {
                    authType = HttpConnectFilter::AuthType_Basic;
                }
                else if ( istartsWith(headerValue, "NTLM") )
                {
                    authType = HttpConnectFilter::AuthType_Ntlm;
                }
                else if ( istartsWith(headerValue, "Negotiate") )
                {
                    authType = HttpConnectFilter::AuthType_Negotiate;
                }
                else if ( istartsWith(headerValue, "Digest") )
                {
                    authType = HttpConnectFilter::AuthType_Digest;
                }

                std::string lookFor = "realm=\"";
                std::size_t pos1 = headerValue.find(lookFor);
                if ( pos1 != std::string::npos )
                {
                    pos1 += lookFor.size();
                    std::size_t pos2 = headerValue.find("\"", pos1);
                    if ( pos2 != std::string::npos )
                    {
                        realmValue = headerValue.substr(pos1, pos2 - pos1);
                    }
                }

                if ( authType != HttpConnectFilter::AuthType_None )
                {
                    mProxyAuthTypes.push_back(std::make_pair(authType, realmValue));
                }
            }
        }

        tryNextAuthType();
    }

    void HttpConnectFilter::onReadCompleted(const ByteBuffer &byteBuffer)
    {
        if (mPassThrough)
        {
            mpPreFilter->onReadCompleted(byteBuffer);
        }
        else
        {
            RCF_ASSERT(byteBuffer.getLength() <= mReadVector.size());
            mReadPos += byteBuffer.getLength();

            mHttpProxyResponse.assign(&mReadVector[0], mReadPos);

            bool parseOk = mHttpMessage.parseHttpMessage(
                mHttpProxyResponse.c_str(), 
                mHttpProxyResponse.size());

            if ( !parseOk )
            {
                // Don't have a complete message yet. Read some more.
                if ( mReadPos == mReadVector.size() )
                {
                    RCF_THROW(Exception(RcfError_InvalidHttpMessage));
                }
                ByteBuffer buffer(&mReadVector[mReadPos], mReadVector.size() - mReadPos);
                mpPostFilter->read(buffer, buffer.getLength());
            }
            else
            {
                std::string httpStatus;
                std::string httpStatusMsg;
                mHttpMessage.getHttpStatus(httpStatus, httpStatusMsg);
                if ( httpStatus == "200" )
                {
                    // CONNECT is OK. All done.
                    mPassThrough = true;
                    mpPostFilter->write(mOrigWriteBuffers);
                }
                else if ( httpStatus == "407" )
                {
                    if ( mCurrentAuthType == 0 )
                    {
                        // Proxy has notified us that we need to authenticate.
                        doProxyAuthRetry();
                    }
                    else
                    {
                        // We have already initiated Basic or NTLM authentication.

                        AuthType authType = mProxyAuthTypes[mCurrentAuthType].first;
                        if ( authType == HttpConnectFilter::AuthType_Basic )
                        {
                            // Basic authentication credentials did not work. 
                            // See if there are any other authentication mechanisms we can use.
                            tryNextAuthType();
                        }
                        else if ( authType == HttpConnectFilter::AuthType_Ntlm )
                        {
                            doNtlmHandshake();
                        }
                        else
                        {
                            // Not sure how we would end up here.
                            tryNextAuthType();
                        }
                    }
                }
            }
        }
    }

    void HttpConnectFilter::onWriteCompleted(std::size_t bytesTransferred)
    {
        if (mPassThrough)
        {
            mpPreFilter->onWriteCompleted(bytesTransferred);
        }
        else
        {
            mWritePos += bytesTransferred;
            if (mWritePos < mHttpProxyRequest.size())
            {
                // Still have more bytes to write.
                std::vector<ByteBuffer> writeBuffers;

                writeBuffers.push_back( ByteBuffer(
                    (char *) (mHttpProxyRequest.c_str() + mWritePos), 
                    mHttpProxyRequest.size() - mWritePos));

                mpPostFilter->write(writeBuffers);
            }
            else
            {
                // Request has been sent, now wait for reply.
                mReadVector.resize(1024);
                ByteBuffer buffer(&mReadVector[0], mReadVector.size());
                mpPostFilter->read(buffer, buffer.getLength());
            }
        }
    }

    int HttpConnectFilter::getFilterId() const
    {
        return RcfFilter_Unknown;
    }

} // namespace RCF
