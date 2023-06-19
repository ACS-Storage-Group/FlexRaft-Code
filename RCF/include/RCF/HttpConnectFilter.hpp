
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

#ifndef INCLUDE_RCF_HTTPCONNECTFILTER_HPP
#define INCLUDE_RCF_HTTPCONNECTFILTER_HPP

#include <memory>
#include <memory>

#include <RCF/Filter.hpp>
#include <RCF/HttpFrameFilter.hpp>

namespace RCF {

    class NtlmWrapper;

    class ByteBuffer;

    class HttpConnectFilter : public Filter
    {
    public:

        HttpConnectFilter();
        HttpConnectFilter(const std::string serverAddr, int serverPort);
        ~HttpConnectFilter();

        void resetState();

        void read(
            const ByteBuffer &byteBuffer,
            std::size_t bytesRequested);

        void write(const std::vector<ByteBuffer> &byteBuffers);

        void onReadCompleted(const ByteBuffer &byteBuffer);

        void onWriteCompleted(std::size_t bytesTransferred);

        int getFilterId() const;

        enum AuthType
        {
            AuthType_None,
            AuthType_Basic,
            AuthType_Digest,
            AuthType_Ntlm,
            AuthType_Negotiate
        };

    private:

        void                        sendProxyRequest();
        void                        tryNextAuthType();

        void                        doNtlmHandshake();
        void                        doProxyAuthRetry();

        std::string                 mServerAddr;
        int                         mServerPort;

        bool                        mPassThrough;

        std::vector<ByteBuffer>     mOrigWriteBuffers;

        std::string                 mHttpProxyRequest;
        std::string                 mHttpProxyResponse;

        HttpMessage                 mHttpMessage;

        std::size_t                 mWritePos;
        std::size_t                 mReadPos;

        std::vector<char>           mReadVector;

        // List of (Auth type, realm) returned by the proxy.
        std::vector< std::pair<AuthType, std::string> >       
                                    mProxyAuthTypes;

        std::size_t                 mCurrentAuthType;

        std::unique_ptr<NtlmWrapper>    mNtlmWrapper;
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_HTTPCONNECTFILTER_HPP
