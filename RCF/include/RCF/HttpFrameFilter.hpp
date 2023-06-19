
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

#ifndef INCLUDE_RCF_HTTPFRAMEFILTER_HPP
#define INCLUDE_RCF_HTTPFRAMEFILTER_HPP

#include <map>

#include <memory>

#include <RCF/ByteBuffer.hpp>
#include <RCF/Filter.hpp>
#include <RCF/RecursionLimiter.hpp>

namespace RCF {

    class Exception;
    class MemOstream;
    class MemIstream;
    class ReallocBuffer;
    class HttpMessage;
    class HttpCookie;

    typedef std::shared_ptr<MemOstream> MemOstreamPtr;
    typedef std::shared_ptr<ReallocBuffer> ReallocBufferPtr;

    void splitString(
        const std::string & stringToSplit,
        const char * splitAt,
        std::vector<std::string> & lines);

    /// HTTP message verification mechanism, to allow applications to verify HTTP message payloads using custom HTTP headers.
    class HttpMessageVerifier
    {
    public:

        /// Set verification header on an outgoing HTTP message, based on the message payload passed in messageBuffers.
        virtual void applyHeader(const std::vector<ByteBuffer>& messageBuffers, std::string& headerName, std::string& headerValue) = 0;

        /// Check verification header on an incoming HTTP message, based on the message payload passed in msgData.
        virtual void verifyHeader(const HttpMessage& msg, ByteBuffer msgData) = 0;
    };

    typedef std::shared_ptr<HttpMessageVerifier> HttpMessageVerifierPtr;

    class RCF_EXPORT HttpMessage
    {
    public:

        HttpMessage() : 
            mHeaderLen(0), 
            mContentLen(0), 
            mFrameLen(0)
        {
        }

        ~HttpMessage()
        {
        }

        bool parseHttpMessage(const char * pFrame, std::size_t bytesAvailable);

        void getHttpStatus(std::string& httpStatus, std::string& httpStatusMsg);
        void getHeaderValue(const std::string& headerName, std::string& headerValue) const;

        typedef std::vector< std::pair< std::string, std::string > > HeaderList;
        HeaderList                              mHeaderList;

        std::size_t                             mHeaderLen;
        std::size_t                             mContentLen;
        std::size_t                             mFrameLen;
        std::string                             mHttpMessageHeader;
        std::vector<std::string>                mMessageLines;
        std::string                             mRequestLine;
        std::string                             mResponseLine;
    };

    class HttpFrameFilter : public Filter
    {
    public:

        // Server-side constructor.
        HttpFrameFilter(std::size_t maxMessageLength);

        // Client-side constructor.
        HttpFrameFilter(
            const std::string &     serverAddr, 
            int                     serverPort,
            const std::string &     serverUrlPath);
        
        ~HttpFrameFilter();

        void                    init();
        void                    resetState();

        void                    read(
                                    const ByteBuffer &      byteBuffer,
                                    std::size_t             bytesRequested);

        void                    write(const std::vector<ByteBuffer> &byteBuffers);

        void                    onReadCompleted(const ByteBuffer &byteBuffer);

        void                    onWriteCompleted(std::size_t bytesTransferred);

        int                     getFilterId() const;

        virtual std::size_t     getFrameSize();

        const std::string &     getHttpSessionId();
        std::uint32_t           getHttpSessionIndex();
        const std::string &     getConnectionHeader();

        void                    sendHttpTextResponse(const std::string & httpStatus, const std::string & responseText);
        void                    sendHttpErrorResponse(MemOstreamPtr osPtr);
        void                    onError(const Exception& e);

        // If these are set, then we are doing a HTTP response with chunked transfer encoding.
        bool                    mChunkedResponseMode;
        std::size_t             mChunkedResponseCounter;

        bool                    tryParseHttpHeader();
        
        bool                    tryParseHttpChunkHeader();

        void                    getHttpFrameInfo(
                                    std::string&                                            requestLine,
                                    std::vector< std::pair<std::string, std::string> >&     headers);

        bool                    verifyReceivedMessage();

    private:

        void                    processSetCookieHeader(const std::string& headerValue);
        void                    processLocationHeader();


        void                    sendServerError(int error);

        void                    resizeReadBuffer(std::size_t newSize);

        const std::string &     getServerHeader();

        std::string                         mServerAddr;
        int                                 mServerPort;
        std::string                         mServerUrlPath;
        std::string                         mServerHeaderValue;

        bool                                mClientSide;
        std::string                         mHttpSessionId;
        std::uint32_t                       mHttpSessionIndex;
        std::string                         mConnectionHeader;
        std::string                         mTransferEncoding;

        MemOstreamPtr                       mOsPtr;
        std::vector<ByteBuffer>             mWriteBuffers;
        std::size_t                         mWritePos;

        ByteBuffer                          mOrigReadBuffer;
        std::size_t                         mOrigBytesRequested;

        ReallocBufferPtr                    mReadBufferPtr;
        std::size_t                         mBytesReceived;
        std::size_t                         mReadPos;

        bool                                mProtocolChecked;
        std::size_t                         mChunkHeaderLen;
        std::size_t                         mChunkLen;
        std::size_t                         mMaxReadPos;
        std::size_t                         mMaxMessageLength;
        bool                                mMaxMessageLengthSet;

        std::string                         mPrevHttpSessionId;
        std::uint32_t                       mPrevHttpSessionIndex;

        HttpMessage                         mHttpMessage;

        std::map<std::string, HttpCookie>   mCookies;

        RecursionState<ByteBuffer, std::size_t>         mRecursionStateRead;
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_HTTPFRAMEFILTER_HPP
