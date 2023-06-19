
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

#include <RCF/HttpFrameFilter.hpp>

#include <RCF/AsioServerTransport.hpp>
#include <RCF/ByteBuffer.hpp>
#include <RCF/ClientStub.hpp>
#include <RCF/Exception.hpp>
#include <RCF/ObjectPool.hpp>
#include <RCF/RcfSession.hpp>
#include <RCF/ThreadLocalData.hpp>
#include <RCF/Log.hpp>
#include <RCF/Uuid.hpp>

#include <iomanip>
#include <time.h>

#ifndef RCF_WINDOWS
#define strnicmp strncasecmp
#define stricmp strcasecmp
#endif

namespace RCF  {

    // Splits a string without making any memory allocations, by reusing data structures.
    void splitString(
        const std::string & stringToSplit, 
        const char * splitAt, 
        std::vector<std::string> & lines)
    {
        std::size_t lineCounter = 0;
        std::size_t splitAtLen = strlen(splitAt);

        std::size_t pos = 0;
        std::size_t posNext = 0;
        while (pos < stringToSplit.size() && pos != std::string::npos && lineCounter < 50)
        {
            posNext = stringToSplit.find(splitAt, pos);

            // The split will not include empty strings.
            if ( posNext > pos + splitAtLen )
            {
                RCF_ASSERT(lineCounter <= lines.size());
                if ( lineCounter == lines.size() )
                {
                    lines.push_back(std::string());
                }
                if ( posNext == std::string::npos )
                {
                    posNext = stringToSplit.size();
                }
                lines[lineCounter].assign(stringToSplit.c_str() + pos, posNext - pos);
                ++lineCounter;
            }

            pos = posNext + splitAtLen;
        }

        // Pad with empty strings on the end. That way we can hang onto memory that has already been
        // allocated for the list.
        for ( std::size_t i = lineCounter; i < lines.size(); ++i )
        {
            lines[i].resize(0);
        }
    }

//#ifdef _MSC_VER
//#pragma warning( push )
//#pragma warning( disable : 4996 )  // warning C4996: 'ctime' was declared deprecated
//#endif
//
//    bool istarts_with(const std::string& line, const std::string& searchFor)
//    {
//        return 0 == strnicmp(line.c_str(), searchFor.c_str(), searchFor.size());
//    }
//
//    bool iequals(const std::string& lhs, const std::string& rhs)
//    {
//        return 0 == stricmp(lhs.c_str(), rhs.c_str());
//    }
//
//#ifdef _MSC_VER
//#pragma warning( pop )
//#endif

    HttpFrameFilter::HttpFrameFilter(std::size_t maxMessageLength) :
        mChunkedResponseMode(false),
        mChunkedResponseCounter(0),
        mServerPort(0),
        mClientSide(false),
        mHttpSessionIndex(0),
        mMaxMessageLength(maxMessageLength),
        mMaxMessageLengthSet(true)
    {
        init();
    }

    HttpFrameFilter::HttpFrameFilter(const std::string & serverAddr, int serverPort, const std::string & serverUrlPath) :
        mChunkedResponseMode(false),
        mChunkedResponseCounter(0),
        mServerAddr(serverAddr),
        mServerPort(serverPort),
        mServerUrlPath(serverUrlPath),
        mClientSide(true),
        mHttpSessionIndex(0),
        mMaxMessageLength(0),
        mMaxMessageLengthSet(false)
    {

        if ( mServerUrlPath.empty() )
        {
            mServerUrlPath = "/";
        }
        if ( mServerUrlPath.front() != '/' )
        {
            mServerUrlPath = "/" + mServerUrlPath;
        }
        if ( mServerUrlPath.back() != '/' )
        {
            mServerUrlPath += "/" ;
        }

        init();
    }

    HttpFrameFilter::~HttpFrameFilter()
    {
    }

    void HttpFrameFilter::init()
    {
        resetState();

        mOsPtr.reset(new MemOstream());
    }

    void HttpFrameFilter::resetState()
    {
        mWriteBuffers.clear();
        mWritePos = 0;
        mReadBufferPtr = getObjectPool().getReallocBufferPtr();
        mBytesReceived = 0;
        mReadPos = 0;
        mHttpMessage.mHeaderLen = 0;
        mHttpMessage.mContentLen = 0;
        mChunkHeaderLen = 0;
        mChunkLen = 0;
        mHttpMessage.mFrameLen = 0;
        mMaxReadPos = 0;
        mOrigBytesRequested = 0;
        mProtocolChecked = false;

        mPrevHttpSessionIndex = 0;

        if ( mClientSide )
        {
            // Client-side needs to specify a session id to send through in the HTTP header,
            // so the server-side can piece together the RPC stream.

            //boost::uuids::uuid uuid = boost::uuids::random_generator()();
            //std::string uuidStr = boost::uuids::to_string(uuid);
            //mHttpSessionId = uuidStr;

            mHttpSessionId = generateUuid();

            mHttpSessionIndex = 0;
        }
    }

    void HttpFrameFilter::resizeReadBuffer(std::size_t newSize)
    {
        if ( !mReadBufferPtr )
        {
            mReadBufferPtr = getObjectPool().getReallocBufferPtr();
        }
        mReadBufferPtr->resize(newSize);
    }

    void HttpFrameFilter::read(
        const ByteBuffer &byteBuffer,
        std::size_t bytesRequested)
    {
        RCF_ASSERT(mReadPos <= mMaxReadPos);
        std::size_t bytesAvailableInCurrentFrame = mMaxReadPos - mReadPos;

        if ( !mMaxMessageLengthSet )
        {
            ClientStub * pClientStub = getTlsClientStubPtr();
            if ( pClientStub )
            {
                ClientTransport & clientTransport = pClientStub->getTransport();
                mMaxMessageLength = clientTransport.getMaxIncomingMessageLength();
                mMaxMessageLengthSet = true;
            }
        }
        
        if (bytesRequested == 0)
        {
            // Return any buffers we have, back to the pool.
            mReadBufferPtr.reset();

            mpPostFilter->read(ByteBuffer(), bytesRequested);
        }
        else if ( bytesAvailableInCurrentFrame == 0 )
        {
            // Start reading a new HTTP message.
            mOrigReadBuffer = byteBuffer;
            mOrigBytesRequested = bytesRequested;

            // If we already have bytes available for the next frame, move those bytes to 
            // the front of the buffer.
            if ( mBytesReceived > mHttpMessage.mFrameLen && mReadBufferPtr )
            {
                std::memmove(&(*mReadBufferPtr)[0], &(*mReadBufferPtr)[mHttpMessage.mFrameLen], mBytesReceived - mHttpMessage.mFrameLen);
                mReadBufferPtr->resize(mBytesReceived - mHttpMessage.mFrameLen);
                mBytesReceived = mReadBufferPtr->size();
            }
            else
            {
                mBytesReceived = 0;
            }

            // Prepare for the next frame.
            mHttpMessage.mHeaderLen = 0;
            mHttpMessage.mContentLen = 0;
            mHttpMessage.mFrameLen = 0;
            mReadPos = 0;
            mMaxReadPos = 0;
            mProtocolChecked = false;

            if ( mBytesReceived )
            {
                // Fake a completion with the bytes we already have.
                mBytesReceived = 0;
                onReadCompleted(ByteBuffer(mReadBufferPtr));
            }
            else
            {
                // Read from the network.
                mBytesReceived = 0;
                resizeReadBuffer(1024);
                mpPostFilter->read(
                    ByteBuffer(mReadBufferPtr),
                    mReadBufferPtr->size());
            }
        }
        else
        {
            // Return bytes from currently loaded frame.
            std::size_t bytesToReturn = RCF_MIN(bytesAvailableInCurrentFrame, bytesRequested);
            ByteBuffer byteBuffer_ = byteBuffer;
            if ( byteBuffer_.isEmpty() )
            {
                ReallocBufferPtr reallocBuffer = getObjectPool().getReallocBufferPtr();
                reallocBuffer->resize(bytesToReturn);
                byteBuffer_ = ByteBuffer(reallocBuffer);
            }
            bytesToReturn = RCF_MIN(bytesToReturn, byteBuffer_.getLength());
            memcpy(byteBuffer_.getPtr(), &(*mReadBufferPtr)[mReadPos], bytesToReturn);
            mReadPos += bytesToReturn;
            mRecursionStateRead.clear();
            mpPreFilter->onReadCompleted(ByteBuffer(byteBuffer_, 0, bytesToReturn));
        }
    }

    const char * strnstr(const char * str, std::size_t strSize, const char * searchFor)
    {
        const char * iter = std::search(
            str,
            str + strSize,
            searchFor,
            searchFor + strlen(searchFor));

        if ( iter == str + strSize )
        {
            return NULL;
        }

        return iter;
    }

    static const std::string CrLf               = "\r\n";
    static const std::string CrLfCrLf           = "\r\n\r\n";

    static const std::string ContentLength      = "Content-Length";
    static const std::string Connection         = "Connection";
    static const std::string TransferEncoding   = "Transfer-Encoding";
    static const std::string Chunked            = "chunked";
    static const std::string XRcfSessionId      = "X-RCFSessionId";
    static const std::string XRcfSessionIndex   = "X-RCFSessionIndex";
    static const std::string XRcfError          = "X-RCFError";
    static const std::string SetCookie          = "Set-Cookie";


    bool HttpFrameFilter::tryParseHttpChunkHeader()
    {
        const char * pBody = &(*mReadBufferPtr)[mHttpMessage.mHeaderLen];
        const char * pChar = strnstr(pBody, mBytesReceived - mHttpMessage.mHeaderLen, CrLf.c_str());
        if ( pChar )
        {
            mChunkHeaderLen = pChar - pBody + 2;
            mChunkLen = strtoul(pBody, NULL, 16);

            mHttpMessage.mFrameLen = mHttpMessage.mHeaderLen + mChunkHeaderLen + mChunkLen + 2;

            if ( RCF::LogManager::instance().isEnabled(LogNameRcf, LogLevel_3) )
            {
                std::string httpMessageHeader(pBody, mChunkHeaderLen);
                RCF_LOG_3()(this)(mHttpSessionId)(mHttpSessionIndex)(mHttpMessage.mFrameLen)("\n" + httpMessageHeader) << "Received chunked HTTP message";
            }

            // Message length check.
            if ( mMaxMessageLength && mHttpMessage.mFrameLen > mMaxMessageLength )
            {
                int rcfError = mClientSide ? RcfError_ClientMessageLength_Id : RcfError_ServerMessageLength_Id;
                RCF_THROW(Exception(ErrorMsg(rcfError)));
            }

            mMaxReadPos = mHttpMessage.mFrameLen - 2;
            ++mChunkedResponseCounter;
        }
        return true;
    }


    void HttpMessage::getHttpStatus(std::string& httpStatus, std::string& httpStatusMsg)
    {
        MemIstream is(mMessageLines[0].c_str(), mMessageLines[0].size());
        std::string httpVer;
        is >> httpVer >> httpStatus >> httpStatusMsg;
        trimLeft(httpStatus);
    }

    void HttpMessage::getHeaderValue(const std::string& headerName, std::string& headerValue) const
    {
        headerValue = "";
        for ( std::size_t i = 0; i < mHeaderList.size(); ++i )
        {
            if ( iequals(headerName, mHeaderList[i].first) )
            {
                headerValue = mHeaderList[i].second;
            }
        }
    }
    
    bool HttpMessage::parseHttpMessage(const char * pFrame, std::size_t bytesAvailable)
    {
        // Search for CRLF CRLF to mark end of HTTP header, and then parse the 
        // lines in the header.

        const char * pChar = strnstr(pFrame, bytesAvailable, CrLfCrLf.c_str());
        if ( !pChar )
        {
            return false;
        }

        mHeaderLen = pChar - pFrame + 4;

        mRequestLine.clear();
        mResponseLine.clear();

        // Store the HTTP header in a string. Round up the size so the same string
        // can fit subsequent headers as well, without new allocations. X-RCFSessionIndex 
        // in particular will make the header size grow.
        std::size_t headerLenRoundedUp = 100 * (1 + mHeaderLen / 100);
        mHttpMessageHeader.reserve(headerLenRoundedUp);
        mHttpMessageHeader.assign(pFrame, mHeaderLen);

        // Split HTTP message into lines.
        splitString(mHttpMessageHeader, CrLf.c_str(), mMessageLines);

        if ( mMessageLines.empty() )
        {
            return false;
        }

        // Parse request/response line.
        const std::string & firstLine = mMessageLines.front();
        if ( 0 == strncmp(firstLine.c_str(), "POST", 4) )
        {
            mRequestLine = mMessageLines.front();
        }
        else if ( 0 == strncmp(firstLine.c_str(), "GET", 3) )
        {
            mRequestLine = mMessageLines.front();
        }
        else if ( 0 == strncmp(firstLine.c_str(), "HTTP/", 5) )
        {
            mResponseLine = mMessageLines.front();
        }
        
        // Parse headers.
        mHeaderList.resize(mMessageLines.size() - 1);
        for ( std::size_t i = 1; i < mMessageLines.size(); ++i )
        {
            const std::string & line = mMessageLines[i];

            std::pair<std::string, std::string> & header = mHeaderList[i-1];

            header.first.assign("");
            header.second.assign("");

            std::size_t pos = line.find(':');
            if ( pos != std::string::npos )
            {
                const char * pHeaderName = line.c_str();
                const char * pHeaderValue = pHeaderName + pos;
                while ( isspace((int)*(++pHeaderValue)) );

                header.first.assign(pHeaderName, pos);
                header.second.assign(pHeaderValue);
            }
            else if ( line.size() > 0 )
            {
                // Empty lines are OK and just an artifact of our efforts to reuse objects to avoid memory allocations.
                RCF_ASSERT(0 && "Encountered invalid header line in HTTP message.");
            }
        }

        return true;
    }

    const char* ws = " \t\n\r\f\v";

    // Trim whitespace from right.
    std::string& trimRight(std::string& s, const char* t = ws)
    {
        s.erase(s.find_last_not_of(t) + 1);
        return s;
    }

    // Trim whitespace from left.
    std::string& trimLeft(std::string& s, const char* t = ws)
    {
        s.erase(0, s.find_first_not_of(t));
        return s;
    }

    // Trim whitespace.
    inline std::string& trimBoth(std::string& s, const char* t = ws)
    {
        return trimLeft(trimRight(s, t), t);
    }

    void makeHttpDateHeaderValue(char * buf, std::size_t bufSize)
    {

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable: 4996) // warning C4996: 'gmtime': This function or variable may be unsafe. Consider using gmtime_s instead. To disable deprecation, use _CRT_SECURE_NO_WARNINGS. See online help for details.
#endif

        time_t now = time(0);
        struct tm tm = *gmtime(&now);
        strftime(buf, bufSize, "%a, %d %b %Y %H:%M:%S %Z", &tm);

#if defined(_MSC_VER)
#pragma warning(pop)
#endif

    }

    // Drop in replacement for Linux strptime() function, for parsing HTTP date time strings.
    char* RCF_strptime(
        const char* szToParse,
        const char* szFormat,
        struct tm* tm) 
    {
        std::istringstream input(szToParse);
        input.imbue(std::locale(setlocale(LC_ALL, nullptr)));
        input >> std::get_time(tm, szFormat);
        if ( input.fail() ) 
        {
            return nullptr;
        }
        return (char*)(szToParse + (std::size_t) input.tellg());
    }

    void HttpFrameFilter::sendHttpTextResponse(const std::string & httpStatus, const std::string & responseBody)
    {
        MemOstreamPtr osPtr(new MemOstream());

        char dateHeader[1000] = {};
        makeHttpDateHeaderValue(dateHeader, sizeof(dateHeader));

        *osPtr
            << "HTTP/1.1 " << httpStatus << "\r\n"
            << "Content-Length: " << responseBody.length() << "\r\n"
            << "Content-Type: text/plain\r\n"
            << "Server: " << getServerHeader() << "\r\n"
            << "Date: " << dateHeader << "\r\n"
            << "\r\n"
            << responseBody;

        sendHttpErrorResponse(osPtr);
    }

    bool HttpFrameFilter::tryParseHttpHeader()
    {
        // Sanity check that we are receiving a HTTP message.
        if ( !mProtocolChecked && mBytesReceived >= 4 )
        {
            char * pBuffer = &(*mReadBufferPtr)[0];

            if ( mClientSide && 0 != strncmp(pBuffer, "HTTP", 4) )
            {
                std::size_t displayLen = RCF_MIN(std::size_t(1024), mBytesReceived);
                std::string responseBytes(pBuffer, displayLen);
                Exception e(RcfError_NotHttpResponse, responseBytes);
                onError(e);
                return false;
            }
            else if ( !mClientSide && mBytesReceived >= 6 )
            {
                if ( 0 == strncmp(pBuffer, "GET / ", 6) )
                {
                    sendHttpTextResponse("200 OK", "Server is online.");
                    return false;
                }
                else if ( 0 == strncmp(pBuffer, "GET", 3) )
                {
                    // Returning a 200 here, because although the server doesn't care what the URL path is, it may have been set 
                    // by the client to a specific value in order to facilitate routing through a reverse proxy.
                    sendHttpTextResponse("200 OK", "Server is online.");
                    return false;
                }
                else if ( 0 != strncmp(pBuffer, "POST", 4) )
                {
                    sendHttpTextResponse("400 Bad Request", "Bad request.");
                    return false;
                }
                else
                {
                    mProtocolChecked = true;
                }
            }
        }

        const char * pFrame = &(*mReadBufferPtr)[0];
        bool parseOk = mHttpMessage.parseHttpMessage(pFrame, mBytesReceived);
        if ( !parseOk )
        {
            return true;
        }

        RCF_LOG_3()(this)(mHttpMessage.mFrameLen)("\n" + mHttpMessage.mHttpMessageHeader) << "Received HTTP message";

        // Scan the HTTP headers.

        mPrevHttpSessionId = mHttpSessionId;
        mPrevHttpSessionIndex = mHttpSessionIndex;

        mConnectionHeader.clear();
        mTransferEncoding.clear();
        mHttpSessionId.clear();
        mHttpSessionIndex = 0;
        
        for ( std::size_t i = 0; i < mHttpMessage.mHeaderList.size(); ++i )
        {
            const std::string& headerName = mHttpMessage.mHeaderList[i].first;
            const std::string& headerValue = mHttpMessage.mHeaderList[i].second;
            if ( iequals(headerName, ContentLength) )
            {
                // Should not be setting this more than once.
                RCF_ASSERT(mHttpMessage.mContentLen == 0);

                mHttpMessage.mContentLen = atoi(headerValue.c_str());
                mHttpMessage.mFrameLen = mHttpMessage.mHeaderLen + mHttpMessage.mContentLen;
                mMaxReadPos = mHttpMessage.mFrameLen;
            }
            else if ( iequals(headerName, XRcfSessionId) )
            {
                mHttpSessionId = headerValue;
                mPrevHttpSessionId.reserve(mHttpSessionId.size());
            }
            else if ( iequals(headerName, XRcfSessionIndex) )
            {
                std::uint32_t idx = atoi(headerValue.c_str());
                if ( idx )
                {
                    mHttpSessionIndex = idx;
                }
            }
            else if ( iequals(headerName, Connection) )
            {
                mConnectionHeader = headerValue;
            }
            else if ( iequals(headerName, XRcfError) )
            {
                if ( mClientSide )
                {
                    Exception e(RcfError_HttpTunnelError, headerValue);
                    RCF_THROW(e);
                }
            }
            else if ( iequals(headerName, TransferEncoding) )
            {
                mTransferEncoding = headerValue;
                if ( iequals(mTransferEncoding, Chunked) )
                {
                    mChunkedResponseMode = true;
                    mChunkedResponseCounter = 0;
                }
            }
            else if ( iequals(headerName, SetCookie) )
            {
                processSetCookieHeader(headerValue);
            }
            else if ( iequals(headerName, "Location") )
            {
                processLocationHeader();
            }
        }

        // Message length check.
        if ( mMaxMessageLength && mHttpMessage.mFrameLen > mMaxMessageLength )
        {
            if ( mClientSide )
            {
                RCF_THROW(Exception(RcfError_ClientMessageLength));
            }
            else
            {
                sendServerError(RcfError_ServerMessageLength_Id);
                return false;
            }
        }

        // For non-chunked responses, check that the header specified a Content-Length value.
        if ( mHttpMessage.mHeaderLen && !mChunkedResponseMode && mHttpMessage.mContentLen == 0 )
        {
            std::string httpMessage(&(*mReadBufferPtr)[0], mHttpMessage.mHeaderLen);
            if ( mHttpMessage.mResponseLine.size() > 0 )
            {
                onError(Exception(RcfError_HttpResponseContentLength, mHttpMessage.mResponseLine, httpMessage));
            }
            else
            {
                onError(Exception(RcfError_HttpRequestContentLength));
            }
            return false;
        }

        // Client side - check that the response session ID and session index matches with the request.
        if ( mClientSide )
        {
            if (    mPrevHttpSessionId.size() > 0 
                &&  mHttpSessionId.size() > 0 
                &&  mPrevHttpSessionId != mHttpSessionId )
            {
                Exception e(RcfError_HttpResponseSessionId, mPrevHttpSessionId, mHttpSessionId);
                onError(e);
                return false;
            }
            if (    mPrevHttpSessionIndex 
                &&  mHttpSessionIndex 
                &&  mPrevHttpSessionIndex != mHttpSessionIndex )
            {
                Exception e(RcfError_HttpResponseSessionIndex, mPrevHttpSessionIndex, mHttpSessionIndex);
                onError(e);
                return false;
            }
        }

        return true;
    }

    void HttpFrameFilter::processLocationHeader()
    {
        // Check that we are dealing with a 3xx redirect response.
        std::string status;
        std::string statusMsg;
        mHttpMessage.getHttpStatus(status, statusMsg);
        int nStatus = atoi(status.c_str());
        if ( nStatus / 100 == 3 )
        {
            ClientStub * pStub = RCF::getTlsClientStubPtr();
            if ( pStub )
            {
                HttpRedirectHandler redirectHandler = pStub->getHttpRedirectHandler();
                if ( redirectHandler )
                {
                    redirectHandler(mHttpMessage.mResponseLine, mHttpMessage.mHeaderList);
                }
                RCF_THROW(Exception(RcfError_HttpRedirect, mHttpMessage.mHttpMessageHeader));
            }
        }
    }

    void HttpFrameFilter::processSetCookieHeader(const std::string& headerValue)
    {
        // Check for any cookies returned by server.
        std::string cookieName;
        std::string cookieValue;
        bool cookieExpired = false;

        std::vector<std::string> parts;
        splitString(headerValue, ";", parts);
        if ( parts.size() > 0 )
        {
            for ( std::size_t j = 0; j < parts.size(); ++j )
            {
                auto pos = parts[j].find('=');
                if ( pos != std::string::npos )
                {
                    std::string keyName = parts[j].substr(0, pos);
                    keyName = trimBoth(keyName);

                    std::string keyValue = parts[j].substr(pos + 1);
                    keyValue = trimBoth(keyValue);

                    if ( j == 0 )
                    {
                        cookieName = keyName;
                        cookieValue = keyValue;
                    }
                    else
                    {
                        // Check for expiration, as that's how the server informs us that a cookie should be deleted.

                        if ( iequals(keyName, "Expires") )
                        {
                            std::string expiry = keyValue;

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable: 4996) // warning C4996: 'gmtime': This function or variable may be unsafe. Consider using gmtime_s instead. To disable deprecation, use _CRT_SECURE_NO_WARNINGS. See online help for details.
#endif

                            struct tm tmExpiryGmt = {};
                            char * ret = RCF_strptime(expiry.c_str(), "%a, %d-%b-%Y %H:%M:%S", &tmExpiryGmt);
                            RCF_ASSERT(ret && "Couldn't parse cookie expiry date time.");
                            if ( ret )
                            {
                                time_t secsNow = time(NULL);
                                struct tm * tmNowGmt = gmtime(&secsNow);

                                // Before comparing, increment both dates by a year so we don't encounter corner case of Jan 1 1970 as the expiry date, as mktime() may fail, depending on our timezome.
                                tmExpiryGmt.tm_year += 1;
                                tmNowGmt->tm_year += 1;
                                time_t secsExpiryGmt = mktime(&tmExpiryGmt);
                                time_t secsNowGmt = mktime(tmNowGmt);
                                double secsDiff = difftime(secsExpiryGmt, secsNowGmt);
                                if ( secsDiff < 0 )
                                {
                                    cookieExpired = true;
                                }
                            }
#if defined(_MSC_VER)
#pragma warning(pop)
#endif

                        }
                    }
                }
            }

            ClientStub * pStub = RCF::getTlsClientStubPtr();
            if ( pStub && cookieName.size() > 0 )
            {
                std::map<std::string, HttpCookie> & cookieMap = pStub->getCookieMap();
                if ( cookieExpired )
                {
                    cookieMap.erase(cookieName);
                }
                else
                {
                    cookieMap[cookieName] = HttpCookie(cookieName, cookieValue);
                }
            }
        }
    }

    void HttpFrameFilter::sendServerError(int error)
    {
        RcfSession * pSession = getCurrentRcfSessionPtr();
        if ( pSession )
        {
            NetworkSession& nwSession = pSession->getNetworkSession();
            AsioNetworkSession& asioNwSession = static_cast<AsioNetworkSession&>(nwSession);
            asioNwSession.setCloseAfterWrite();
            RcfServer& server = pSession->getRcfServer();

            // Encode error message.
            std::vector<ByteBuffer> byteBuffers(1);
            encodeServerError(server, byteBuffers.front(), error);

            // Add frame (4 byte length prefix).
            int messageSize = static_cast<int>(RCF::lengthByteBuffers(byteBuffers));
            ByteBuffer &byteBuffer = byteBuffers.front();
            RCF_ASSERT(byteBuffer.getLeftMargin() >= 4);
            byteBuffer.expandIntoLeftMargin(4);
            memcpy(byteBuffer.getPtr(), &messageSize, 4);
            RCF::machineToNetworkOrder(byteBuffer.getPtr(), 4, 1);

            write(byteBuffers);
        }
    }

    void HttpFrameFilter::onReadCompleted(const ByteBuffer & byteBuffer)
    {
        if (byteBuffer.isEmpty())
        {
            mpPreFilter->onReadCompleted(byteBuffer);
            return;
        }

        mBytesReceived += byteBuffer.getLength();
        RCF_ASSERT(mBytesReceived <= mReadBufferPtr->size());

        if ( mHttpMessage.mFrameLen )
        {
            std::size_t bytesRemainingInFrame = mHttpMessage.mFrameLen - mBytesReceived;
            if (bytesRemainingInFrame)
            {
                if ( mReadBufferPtr->size() < mHttpMessage.mFrameLen )
                {
                    mReadBufferPtr->resize(mHttpMessage.mFrameLen);
                }

                ByteBuffer bufferRemaining(ByteBuffer(mReadBufferPtr), mBytesReceived, bytesRemainingInFrame);
                if ( mClientSide )
                {
                    applyRecursionLimiter(
                        mRecursionStateRead,
                        &Filter::read,
                        *mpPostFilter,
                        bufferRemaining,
                        bytesRemainingInFrame);
                }
                else
                {
                    mpPostFilter->read(
                        bufferRemaining,
                        bytesRemainingInFrame);
                }
            }
            else
            {
                mReadPos = mHttpMessage.mHeaderLen + mChunkHeaderLen;

                ByteBuffer origReadBuffer = mOrigReadBuffer;
                std::size_t origBytesRequested = mOrigBytesRequested;
                mOrigReadBuffer = ByteBuffer();
                mOrigBytesRequested = 0;

                read(origReadBuffer, origBytesRequested);
            }
        }
        else
        {
            RCF_ASSERT(mHttpMessage.mFrameLen == 0);

            // Try to pick out HTTP header or chunk header, to determine frame length.
            bool shouldContinueReading = true;
            if ( mChunkedResponseMode && mChunkedResponseCounter > 0)
            {
                shouldContinueReading = tryParseHttpChunkHeader();
            }
            else
            {
                shouldContinueReading = tryParseHttpHeader();
                if ( shouldContinueReading && mChunkedResponseMode )
                {
                    shouldContinueReading = tryParseHttpChunkHeader();
                }
            }
            if ( !shouldContinueReading )
            {
                return;
            }

            if ( mHttpMessage.mFrameLen == 0 )
            {
                // Still don't know the frame length, so do another read on the network.
                if ( mBytesReceived > 10 * 1024 )
                {
                    onError(Exception(RcfError_InvalidHttpMessage));
                    return;
                }
                if ( mBytesReceived == mReadBufferPtr->size() )
                {
                    mReadBufferPtr->resize(mReadBufferPtr->size() + 1024);
                }
                mpPostFilter->read(
                    ByteBuffer(ByteBuffer(mReadBufferPtr), mBytesReceived),
                    mReadBufferPtr->size() - mBytesReceived);
            }
            else 
            {
                // We have the frame length.
                RCF_ASSERT(mHttpMessage.mFrameLen);
                if ( mChunkedResponseMode )
                {
                    RCF_ASSERT(mHttpMessage.mFrameLen == mHttpMessage.mHeaderLen + mChunkHeaderLen + mChunkLen + 2);
                }
                else
                {
                    RCF_ASSERT(mHttpMessage.mFrameLen == mHttpMessage.mHeaderLen + mHttpMessage.mContentLen);
                }

                // Read in any remaining bytes.
                if ( mBytesReceived < mHttpMessage.mFrameLen )
                {
                    std::size_t bytesRemainingInFrame = mHttpMessage.mFrameLen - mBytesReceived;
                    if ( mReadBufferPtr->size() < mBytesReceived + bytesRemainingInFrame )
                    {
                        mReadBufferPtr->resize(mBytesReceived + bytesRemainingInFrame);
                    }
                    mpPostFilter->read(
                        ByteBuffer(ByteBuffer(mReadBufferPtr), mBytesReceived, bytesRemainingInFrame),
                        bytesRemainingInFrame);
                }
                else
                {
                    // Entire frame now available.

                    // Client side - check HTTP response status.
                    if ( mClientSide )
                    {
                        std::size_t pos = mHttpMessage.mResponseLine.find("200");
                        if ( pos == std::string::npos )
                        {
                            std::string httpMessage(&(*mReadBufferPtr)[0], mHttpMessage.mHeaderLen);
                            onError(Exception(RcfError_HttpResponseStatus, mHttpMessage.mResponseLine, httpMessage));
                            return;
                        }
                    }

                    // Client side - verify message data if appropriate.
                    if ( mClientSide )
                    {
                        verifyReceivedMessage();
                    }

                    // Re-issue the original read.
                    mReadPos = mHttpMessage.mHeaderLen + mChunkHeaderLen;
                    read(mOrigReadBuffer, mOrigBytesRequested);
                }
            }
        }
    }

    bool HttpFrameFilter::verifyReceivedMessage()
    {
        bool verified = false;
        try
        {
            ByteBuffer messageData(ByteBuffer(mReadBufferPtr), mHttpMessage.mHeaderLen + mChunkHeaderLen, mHttpMessage.mContentLen);

            // Verify message data if we have a verifier.
            ClientStub* pStub = RCF::getTlsClientStubPtr();
            RCF::RcfSession* pSession = getCurrentRcfSessionPtr();
            HttpMessageVerifierPtr verifierPtr;
            if ( pStub )
            {
                verifierPtr = pStub->getHttpMessageVerifier();
            }
            else if ( pSession )
            {
                verifierPtr = pSession->getRcfServer().getHttpMessageVerifier();
            }
            if ( verifierPtr )
            {
                verifierPtr->verifyHeader(mHttpMessage, messageData);
            }

            verified = true;
        }
        catch ( const std::exception & e )
        {
            verified = false;
            std::string msg = e.what();

            RCF_LOG_1() << Exception(RcfError_HttpMessageVerificationAdmin, msg).getErrorMessage();
            onError(Exception(RcfError_HttpMessageVerification));

        }

        return verified;
    }

    std::size_t HttpFrameFilter::getFrameSize()
    {
        return mHttpMessage.mContentLen;
    }

    const std::string & HttpFrameFilter::getHttpSessionId()
    {
        return mHttpSessionId;
    }

    std::uint32_t HttpFrameFilter::getHttpSessionIndex()
    {
        return mHttpSessionIndex;
    }

    const std::string & HttpFrameFilter::getConnectionHeader()
    {
        return mConnectionHeader;
    }

    const std::string & HttpFrameFilter::getServerHeader()
    {
        if ( mServerHeaderValue.empty() )
        {
            mServerHeaderValue = RCF::getCurrentRcfSession().getRcfServer().getHttpServerHeader();
            if ( mServerHeaderValue.empty() )
            {
                mServerHeaderValue = std::string("Remote Call Framework/") + RCF_VERSION_STR_BASE;
            }
        }
        return mServerHeaderValue;
    }

    void HttpFrameFilter::getHttpFrameInfo(
        std::string&                                            requestLine,
        std::vector< std::pair<std::string, std::string> >&     headers)
    {
        requestLine = mHttpMessage.mRequestLine;
        for ( auto header : mHttpMessage.mHeaderList )
        {
            if ( header.first.size() > 0 )
            {
                headers.push_back(header);
            }
        }
    }

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4996 )  // warning C4996: 'ctime' was declared deprecated
#endif

    void HttpFrameFilter::write(const std::vector<ByteBuffer> & byteBuffers)
    {
        mOrigReadBuffer = ByteBuffer();
        mOrigBytesRequested = 0;

        mWriteBuffers = byteBuffers;
        mWritePos = 0;

        unsigned int messageLength = static_cast<unsigned int>(
            lengthByteBuffers(byteBuffers));

        mOsPtr->rewind();

        // Check if a message verifier is in place.
        ClientStub* pStub = RCF::getTlsClientStubPtr();
        RCF::RcfSession* pSession = getCurrentRcfSessionPtr();
        std::string verifierHeaderName;
        std::string verifierHeaderValue;
        HttpMessageVerifierPtr verifierPtr;
        if ( pStub )
        {
            verifierPtr = pStub->getHttpMessageVerifier();
        }
        else if ( pSession )
        {
            verifierPtr = pSession->getRcfServer().getHttpMessageVerifier();
        }
        if ( verifierPtr )
        {
            verifierPtr->applyHeader(mWriteBuffers, verifierHeaderName, verifierHeaderValue);
        }
        
        if (mServerAddr.size() > 0)
        {
            // Client-side request.

            // This needs to work whether or not we are going through a proxy.

            RCF_ASSERT(pStub);

            ++mHttpSessionIndex;

            std::string parameterString;
            if ( pStub )
            {
                parameterString = pStub->getHttpUrlParameterString();
                if ( parameterString.size() > 0 && parameterString.front() != '?' )
                {
                    parameterString = "?" + parameterString;
                }
            }

            *mOsPtr <<
                "POST " << mServerUrlPath << parameterString << " HTTP/1.1\r\n"
                "Host: " << mServerAddr << ":" << mServerPort << "\r\n"
                "Accept: */*\r\n"
                "Connection: Keep-Alive\r\n"
                //"Connection: close\r\n"
                "X-RCFSessionId: " << mHttpSessionId << "\r\n"
                "X-RCFSessionIndex: " << mHttpSessionIndex << "\r\n";

            if ( verifierHeaderName.size() > 0 )
            {
                *mOsPtr << verifierHeaderName << ": " << verifierHeaderValue << "\r\n";
            }

            // Feed in any cookies we have received from the server.
            
            if ( pStub )
            {
                std::map<std::string, HttpCookie> & cookieMap = pStub->getCookieMap();
                for ( const auto & cookieEntry : cookieMap )
                {
                    *mOsPtr << "Cookie: " << cookieEntry.first << "=" << cookieEntry.second.mValue << "\r\n";
                }
            }

            *mOsPtr 
                << "Content-Length: " << messageLength << "\r\n"
                << "\r\n";
        }
        else
        {
            // Server-side response.

            char dateHeader[1000] = {};
            makeHttpDateHeaderValue(dateHeader, sizeof(dateHeader));

            if ( mChunkedResponseMode && mChunkedResponseCounter == 0 )
            {
                *mOsPtr 
                    << "HTTP/1.1 200 OK\r\n"
                    << "X-RCFSessionId: " << mHttpSessionId << "\r\n"
                    << "X-RCFSessionIndex: " << mHttpSessionIndex << "\r\n"
                    << "Server: " << getServerHeader() << "\r\n"
                    << "Date: " << dateHeader << "\r\n"
                    << "Connection: Keep-Alive\r\n"
                    << "Transfer-Encoding: chunked" << "\r\n"
                    //<< "Connection: close\r\n"
                    << "\r\n";

                char buf[255] = { 0 };
                sprintf(buf, "%x", messageLength);
                *mOsPtr << buf << "\r\n";
            }
            else if ( mChunkedResponseMode && mChunkedResponseCounter > 0 )
            {
                char buf[255] = { 0 };
                sprintf(buf, "%x", messageLength);
                *mOsPtr << buf << "\r\n";
            }
            else
            {
                *mOsPtr
                    << "HTTP/1.1 200 OK\r\n"
                    << "X-RCFSessionId: " << mHttpSessionId << "\r\n"
                    << "X-RCFSessionIndex: " << mHttpSessionIndex << "\r\n";

                if ( verifierHeaderName.size() > 0 )
                {
                    *mOsPtr << verifierHeaderName << ": " << verifierHeaderValue << "\r\n";
                }

                *mOsPtr
                    << "Server: " << getServerHeader() << "\r\n"
                    << "Date: " << dateHeader << "\r\n"
                    << "Content-Length: " << messageLength << "\r\n"
                    << "Connection: Keep-Alive\r\n"
                    << "\r\n";
            }
        }   

        ByteBuffer httpMessageHeaderBuf(mOsPtr);
        mWriteBuffers.insert(mWriteBuffers.begin(), httpMessageHeaderBuf);
        if ( mChunkedResponseMode )
        {
            mWriteBuffers.push_back( ByteBuffer( (char*) CrLf.c_str(), 2, true) );
            ++mChunkedResponseCounter;
        }

        if ( RCF::LogManager::instance().isEnabled(LogNameRcf, LogLevel_3) )
        {
            std::string httpMessageHeader(httpMessageHeaderBuf.getPtr(), httpMessageHeaderBuf.getLength());
            std::size_t frameLen = lengthByteBuffers(mWriteBuffers);
            if ( mChunkedResponseMode )
            {
                RCF_LOG_3()(this)(mHttpSessionId)(mHttpSessionIndex)(frameLen)("\n" + httpMessageHeader) << "Sending chunked HTTP message.";
            }
            else
            {
                RCF_LOG_3()(this)(frameLen)("\n" + httpMessageHeader) << "Sending HTTP message.";
            }
        }

        mpPostFilter->write(mWriteBuffers);
    }

#ifdef _MSC_VER
#pragma warning( pop )
#endif

    void HttpFrameFilter::onWriteCompleted(std::size_t bytesTransferred)
    {
        mWritePos += bytesTransferred;
        RCF_ASSERT(mWritePos <= lengthByteBuffers(mWriteBuffers));

        if (mWritePos < lengthByteBuffers(mWriteBuffers))
        {
            std::vector<ByteBuffer> slicedBuffers;
            sliceByteBuffers(slicedBuffers, mWriteBuffers, mWritePos);
            mpPostFilter->write(slicedBuffers);
        }
        else
        {
            std::size_t bytesWritten = mWritePos - mWriteBuffers.front().getLength();
            if ( mChunkedResponseMode )
            {
                // Take chunk trailer '\r\n' into account.
                bytesWritten -= mWriteBuffers.back().getLength();
            }
            if ( bytesWritten )
            {
                mWriteBuffers.clear();
                mpPreFilter->onWriteCompleted(bytesWritten);
            }
            else
            {
                // Zero bytes written indicates we sent a HTTP frame level error back to the client.
                // We do  nothing here, which will close the connection.
            }
        }
    }

    int HttpFrameFilter::getFilterId() const
    {
        return RcfFilter_Unknown;
    }   

    void HttpFrameFilter::sendHttpErrorResponse(MemOstreamPtr osPtr)
    {
        RcfSession * pSession = getCurrentRcfSessionPtr();
        if ( pSession )
        {
            NetworkSession& nwSession = pSession->getNetworkSession();
            AsioNetworkSession& asioNwSession = static_cast<AsioNetworkSession&>(nwSession);
            asioNwSession.setCloseAfterWrite();
        }

        ByteBuffer buffer(osPtr);
        mWriteBuffers.clear();
        mWriteBuffers.push_back(buffer);
        mWritePos = 0;
        mpPostFilter->write(mWriteBuffers);
    }

    void HttpFrameFilter::onError(const Exception& e)
    {
        if ( mClientSide )
        {
            RCF_THROW(e);
        }
        else
        {
            std::string msg = e.getErrorMessage();

            MemOstreamPtr osPtr(new MemOstream());

            *osPtr
                << "HTTP/1.1 400 Bad Request\r\n"
                << "Content-Length: " << msg.length() << "\r\n"
                << "Content-Type: text/plain\r\n"
                << "Server: " << getServerHeader() << "\r\n"
                << "X-RCFError: " << e.getErrorMessage() << "\r\n"
                << "\r\n"
                << e.getErrorMessage();

            sendHttpErrorResponse(osPtr);
        }
    }

} // namespace RCF
