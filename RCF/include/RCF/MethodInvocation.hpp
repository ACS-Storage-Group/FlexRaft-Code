
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

#ifndef INCLUDE_RCF_METHODINVOCATION_HPP
#define INCLUDE_RCF_METHODINVOCATION_HPP

#include <string>
#include <vector>

#include <memory>

#include <RCF/ByteBuffer.hpp>
#include <RCF/Export.hpp>
#include <RCF/Exception.hpp>
#include <RCF/SerializationProtocol_Base.hpp>

namespace RCF {

    class RcfServer;
    class ClientStub;
    class RcfSession;
    typedef std::shared_ptr<RcfSession> RcfSessionPtr;
    class SerializationProtocolIn;
    class SerializationProtocolOut;

    class MethodInvocationResponse;
    class MethodInvocationRequest;

    class ByteBuffer;
    class Filter;
    typedef std::shared_ptr<Filter> FilterPtr;

    class I_RcfClient;
    typedef std::shared_ptr<I_RcfClient> RcfClientPtr;

    // message types
    static const int Descriptor_Error               = 0;
    static const int Descriptor_Request             = 1;
    static const int Descriptor_Response            = 2;
    static const int Descriptor_FilteredPayload     = 3;

    void encodeServerError(RcfServer & server, ByteBuffer & byteBuffer, int error);
    void encodeServerError(RcfServer & server, ByteBuffer & byteBuffer, int error, int arg0, int arg1);

    class Protobufs;

    /// Contains details about the currently executing remote call.
    class RemoteCallInfo
    {
    public:

        RemoteCallInfo(const MethodInvocationRequest & req);

        /// Servant binding name.
        std::string             mServantBindingName;

        /// RCF interface method ID.
        int                     mFnId;

        /// True if the call is a oneway call.
        bool                    mOneway;

        /// Serialization protocol.
        SerializationProtocol   mSerializationProtocol;

        /// Runtime version.
        int                     mRuntimeVersion;

        /// Archive version.
        int                     mArchiveVersion;

        /// Pingback interval in ms.
        std::uint32_t           mPingBackIntervalMs;

        /// Whether SF pointer tracking is enabled for serialization of parameters.
        bool                    mEnableSfPointerTracking;
    };

    class RCF_EXPORT MethodInvocationRequest : Noncopyable
    {
    public:
        MethodInvocationRequest();

        void            init(                            
                            int                             runtimeVersion);

        void            init(
                            const MethodInvocationRequest & rhs);

        void            init(
                            const std::string &             service,
                            int                             fnId,
                            SerializationProtocol           serializationProtocol,
                            bool                            oneway,
                            bool                            close,
                            int                             runtimeVersion,
                            bool                            ignoreRuntimeVersion,
                            std::uint32_t                   pingBackIntervalMs,
                            int                             archiveVersion,
                            bool                            enableSfPointerTracking,
                            bool                            enableNativeWstringSerialization);

        int             getFnId() const;
        bool            getOneway() const;
        bool            getClose() const;
        const std::string & getService() const;
        void            setService(const std::string &service);
        int             getPingBackIntervalMs();

        ByteBuffer      encodeRequestHeader();

        void            encodeRequest(
                            const std::vector<ByteBuffer> & buffers,
                            std::vector<ByteBuffer> &       message,
                            const std::vector<FilterPtr> &  filters);

        bool            decodeRequest(
                            const ByteBuffer &              message,
                            ByteBuffer &                    messageBody,
                            RcfSessionPtr                   rcfSessionPtr,
                            RcfServer &                     rcfServer);

        bool            encodeResponse(
                            const RemoteException *         pRe,
                            ByteBuffer &                    buffer,
                            bool                            enableSfPointerTracking);

        void            decodeResponse(
                            const ByteBuffer &              message,
                            ByteBuffer &                    buffer,
                            MethodInvocationResponse &      response,
                            const std::vector<FilterPtr> &  filters);

        RcfClientPtr    locateStubEntryPtr(
                            RcfServer &                     rcfServer);

    private:

        friend class RcfSession;
        friend class ClientStub;
        friend class RemoteCallInfo;

        void            decodeFromMessage(
                            const ByteBuffer &              message,
                            ByteBuffer &                    buffer,
                            RcfServer *                     pRcfServer,
                            RcfSessionPtr                   rcfSessionPtr,
                            const std::vector<FilterPtr> &  existingFilters);

        void            encodeToMessage(
                            std::vector<ByteBuffer> &       message,
                            const std::vector<ByteBuffer> & buffers,
                            const std::vector<FilterPtr> &  filters);

        std::string             mService;
        int                     mFnId;
        SerializationProtocol   mSerializationProtocol;
        bool                    mOneway;
        bool                    mClose;
        std::uint32_t           mRuntimeVersion;
        bool                    mIgnoreRuntimeVersion; // Legacy field, no longer used.
        int                     mPingBackIntervalMs;
        std::uint32_t           mArchiveVersion;
        ByteBuffer              mRequestUserData;
        ByteBuffer              mResponseUserData;
        bool                    mEnableSfPointerTracking;
        bool                    mEnableNativeWstringSerialization = false;
        ByteBuffer              mOutOfBandRequest;
        ByteBuffer              mOutOfBandResponse;

        std::shared_ptr<std::vector<char> >   mVecPtr;
        

        friend RCF::MemOstream& operator<<(RCF::MemOstream& os, const MethodInvocationRequest& r);
    };

    class RCF_EXPORT MethodInvocationResponse
    {
    public:
        MethodInvocationResponse();

        bool    isException() const;
        bool    isError() const;
        int     getError() const;
        int     getArg0() const;
        int     getArg1() const;
        bool    getEnableSfPointerTracking() const;

        std::unique_ptr<RemoteException> getExceptionPtr();

    private:
        friend class MethodInvocationRequest;
        

        typedef std::unique_ptr<RemoteException> RemoteExceptionPtr;

        bool                mException;
        RemoteExceptionPtr  mExceptionPtr;
        bool                mError;
        int                 mErrorCode;
        int                 mArg0;
        int                 mArg1;
        bool                mEnableSfPointerTracking;

        friend RCF::MemOstream& operator<<(RCF::MemOstream& os, const MethodInvocationResponse& r);
    };

    // Out of band messages

    enum OobMessageType
    {
        Omt_RequestTransportFilters = 1,
        Omt_CreateCallbackConnection = 2,
        Omt_RequestSubscription = 3,
        Omt_RequestProxyConnection = 4,
    };

    class OobMessage;
    typedef std::shared_ptr<OobMessage> OobMessagePtr;
    typedef std::shared_ptr< std::vector<char> > VecPtr;

    class RCF_EXPORT OobMessage
    {
    public:

        OobMessage(int runtimeVersion);
        virtual ~OobMessage();

        virtual OobMessageType  getMessageType() = 0;

        virtual void            encodeRequest(ByteBuffer & buffer) = 0;
        virtual void            decodeRequest(const ByteBuffer & buffer, std::size_t & pos) = 0;

        virtual void            encodeResponse(ByteBuffer & buffer);
        virtual void            decodeResponse(const ByteBuffer & buffer);

    protected:
        void                    encodeRequestCommon(VecPtr vecPtr, std::size_t & pos);

    public:
        static OobMessagePtr    decodeRequestCommon(const ByteBuffer & buffer);

    protected:
        void                    encodeResponseCommon(VecPtr vecPtr, std::size_t & pos);
        void                    decodeResponseCommon(const ByteBuffer & buffer, std::size_t & pos);

        int                     mRuntimeVersion;

    public:

        // Common return values.
        std::uint32_t           mResponseError;
        std::string             mResponseErrorString;
    };

    class RCF_EXPORT OobRequestTransportFilters : public OobMessage
    {
    public:
        OobRequestTransportFilters(int runtimeVersion);

        OobRequestTransportFilters(
            int runtimeVersion, 
            const std::vector<FilterPtr> &filters);

        virtual OobMessageType  getMessageType();
        virtual void            encodeRequest(ByteBuffer & buffer);
        virtual void            decodeRequest(const ByteBuffer & buffer, std::size_t & pos);

        std::vector<std::int32_t>        mFilterIds;
    };

    class RCF_EXPORT OobCreateCallbackConnection : public OobMessage
    {
    public:
        OobCreateCallbackConnection(int runtimeVersion);

        virtual OobMessageType  getMessageType();
        virtual void            encodeRequest(ByteBuffer & buffer);
        virtual void            decodeRequest(const ByteBuffer & buffer, std::size_t & pos);
    };

    class RCF_EXPORT OobRequestSubscription : public OobMessage
    {
    public:
        OobRequestSubscription(int runtimeVersion);

        OobRequestSubscription(
            int                     runtimeVersion, 
            const std::string &     publisherName, 
            std::uint32_t           subToPubPingIntervalMs);

        virtual OobMessageType  getMessageType();
        virtual void            encodeRequest(ByteBuffer & buffer);
        virtual void            decodeRequest(const ByteBuffer & buffer, std::size_t & pos);
        virtual void            encodeResponse(ByteBuffer & buffer);
        virtual void            decodeResponse(const ByteBuffer & buffer);


        std::string             mPublisherName;
        std::uint32_t           mSubToPubPingIntervalMs;
        std::uint32_t           mPubToSubPingIntervalMs;
    };

    class RCF_EXPORT OobRequestProxyConnection : public OobMessage
    {
    public:
        OobRequestProxyConnection(int runtimeVersion);

        OobRequestProxyConnection(
            int                     runtimeVersion,
            const std::string &     proxyEndpointName);

        virtual OobMessageType  getMessageType();
        virtual void            encodeRequest(ByteBuffer & buffer);
        virtual void            decodeRequest(const ByteBuffer & buffer, std::size_t & pos);
        virtual void            encodeResponse(ByteBuffer & buffer);
        virtual void            decodeResponse(const ByteBuffer & buffer);

        std::string             mProxyEndpointName;
    };


} // namespace RCF

#endif // ! INCLUDE_RCF_METHODINVOCATION_HPP
