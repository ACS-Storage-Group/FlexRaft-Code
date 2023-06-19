
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

#include <RCF/Config.hpp>
#include <RCF/SubscriptionService.hpp>

#if RCF_FEATURE_LEGACY==0

namespace RCF
{

    std::int32_t SubscriptionService::doRequestSubscription_Legacy(
        ClientStub &                    clientStubOrig,
        const std::string &             publisherName,
        std::uint32_t                 subToPubPingIntervalMs,
        std::uint32_t &               pubToSubPingIntervalMs,
        bool &                          pingsEnabled)
    {
        RCF_UNUSED_VARIABLE(clientStubOrig);
        RCF_UNUSED_VARIABLE(publisherName);
        RCF_UNUSED_VARIABLE(subToPubPingIntervalMs);
        RCF_UNUSED_VARIABLE(pubToSubPingIntervalMs);
        RCF_UNUSED_VARIABLE(pingsEnabled);

        RCF_THROW(Exception(
            RcfError_NotSupportedInThisBuild, "Legacy subscription request"));

        return RcfError_NotSupportedInThisBuild_Id;
    }

    void SubscriptionService::doRequestSubscriptionAsync_Legacy(
        ClientStub &                    clientStubOrig,
        const std::string &             publisherName,
        RcfClientPtr                    rcfClientPtr,
        const SubscriptionParms &       parms)
    {
        RCF_UNUSED_VARIABLE(clientStubOrig);
        RCF_UNUSED_VARIABLE(publisherName);
        RCF_UNUSED_VARIABLE(rcfClientPtr);
        RCF_UNUSED_VARIABLE(parms);

        RCF_THROW(Exception(
            RcfError_NotSupportedInThisBuild, "Legacy subscription request"));
    }

    void SubscriptionService::doRequestSubscriptionAsync_Legacy_Complete(
        ClientStubPtr                   clientStubPtr,
        Future<std::int32_t>          fRet,
        const std::string &             publisherName,
        RcfClientPtr                    rcfClientPtr,
        OnSubscriptionDisconnect        onDisconnect,
        OnAsyncSubscribeCompleted       onCompletion,
        Future<std::uint32_t>         pubToSubPingIntervalMs,
        bool                            pingsEnabled)
    {
        RCF_UNUSED_VARIABLE(clientStubPtr);
        RCF_UNUSED_VARIABLE(fRet);
        RCF_UNUSED_VARIABLE(publisherName);
        RCF_UNUSED_VARIABLE(rcfClientPtr);
        RCF_UNUSED_VARIABLE(onDisconnect);
        RCF_UNUSED_VARIABLE(onCompletion);
        RCF_UNUSED_VARIABLE(pubToSubPingIntervalMs);
        RCF_UNUSED_VARIABLE(pingsEnabled);

        RCF_THROW(Exception(
            RcfError_NotSupportedInThisBuild, "Legacy subscription request"));
    }

} // namespace RCF

#else

#include <RCF/Idl.hpp>

namespace RCF
{

    //--------------------------------------------------------------------------
    // I_RequestSubscription

    RCF_BEGIN( I_RequestSubscription, "" )

        RCF_METHOD_R1(
            std::int32_t, 
                RequestSubscription, 
                    const std::string &)

        RCF_METHOD_R3(
            std::int32_t,
                RequestSubscription, 
                    const std::string &,    // subscriber name
                    std::uint32_t,        // sub-to-pub ping interval
                    std::uint32_t &)      // pub-to-sub ping interval

    RCF_END(I_RequestSubscription)

    std::int32_t SubscriptionService::doRequestSubscription_Legacy(
        ClientStub &            clientStubOrig,
        const std::string & publisherName,
        std::uint32_t subToPubPingIntervalMs,
        std::uint32_t &       pubToSubPingIntervalMs,
        bool & pingsEnabled)
    {
        RcfClient<I_RequestSubscription> client(clientStubOrig);
        client.getClientStub().setTransport(clientStubOrig.releaseTransport());

        std::int32_t ret = 0;
        if ( clientStubOrig.getRuntimeVersion() < 8 )
        {
            pingsEnabled = false;

            ret = client.RequestSubscription(
                Twoway,
                publisherName);
        }
        else
        {
            pingsEnabled = true;

            ret = client.RequestSubscription(
                Twoway,
                publisherName,
                subToPubPingIntervalMs,
                pubToSubPingIntervalMs);
        }

        clientStubOrig.setTransport(client.getClientStub().releaseTransport());

        return ret;
    }

    void SubscriptionService::doRequestSubscriptionAsync_Legacy_Complete(
        ClientStubPtr                           clientStubPtr,
        Future<std::int32_t>                  fRet,
        const std::string &                     publisherName,
        RcfClientPtr                            rcfClientPtr,
        OnSubscriptionDisconnect                onDisconnect,
        OnAsyncSubscribeCompleted               onCompletion,
        Future<std::uint32_t>                 fPubToSubPingIntervalMs,
        bool                                    pingsEnabled)
    {
        std::uint32_t ret = 0;
        std::uint32_t pubToSubPingIntervalMs = 0;

        ExceptionPtr ePtr(clientStubPtr->getAsyncException().release());
        if ( !ePtr )
        {
            ret = *fRet;
            pubToSubPingIntervalMs = *fPubToSubPingIntervalMs;
        }

        createSubscriptionImplEnd(
            ePtr,
            clientStubPtr,
            ret,
            publisherName,
            rcfClientPtr,
            onDisconnect,
            onCompletion,
            pubToSubPingIntervalMs,
            pingsEnabled);
    }

    void SubscriptionService::doRequestSubscriptionAsync_Legacy(
        ClientStub &            clientStubOrig,
        const std::string &     publisherName,
        RcfClientPtr            rcfClientPtr,
        const SubscriptionParms & parms)
    {
        typedef RcfClient<I_RequestSubscription> AsyncClient;
        typedef std::shared_ptr<AsyncClient> AsyncClientPtr;

        AsyncClientPtr asyncClientPtr(new AsyncClient(clientStubOrig));

        asyncClientPtr->getClientStub().setTransport(
            clientStubOrig.releaseTransport());

        asyncClientPtr->getClientStub().setAsyncDispatcher(*mpServer);

        Future<std::int32_t>      ret;
        Future<std::uint32_t>     incomingPingIntervalMs;

        bool pingsEnabled = true;

        if ( clientStubOrig.getRuntimeVersion() < 8 )
        {
            pingsEnabled = false;

            ret = asyncClientPtr->RequestSubscription(

                AsyncTwoway(std::bind(
                    &SubscriptionService::doRequestSubscriptionAsync_Legacy_Complete,
                    this,
                    asyncClientPtr->getClientStubPtr(),
                    ret,
                    publisherName,
                    rcfClientPtr,
                    parms.mOnDisconnect,
                    parms.mOnAsyncSubscribeCompleted,
                    incomingPingIntervalMs,
                    pingsEnabled)),

                publisherName);
        }
        else
        {
            std::uint32_t outgoingPingIntervalMs = mPingIntervalMs;

            ret = asyncClientPtr->RequestSubscription(

                AsyncTwoway(std::bind(
                    &SubscriptionService::doRequestSubscriptionAsync_Legacy_Complete,
                    this,
                    asyncClientPtr->getClientStubPtr(),
                    ret,
                    publisherName,
                    rcfClientPtr,
                    parms.mOnDisconnect,
                    parms.mOnAsyncSubscribeCompleted,
                    incomingPingIntervalMs,
                    pingsEnabled)),

                publisherName,
                outgoingPingIntervalMs,
                incomingPingIntervalMs);
        }
    }

} // namespace RCF

#endif
