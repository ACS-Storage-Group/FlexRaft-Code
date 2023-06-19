
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

#ifndef INCLUDE_RCF_SUBSCRIPTIONSERVICE_HPP
#define INCLUDE_RCF_SUBSCRIPTIONSERVICE_HPP

#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include <RCF/ClientStub.hpp>
#include <RCF/Export.hpp>
#include <RCF/RcfClient.hpp>
#include <RCF/RcfFwd.hpp>
#include <RCF/PeriodicTimer.hpp>
#include <RCF/ServerStub.hpp>
#include <RCF/Service.hpp>

namespace RCF {

    /// Represents a subscription to a RCF publisher. To create a subscription, use RcfServer::createSubscription().
    class RCF_EXPORT Subscription : Noncopyable
    {
    private:

        Subscription(
            SubscriptionService &       subscriptionService,
            ClientTransportUniquePtr    clientTransportUniquePtr,
            RcfSessionWeakPtr           rcfSessionWeakPtr,
            std::uint32_t               incomingPingIntervalMs,
            const std::string &         publisherUrl,
            const std::string &         topic,
            OnSubscriptionDisconnect    onDisconnect);

        void setWeakThisPtr(SubscriptionWeakPtr thisWeakPtr);

    public:

        ~Subscription();

        unsigned int    getPingTimestamp();
        RcfSessionPtr   getRcfSessionPtr();

        /// Checks if the subscriber has a valid network connection handle to the publisher.
        bool            isConnected();

        /// Closes the subscription and disconnects from the publisher.
        void            close();

    private:
        friend class SubscriptionService;

        static void     onDisconnect(SubscriptionWeakPtr subPtr, RcfSession & session);

        SubscriptionService &       mSubscriptionService;
        SubscriptionWeakPtr         mThisWeakPtr;

        RecursiveMutex              mMutex;
        RcfSessionWeakPtr           mRcfSessionWeakPtr;
        
        RcfClientPtr                mConnectionPtr;

        std::uint32_t               mPubToSubPingIntervalMs = 0;
        bool                        mPublisherSupportsPubSubPings = true;
        std::string                 mPublisherUrl;
        std::string                 mTopic;

        OnSubscriptionDisconnect    mOnDisconnect;
        bool                        mClosed = false;
    };

    /// General configuration of a subscription.
    class RCF_EXPORT SubscriptionParms
    {
    public:
        SubscriptionParms();

        /// Sets the topic name of the subscription. The default topic name is the name of the RCF interface of the subscription.
        void        setTopicName(const std::string & publisherName);

        /// Gets the topic name of the subscription. The default topic name is the name of the RCF interface of the subscription.
        std::string getTopicName() const;

        /// Sets the network endpoint of the publishing server.
        void        setPublisherEndpoint(const Endpoint & publisherEp);

        /// Sets the network endpoint of the publishing server.
        void        setPublisherEndpoint(I_RcfClient & rcfClient);

        /// Configures a callback to be called whenever the subscriber is disconnected from the publisher.
        void        setOnSubscriptionDisconnect(OnSubscriptionDisconnect onSubscriptionDisconnect);

        /// Configures a callback to be called when an asynchronous subscription connection is established.
        void        setOnAsyncSubscribeCompleted(OnAsyncSubscribeCompleted onAsyncSubscribeCompleted);

    private:

        friend class SubscriptionService;

        std::string                             mPublisherName;
        ClientStub                              mClientStub;
        OnSubscriptionDisconnect                mOnDisconnect;
        OnAsyncSubscribeCompleted               mOnAsyncSubscribeCompleted;
    };

    class RCF_EXPORT SubscriptionService :
        public I_Service,
        Noncopyable
    {
    public:

        SubscriptionService(std::uint32_t pingIntervalMs = 0);

        ~SubscriptionService();

        template<typename Interface, typename T>
        SubscriptionPtr createSubscription(
            T & t, 
            const SubscriptionParms & parms)
        {
            std::string defaultPublisherName = getInterfaceName((Interface *) NULL);

            std::reference_wrapper<T> refWrapper(t);

            RcfClientPtr rcfClientPtr(
                createServerStub((Interface *) 0, (T *) 0, refWrapper));

            return createSubscriptionImpl(rcfClientPtr, parms, defaultPublisherName);
        }

        template<typename Interface, typename T>
        SubscriptionPtr createSubscription(
            T & t, 
            const RCF::Endpoint & publisherEp)
        {
            SubscriptionParms parms;
            parms.setPublisherEndpoint(publisherEp);
            return createSubscription<Interface>(t, parms);
        }

        SubscriptionPtr createSubscriptionImpl(
            RcfClientPtr rcfClientPtr, 
            const SubscriptionParms & parms,
            const std::string & defaultPublisherName);

        void createSubscriptionImplBegin(
            RcfClientPtr rcfClientPtr, 
            const SubscriptionParms & parms,
            const std::string & defaultPublisherName);

        void createSubscriptionImplEnd(
            ExceptionPtr                            ePtr,
            ClientStubPtr                           clientStubPtr,
            std::int32_t                          ret,
            const std::string &                     publisherName,
            RcfClientPtr                            rcfClientPtr,
            OnSubscriptionDisconnect                onDisconnect,
            OnAsyncSubscribeCompleted               onCompletion,
            std::uint32_t                         pubToSubPingIntervalMs,
            bool                                    pingsEnabled);

        void closeSubscription(SubscriptionWeakPtr subscriptionPtr);

        void setPingIntervalMs(std::uint32_t pingIntervalMs);
        std::uint32_t getPingIntervalMs() const;
        
    private:

        void onServerStart(RcfServer &server);
        void onServerStop(RcfServer &server);

        RcfServer *                     mpServer;
        Mutex                           mSubscriptionsMutex;

        typedef std::set<SubscriptionWeakPtr> Subscriptions;
        Subscriptions                   mSubscriptions;

        std::uint32_t                   mPingIntervalMs;
        PeriodicTimer                   mPeriodicTimer;

        virtual void onTimer();
        void pingAllSubscriptions();
        void harvestExpiredSubscriptions();

        static void sOnPingCompleted(RecursiveLockPtr lockPtr);

    public:

        SubscriptionPtr onRequestSubscriptionCompleted(
            std::int32_t                            ret,
            const std::string &                     publisherName,
            ClientStub &                            clientStub,
            RcfClientPtr                            rcfClientPtr,
            OnSubscriptionDisconnect                onDisconnect,
            std::uint32_t                           pubToSubPingIntervalMs,
            bool                                    pingsEnabled);

    private:

        std::int32_t doRequestSubscription(
            ClientStub &                    clientStubOrig, 
            const std::string &             publisherName,
            std::uint32_t                   subToPubPingIntervalMs, 
            std::uint32_t &                 pubToSubPingIntervalMs,
            bool &                          pingsEnabled);

        void doRequestSubscriptionAsync(
            ClientStub &                    clientStubOrig, 
            const std::string &             publisherName,
            RcfClientPtr                    rcfClientPtr,
            const SubscriptionParms &       parms);

        void doRequestSubscriptionAsync_Complete(
            Future<Void>                    fv,
            RcfClientPtr                    requestClientPtr,
            const std::string &             publisherName,
            RcfClientPtr                    rcfClientPtr,
            OnSubscriptionDisconnect        onDisconnect,
            OnAsyncSubscribeCompleted       onCompletion);

        // Legacy subscription requests.

        std::int32_t doRequestSubscription_Legacy(
            ClientStub &                    clientStubOrig,
            const std::string &             publisherName,
            std::uint32_t                   subToPubPingIntervalMs,
            std::uint32_t &                 pubToSubPingIntervalMs,
            bool &                          pingsEnabled);

        void doRequestSubscriptionAsync_Legacy(
            ClientStub &                    clientStubOrig,
            const std::string &             publisherName,
            RcfClientPtr                    rcfClientPtr,
            const SubscriptionParms &       parms);

        void doRequestSubscriptionAsync_Legacy_Complete(
            ClientStubPtr                   clientStubPtr,
            Future<std::int32_t>            fRet,
            const std::string &             publisherName,
            RcfClientPtr                    rcfClientPtr,
            OnSubscriptionDisconnect        onDisconnect,
            OnAsyncSubscribeCompleted       onCompletion,
            Future<std::uint32_t>           pubToSubPingIntervalMs,
            bool                            pingsEnabled);


    };

    typedef std::shared_ptr<SubscriptionService> SubscriptionServicePtr;

} // namespace RCF

#endif // ! INCLUDE_RCF_SUBSCRIPTIONSERVICE_HPP
