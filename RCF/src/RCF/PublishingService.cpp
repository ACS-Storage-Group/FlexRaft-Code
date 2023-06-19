
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

#include <RCF/PublishingService.hpp>

#include <RCF/AsioServerTransport.hpp>
#include <RCF/ConnectedClientTransport.hpp>
#include <RCF/CurrentSession.hpp>
#include <RCF/MulticastClientTransport.hpp>
#include <RCF/RcfServer.hpp>
#include <RCF/RcfSession.hpp>
#include <RCF/ServerTransport.hpp>
#include <RCF/ThreadLibrary.hpp>
#include <RCF/ThreadLocalData.hpp>

namespace RCF {

    void PublisherParms::setTopicName(const std::string & topicName)
    {
        mTopicName = topicName;
    }

    std::string PublisherParms::getTopicName() const
    {
        return mTopicName;
    }

    void PublisherParms::setOnSubscriberConnect(OnSubscriberConnect onSubscriberConnect)
    {
        mOnSubscriberConnect = onSubscriberConnect;
    }

    void PublisherParms::setOnSubscriberDisconnect(OnSubscriberDisconnect onSubscriberDisconnect)
    {
        mOnSubscriberDisconnect = onSubscriberDisconnect;
    }
    
#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4355 ) // warning C4355: 'this' : used in base member initializer list
#endif

    PublishingService::PublishingService() :
        mPingIntervalMs(0),
        mPeriodicTimer(*this, 0)
    {}

#ifdef _MSC_VER
#pragma warning( pop )
#endif

    PublishingService::~PublishingService()
    {
    }

    void PublishingService::setPingIntervalMs(std::uint32_t pingIntervalMs)
    {
        mPingIntervalMs = pingIntervalMs;
    }

    std::uint32_t PublishingService::getPingIntervalMs() const
    {
        return mPingIntervalMs;
    }

    // remotely accessible

    std::int32_t PublishingService::RequestSubscription(
        const std::string &subscriptionName)
    {
        std::uint32_t subToPubPingIntervalMs = 0;
        std::uint32_t pubToSubPingIntervalMs = 0;

        return RequestSubscription(
            subscriptionName, 
            subToPubPingIntervalMs, 
            pubToSubPingIntervalMs);
    }

    std::int32_t PublishingService::RequestSubscription(
        const std::string &subscriptionName,
        std::uint32_t subToPubPingIntervalMs,
        std::uint32_t & pubToSubPingIntervalMs)
    {
        PublisherPtr publisherPtr;
        std::string publisherName = subscriptionName;
        Lock lock(mPublishersMutex);
        Publishers::iterator iter = mPublishers.find(publisherName);
        if (iter != mPublishers.end())
        {
            PublisherWeakPtr publisherWeakPtr = iter->second;
            publisherPtr = publisherWeakPtr.lock();
        }
        lock.unlock();
        if (publisherPtr)
        {
            RcfSession & rcfSession = getTlsRcfSession();

            if (publisherPtr->mParms.mOnSubscriberConnect)
            {
                bool allowSubscriber = publisherPtr->mParms.mOnSubscriberConnect(rcfSession, subscriptionName);
                if (!allowSubscriber)
                {
                    return RcfError_AccessDenied_Id;
                }
            }

            rcfSession.setPingIntervalMs(subToPubPingIntervalMs);

            ServerTransportEx &serverTransport = dynamic_cast<ServerTransportEx &>(
                rcfSession.getNetworkSession().getServerTransport());

            ClientTransportUniquePtrPtr clientTransportUniquePtrPtr( new ClientTransportUniquePtr(
                serverTransport.createClientTransport( rcfSession.shared_from_this() )));

            (*clientTransportUniquePtrPtr)->setRcfSession(
                rcfSession.shared_from_this());

            if ( publisherPtr->mParms.mOnSubscriberDisconnect )
            {
                rcfSession.setOnDestroyCallback( std::bind(
                    publisherPtr->mParms.mOnSubscriberDisconnect,
                    std::placeholders::_1,
                    publisherName));
            }

            rcfSession.setPingTimestamp();

            rcfSession.addOnWriteCompletedCallback( std::bind(
                &PublishingService::addSubscriberTransport,
                this,
                std::placeholders::_1,
                publisherName,
                clientTransportUniquePtrPtr) );
        }  
        pubToSubPingIntervalMs = mPingIntervalMs;
        return publisherPtr ? RcfError_Ok_Id : RcfError_UnknownPublisher_Id;
    }

    void PublishingService::onServiceAdded(RcfServer & server)
    {
        RCF_UNUSED_VARIABLE(server);
    }

    void PublishingService::onServiceRemoved(RcfServer &server)
    {
        RCF_UNUSED_VARIABLE(server);
    }

    void PublishingService::onServerStart(RcfServer &server)
    {
        RCF_UNUSED_VARIABLE(server);
        mPeriodicTimer.setIntervalMs(mPingIntervalMs);
        mPeriodicTimer.start();
    }

    void PublishingService::onServerStop(RcfServer &server)
    {
        RCF_UNUSED_VARIABLE(server);
        mPeriodicTimer.stop();

        // Close all publishers.

        Publishers publishers;
        {
            Lock writeLock(mPublishersMutex);
            publishers = mPublishers;
        }

        Publishers::iterator iter;
        for (iter = publishers.begin(); iter != publishers.end(); ++iter)
        {
            PublisherPtr publisherPtr = iter->second.lock();
            if (publisherPtr)
            {
                publisherPtr->close();
            }
        }

        {
            Lock writeLock(mPublishersMutex);
            RCF_ASSERT(mPublishers.empty());
        }
    }

    void PublishingService::addSubscriberTransport(
        RcfSession &rcfSession,
        const std::string &publisherName,
        ClientTransportUniquePtrPtr clientTransportUniquePtrPtr)
    {
        PublisherPtr publisherPtr;

        {
            Lock lock(mPublishersMutex);
            if ( mPublishers.find(publisherName) != mPublishers.end() )
            {
                publisherPtr = mPublishers[publisherName].lock();
            }
        }

        if ( publisherPtr )
        {        
            AsioNetworkSession& networkSession = static_cast<AsioNetworkSession&>(rcfSession.getNetworkSession());

            // For now we assume the presence of wire filters indicates a HTTP/HTTPS connection.
            if ( networkSession.mWireFilters.size() > 0 )
            {
                // This doesn't actually close anything, it just takes the session out of the server IO loop.
                rcfSession.setCloseSessionAfterWrite(true);
                (*clientTransportUniquePtrPtr)->setRcfSession(RcfSessionWeakPtr());

                std::size_t wireFilterCount = networkSession.mWireFilters.size();
                RCF_ASSERT(wireFilterCount == 1 || wireFilterCount == 2);
                RCF_UNUSED_VARIABLE(wireFilterCount);

                ConnectedClientTransport& connClientTransport = static_cast<ConnectedClientTransport&>(**clientTransportUniquePtrPtr);
                connClientTransport.setWireFilters(networkSession.mWireFilters);
                networkSession.mWireFilters.clear();
                networkSession.setTransportFilters(std::vector<FilterPtr>());
            }

            MulticastClientTransport &multicastClientTransport = 
                static_cast<MulticastClientTransport &>(
                    publisherPtr->mRcfClientPtr->getClientStub().getTransport());

            multicastClientTransport.addTransport(std::move(*clientTransportUniquePtrPtr));
        }
    }

    void PublishingService::closePublisher(const std::string & name)
    {
        Lock writeLock(mPublishersMutex);
        Publishers::iterator iter = mPublishers.find(name);
        if (iter != mPublishers.end())
        {
            mPublishers.erase(iter);
        }
    }

    void PublishingService::onTimer()
    {
        pingAllSubscriptions();
        harvestExpiredSubscriptions();
    }

    void PublishingService::pingAllSubscriptions()
    {
        // Send one way pings on all our subscriptions, so the subscriber
        // knows we're still alive.

        std::vector<PublisherPtr> pubs;

        {
            Lock lock(mPublishersMutex);

            Publishers::iterator iter;
            for (iter = mPublishers.begin(); iter != mPublishers.end(); ++iter)
            {
                PublisherPtr publisherPtr = iter->second.lock();
                if (publisherPtr)
                {
                    pubs.push_back(publisherPtr);
                }
            }
        }

        for (std::size_t i=0; i<pubs.size(); ++i)
        {
            PublisherPtr publisherPtr = pubs[i];
            RCF_ASSERT(publisherPtr);

            ClientTransport & transport = 
                pubs[i]->mRcfClientPtr->getClientStub().getTransport();

            MulticastClientTransport & multiTransport = 
                static_cast<MulticastClientTransport &>(transport);

            multiTransport.pingAllTransports();
        }

        pubs.clear();
    }

    void PublishingService::harvestExpiredSubscriptions()
    {
        // Kill off subscriptions that haven't received any recent pings.

        std::vector<PublisherPtr> pubs;

        {
            Lock lock(mPublishersMutex);

            Publishers::iterator iter;
            for (iter = mPublishers.begin(); iter != mPublishers.end(); ++iter)
            {
                PublisherPtr publisherPtr = iter->second.lock();
                if (publisherPtr)
                {
                    pubs.push_back(publisherPtr);
                }
            }
        }

        for (std::size_t i=0; i<pubs.size(); ++i)
        {
            ClientTransport & transport = 
                pubs[i]->mRcfClientPtr->getClientStub().getTransport();

            MulticastClientTransport & multiTransport = 
                static_cast<MulticastClientTransport &>(transport);

            multiTransport.dropIdleTransports();
        }

        pubs.clear();
    }

    PublisherBase::PublisherBase(PublishingService & pubService, const PublisherParms & parms) : 
        mPublishingService(pubService),
        mParms(parms),
        mClosed(false)
    {
        mTopicName = parms.getTopicName();
    }

    PublisherBase::~PublisherBase()
    {
        if (!mClosed)
        {
            close();
        }
    }

    std::string PublisherBase::getTopicName()
    {
        return mTopicName;
    }

    std::size_t PublisherBase::getSubscriberCount()
    {
        ClientStub & stub = mRcfClientPtr->getClientStub();
        ClientTransport & transport = stub.getTransport();
        MulticastClientTransport & multiTransport = static_cast<MulticastClientTransport &>(transport);
        std::size_t transportCount = multiTransport.getTransportCount();
        return transportCount;
    }

    void PublisherBase::close()
    {
        mPublishingService.closePublisher(mTopicName);

        ClientTransport & transport = mRcfClientPtr->getClientStub().getTransport();

        MulticastClientTransport & multicastTransport = 
            static_cast<MulticastClientTransport &>(transport);
        
        multicastTransport.close();

        mRcfClientPtr.reset();

        mClosed = true;
    }

    void PublisherBase::init()
    {
        mRcfClientPtr->getClientStub().setTransport(
            ClientTransportUniquePtr(new MulticastClientTransport));

        mRcfClientPtr->getClientStub().setRemoteCallMode(Oneway);
        mRcfClientPtr->getClientStub().setServerBindingName("");
    }

} // namespace RCF
