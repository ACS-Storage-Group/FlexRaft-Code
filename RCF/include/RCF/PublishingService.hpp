
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

#ifndef INCLUDE_RCF_PUBLISHINGSERVICE_HPP
#define INCLUDE_RCF_PUBLISHINGSERVICE_HPP

#include <functional>
#include <map>
#include <string>

#include <RCF/Export.hpp>
#include <RCF/PeriodicTimer.hpp>
#include <RCF/RcfClient.hpp>
#include <RCF/RcfFwd.hpp>
#include <RCF/Service.hpp>
#include <RCF/ThreadLibrary.hpp>
#include <RCF/Timer.hpp>
#include <RCF/Tools.hpp>

namespace RCF {

    /// General configuration of a publisher.
    class RCF_EXPORT PublisherParms
    {
    public:

        /// Sets the topic name of the publisher.
        void setTopicName(const std::string & topicName);

        /// Gets the topic name of the publisher.
        std::string getTopicName() const;

        /// Configures a callback to be called whenever a subscriber connects to this publisher.
        void setOnSubscriberConnect(OnSubscriberConnect onSubscriberConnect);

        /// Configures a callback to be called whenever a subscriber disconnects from this publisher.
        void setOnSubscriberDisconnect(OnSubscriberDisconnect onSubscriberDisconnect);

    private:

        friend class PublishingService;

        std::string             mTopicName;
        OnSubscriberConnect     mOnSubscriberConnect;
        OnSubscriberDisconnect  mOnSubscriberDisconnect;
    };

    /// Base class of all publishers.
    class RCF_EXPORT PublisherBase : Noncopyable
    {
    public:
        PublisherBase(PublishingService & pubService, const PublisherParms & parms);
        ~PublisherBase();

        /// Gets the topic name of the publisher. The default topic name is the name of the RCF interface of the publisher.
        std::string     getTopicName();

        /// Gets the number of subscribers currently connected.
        std::size_t     getSubscriberCount();

        /// Closes the publisher and disconnects any current subscribers.
        void            close();

    protected:

        friend class PublishingService;

        void init();

        PublishingService &     mPublishingService;
        PublisherParms          mParms;
        bool                    mClosed;
        std::string             mTopicName;
        RcfClientPtr            mRcfClientPtr;
    };

    /// Represents a single publisher within a RcfServer.  To create a publisher, use RcfServer::createPublisher().
    template<typename Interface>
    class Publisher : public PublisherBase
    {
    public:

        typedef typename Interface::RcfClientT RcfClientT;

        Publisher(PublishingService & pubService, const PublisherParms & parms) :
            PublisherBase(pubService, parms),
            mpClient(NULL)
        {
            if (mTopicName.empty())
            {
                mTopicName = getInterfaceName<Interface>();
            }

            mRcfClientPtr.reset( new RcfClientT( ClientStub(mTopicName) ) );
            mpClient = dynamic_cast<RcfClientT *>(mRcfClientPtr.get());

            init();
        }

        /// Returns a reference to the RcfClient<> instance to use when publishing messages.
        RcfClientT & publish()
        {
            RCF_ASSERT(!mClosed);
            return *mpClient;
        }

    private:

        RcfClientT * mpClient;
    };   

    class I_RequestSubscription;
    template<typename T> class RcfClient; 

    class RCF_EXPORT PublishingService :
        public I_Service,
        Noncopyable
    {
    public:
        PublishingService();

        ~PublishingService();

        template<typename Interface>
        std::shared_ptr< Publisher<Interface> > createPublisher(
            const PublisherParms & parms)
        {
            std::shared_ptr< Publisher<Interface> > publisherPtr(
                new Publisher<Interface>(*this, parms) );

            std::string topicName = publisherPtr->getTopicName();

            RCF_ASSERT(topicName.size() > 0);
            Lock lock(mPublishersMutex);
            mPublishers[topicName] = publisherPtr;
            return publisherPtr;
        }

        void setPingIntervalMs(std::uint32_t pingIntervalMs);
        std::uint32_t getPingIntervalMs() const;


    private:

        friend class RcfClient<I_RequestSubscription>;
        friend class PublisherBase;
        friend class PublishingServicePb;
        friend class RcfSession;

        std::int32_t  RequestSubscription(
                            const std::string &subscriptionName);

        std::int32_t  RequestSubscription(
                            const std::string &subscriptionName,
                            std::uint32_t subToPubPingIntervalMs,
                            std::uint32_t & pubToSubPingIntervalMs);

    private:

        void            onServiceAdded(RcfServer &server);
        void            onServiceRemoved(RcfServer &server);
        void            onServerStart(RcfServer &server);
        void            onServerStop(RcfServer &server);

        void            addSubscriberTransport(
                            RcfSession &session,
                            const std::string &publisherName,
                            ClientTransportUniquePtrPtr clientTransportUniquePtrPtr);

        void            closePublisher(const std::string & name);

        // TODO: should be weak_ptr here probably. ~Publisher() will need
        // to clean them up ?
        typedef std::map<std::string, PublisherWeakPtr>     Publishers;

        Mutex                           mPublishersMutex;
        Publishers                      mPublishers;

        std::uint32_t                 mPingIntervalMs;
        PeriodicTimer                   mPeriodicTimer;

        virtual void onTimer();
        void pingAllSubscriptions();
        void harvestExpiredSubscriptions();

    };

    typedef std::shared_ptr<PublishingService> PublishingServicePtr;

} // namespace RCF

#endif // ! INCLUDE_RCF_PUBLISHINGSERVICE_HPP
