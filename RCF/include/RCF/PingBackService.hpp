
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

#ifndef INCLUDE_RCF_PINGBACKSERVICE_HPP
#define INCLUDE_RCF_PINGBACKSERVICE_HPP

#include <memory>

#include <RCF/Export.hpp>
#include <RCF/Heap.hpp>
#include <RCF/Service.hpp>

namespace RCF {

    class RcfSession;
    typedef std::shared_ptr<RcfSession> RcfSessionPtr;
    typedef std::weak_ptr<RcfSession> RcfSessionWeakPtr;
    
    typedef std::pair<std::uint32_t, RcfSessionWeakPtr>   PingBackTimerEntry;

    class RCF_EXPORT PingBackService : public I_Service
    {
    public:

        typedef std::weak_ptr<RcfSession> RcfSessionWeakPtr;

        PingBackService();

        PingBackTimerEntry  registerSession(RcfSessionPtr rcfSessionPtr);
        void                unregisterSession(const PingBackTimerEntry & entry);

    private:

        typedef PingBackTimerEntry Entry;

        void                onServiceAdded(RcfServer &server);
        void                onServiceRemoved(RcfServer &server);
        void                stop();
        void                cycle(int timeoutMs);

        bool                            mEnabled;
        bool                            mLazyStarted;

        // Internally synchronized.
        TimerHeap<RcfSessionWeakPtr>    mTimerHeap;

        Mutex                           mMutex;
        Condition                       mCondition;
    };

    typedef std::shared_ptr<PingBackService> PingBackServicePtr;    

} // namespace RCF

#endif // ! INCLUDE_RCF_PINGBACKSERVICE_HPP
