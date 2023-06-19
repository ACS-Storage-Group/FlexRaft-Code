
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

#include <RCF/SessionTimeoutService.hpp>

#include <RCF/RcfServer.hpp>
#include <RCF/RcfSession.hpp>
#include <RCF/ServerTransport.hpp>

namespace RCF {

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4355 ) // warning C4355: 'this' : used in base member initializer list
#endif

    SessionTimeoutService::SessionTimeoutService() : 
            mSessionTimeoutMs(0),
            mReapingIntervalMs(0),
            mpRcfServer(),
            mPeriodicTimer(*this, 0)
    {
    }

#ifdef _MSC_VER
#pragma warning( pop )
#endif

    void SessionTimeoutService::onServerStart(RcfServer &server)
    {
        mpRcfServer = & server;
        
        mSessionTimeoutMs = mpRcfServer->getConnectionIdleTimeoutMs();
        mReapingIntervalMs = mpRcfServer->getConnectionIdleScanIntervalMs();

        if (mSessionTimeoutMs)
        {
            mPeriodicTimer.setIntervalMs(mReapingIntervalMs);
            mPeriodicTimer.start();
        }
    }

    void SessionTimeoutService::onServerStop(RcfServer &server)
    {
        RCF_UNUSED_VARIABLE(server);
        mPeriodicTimer.stop();
        mpRcfServer = NULL;
    }

    void SessionTimeoutService::restart()
    {
        if ( mpRcfServer )
        {
            mPeriodicTimer.stop();

            mSessionTimeoutMs = mpRcfServer->getConnectionIdleTimeoutMs();
            mReapingIntervalMs = mpRcfServer->getConnectionIdleScanIntervalMs();

            if ( mSessionTimeoutMs )
            {
                mPeriodicTimer.setIntervalMs(mReapingIntervalMs);
                mPeriodicTimer.start();
            }
        }
    }

    void SessionTimeoutService::onTimer()
    {
        mSessionsTemp.resize(0);

        mpRcfServer->enumerateSessions(std::back_inserter(mSessionsTemp));

        mSessionTimeoutMs = mpRcfServer->getConnectionIdleTimeoutMs();

        for (std::size_t i=0; i<mSessionsTemp.size(); ++i)
        {
            NetworkSessionPtr networkSessionPtr = mSessionsTemp[i].lock();
            if (networkSessionPtr)
            {
                RcfSessionPtr rcfSessionPtr = networkSessionPtr->getSessionPtr();
                if (rcfSessionPtr)
                {
                    bool callInProgress = rcfSessionPtr->getCallInProgress();
                    std::uint32_t lastTouched = rcfSessionPtr->getTouchTimestamp();
                    if (!callInProgress && lastTouched)
                    {
                        RCF::Timer lastTouchedTimer( lastTouched );
                        if (lastTouchedTimer.elapsed(mSessionTimeoutMs))
                        {
                            rcfSessionPtr->disconnect();
                        }
                    }
                }
                else
                {
                    // Network session without a RCF session. Typically related to a HTTP connection.
                    std::uint32_t lastTouched = networkSessionPtr->getLastActivityTimestamp();
                    if ( lastTouched )
                    {
                        RCF::Timer lastTouchedTimer(lastTouched);
                        if ( lastTouchedTimer.elapsed(mSessionTimeoutMs) )
                        {
                            networkSessionPtr->setEnableReconnect(false);
                            networkSessionPtr->postClose();
                        }
                    }
                }
            }
        }
    }

} //namespace RCF
