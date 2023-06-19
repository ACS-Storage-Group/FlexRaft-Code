
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

#include <RCF/CurrentSession.hpp>

#include <memory>

#include <RCF/RcfSession.hpp>
#include <RCF/ThreadLibrary.hpp>
#include <RCF/ThreadLocalData.hpp>

namespace RCF
{
    CurrentRcfSessionSentry::CurrentRcfSessionSentry(RcfSession & session)
    {
        RcfSession * pPrev = getTlsRcfSessionPtr();
        getRcfSessionSentryStack().push_back(pPrev);

        setTlsRcfSessionPtr(&session);
    }

    CurrentRcfSessionSentry::CurrentRcfSessionSentry(RcfSessionPtr sessionPtr)
    {
        RcfSession * pPrev = getTlsRcfSessionPtr();
        getRcfSessionSentryStack().push_back(pPrev);

        RcfSession * pSession = sessionPtr.get();
        setTlsRcfSessionPtr(pSession);
    }

    CurrentRcfSessionSentry::~CurrentRcfSessionSentry()
    {
        RcfSession * pPrev = getRcfSessionSentryStack().back();
        getRcfSessionSentryStack().pop_back();
        setTlsRcfSessionPtr(pPrev);
    }

}
