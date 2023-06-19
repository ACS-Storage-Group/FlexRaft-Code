
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

#include <RCF/CallbackConnectionService.hpp>

#include <RCF/Marshal.hpp>
#include <RCF/RcfServer.hpp>

namespace RCF {

    CallbackConnectionService::CallbackConnectionService() : mpServer(NULL)
    {
    }

    void CallbackConnectionService::onServiceAdded(RcfServer & server)
    {
        RCF_UNUSED_VARIABLE(server);
    }

    void CallbackConnectionService::onServiceRemoved(RcfServer & server)
    {
        RCF_UNUSED_VARIABLE(server);
    }

    void CallbackConnectionService::onServerStart(RcfServer & server)
    {
        mOnCallbackConnectionCreated = server.getOnCallbackConnectionCreated();
    }

    void CallbackConnectionService::CreateCallbackConnection()
    {
        if ( !mOnCallbackConnectionCreated )
        {
            RCF_THROW( Exception(RcfError_ServerCallbacksNotSupported) );
        }

        RCF::convertRcfSessionToRcfClient( mOnCallbackConnectionCreated );
    }

} // namespace RCF
