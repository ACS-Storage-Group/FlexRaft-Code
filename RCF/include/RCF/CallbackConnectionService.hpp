
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

#ifndef INCLUDE_RCF_CALLBACKCONNECTIONSERVICE_HPP
#define INCLUDE_RCF_CALLBACKCONNECTIONSERVICE_HPP

#include <RCF/RcfFwd.hpp>
#include <RCF/Service.hpp>
#include <RCF/Tools.hpp>

namespace RCF {

    class CallbackConnectionService : public I_Service, Noncopyable
    {
    public:

        CallbackConnectionService();

    private:

        void onServiceAdded(RcfServer & server);
        void onServiceRemoved(RcfServer & server);
        void onServerStart(RcfServer & server);

    public:
        friend class RcfClient<I_CreateCallbackConnection>;
        void CreateCallbackConnection();

    private:
        RcfServer * mpServer;
        OnCallbackConnectionCreated mOnCallbackConnectionCreated;
    };

    typedef std::shared_ptr<CallbackConnectionService> CallbackConnectionServicePtr;

} // namespace RCF

#endif // ! INCLUDE_RCF_CALLBACKCONNECTIONSERVICE_HPP
