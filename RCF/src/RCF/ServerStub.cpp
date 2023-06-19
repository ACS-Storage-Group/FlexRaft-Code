
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

#include <RCF/ServerStub.hpp>

#include <iterator>

#include <RCF/Exception.hpp>
#include <RCF/RcfClient.hpp>
#include <RCF/Tools.hpp>
#include <RCF/Log.hpp>

namespace RCF {

    void StubAccess::logBeginRemoteCall(const std::string& callDesc)
    {
        RCF_LOG_2() << "RcfServer - begin remote call. " << callDesc;
    }

    void ServerBinding::setAccessControl(AccessControlCallback cbAccessControl)
    {
        Lock lock(mMutex);
        mCbAccessControl = cbAccessControl;
    }


    void ServerBinding::callMethod(
        int                         fnId,
        RcfSession &                session)
    {
        // Check access control.

        {
            Lock lock(mMutex);
            if (mCbAccessControl)
            {
                bool ok = mCbAccessControl(fnId);
                if (!ok)
                {
                    RCF_THROW( RCF::Exception(RcfError_ServerStubAccessDenied));
                }
            }
        }

        // No mutex here, since there is never anyone writing to mServerMethodMap.

        mServerMethodPtr->callMethod(fnId, session);
    }

} // namespace RCF
