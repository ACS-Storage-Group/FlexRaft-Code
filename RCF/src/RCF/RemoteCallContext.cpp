
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

#include <RCF/RemoteCallContext.hpp>
#include <RCF/AsioServerTransport.hpp>
#include <RCF/Log.hpp>

namespace RCF {

    RemoteCallContextImpl::RemoteCallContextImpl(RCF::RcfSession & session) : mCommitted(false)
    {
        mRcfSessionPtr = session.shared_from_this();
        mRcfSessionPtr->mAutoSend = false;

        mpParametersUntyped = mRcfSessionPtr->mpParameters;

        AsioNetworkSession & networkSession =
            dynamic_cast<AsioNetworkSession &>(
            mRcfSessionPtr->getNetworkSession());

        mNetworkSessionPtr = networkSession.sharedFromThis();
    }

    void RemoteCallContextImpl::commit()
    {
        RCF_ASSERT(!mCommitted);

        if ( mRcfSessionPtr->mRequest.getOneway() )
        {
            RCF_LOG_3()(this) << "RcfServer - suppressing response to oneway call.";
            mRcfSessionPtr->mIn.clearByteBuffer();
            mRcfSessionPtr->clearParameters();
            setTlsRcfSessionPtr();
            mRcfSessionPtr->onWriteCompleted();
        }
        else
        {
            mRcfSessionPtr->sendResponse();
        }

        mpParametersUntyped = NULL;
        mRcfSessionPtr.reset();

        mNetworkSessionPtr.reset();

        mCommitted = true;
    }

    void RemoteCallContextImpl::commit(const std::exception &e)
    {
        RCF_ASSERT(!mCommitted);

        if ( mRcfSessionPtr->mRequest.getOneway() )
        {
            RCF_LOG_3()(this) << "RcfServer - suppressing response to oneway call.";
            mRcfSessionPtr->mIn.clearByteBuffer();
            mRcfSessionPtr->clearParameters();
            setTlsRcfSessionPtr();
            mRcfSessionPtr->onWriteCompleted();
        }
        else
        {
            mRcfSessionPtr->sendResponseException(e);
        }

        mpParametersUntyped = NULL;
        mRcfSessionPtr.reset();

        mNetworkSessionPtr.reset();

        mCommitted = true;
    }

    bool RemoteCallContextImpl::isCommitted() const
    {
        return mCommitted;
    }

    RcfSession& RemoteCallContextImpl::getRcfSession()
    {
        return *mRcfSessionPtr;
    }

} // namespace RCF
