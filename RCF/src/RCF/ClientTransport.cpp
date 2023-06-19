
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

#include <RCF/ClientTransport.hpp>

#include <RCF/Asio.hpp>
#include <RCF/ClientProgress.hpp>
#include <RCF/ClientStub.hpp>
#include <RCF/Exception.hpp>
#include <RCF/ServerTransport.hpp>

namespace RCF {

    ClientTransport::ClientTransport() :
        mMaxMessageLength(getDefaultMaxMessageLength()),
        mMaxOutgoingMessageLength(0),
        mLastRequestSize(0),
        mLastResponseSize(0),
        mRunningTotalBytesSent(0),
        mRunningTotalBytesReceived(0),
        mAsync(false)
    {}

    ClientTransport::ClientTransport(const ClientTransport & rhs) :
        mMaxMessageLength(rhs.mMaxMessageLength),
        mMaxOutgoingMessageLength(rhs.mMaxOutgoingMessageLength),
        mLastRequestSize(),
        mLastResponseSize(0),
        mRunningTotalBytesSent(0),
        mRunningTotalBytesReceived(0),
        mAsync(false)
    {
    }

    void ClientTransport::setClientProgressPtr(ClientProgressPtr clientProgressPtr)
    {
        mClientProgressPtr = clientProgressPtr;
    }

    bool ClientTransport::isConnected()
    {
        return true;
    }

    void ClientTransport::setMaxIncomingMessageLength(
        std::size_t maxMessageLength)
    {
        mMaxMessageLength = maxMessageLength;
    }

    std::size_t ClientTransport::getMaxIncomingMessageLength() const
    {
        return mMaxMessageLength;
    }

    void ClientTransport::setMaxOutgoingMessageLength(
        std::size_t maxMessageLength)
    {
        mMaxOutgoingMessageLength = maxMessageLength;
    }

    std::size_t ClientTransport::getMaxOutgoingMessageLength() const
    {
        return mMaxOutgoingMessageLength;
    }

    RcfSessionWeakPtr ClientTransport::getRcfSession()
    {
        return mRcfSessionWeakPtr;
    }

    void ClientTransport::setRcfSession(RcfSessionWeakPtr rcfSessionWeakPtr)
    {
        mRcfSessionWeakPtr = rcfSessionWeakPtr;
    }

    std::size_t ClientTransport::getLastRequestSize()
    {
        return mLastRequestSize;
    }

    std::size_t ClientTransport::getLastResponseSize()
    {
        return mLastResponseSize;
    }

    std::uint64_t ClientTransport::getRunningTotalBytesSent()
    {
        return mRunningTotalBytesSent;
    }

    std::uint64_t ClientTransport::getRunningTotalBytesReceived()
    {
        return mRunningTotalBytesReceived;
    }

    void ClientTransport::resetRunningTotals()
    {
        mRunningTotalBytesSent = 0;
        mRunningTotalBytesReceived = 0;
    }

    void ClientTransport::getWireFilters(
        std::vector<FilterPtr> &        filters)
    {
        filters.clear();
    }

    void ClientTransportCallback::setAsyncDispatcher(RcfServer & server)
    {
        RCF_ASSERT(!mpAsyncDispatcher);
        mpAsyncDispatcher = &server;
    }

    RcfServer * ClientTransportCallback::getAsyncDispatcher()
    {
        return mpAsyncDispatcher;
    }

    void ClientTransport::setAsync(bool async)
    {
        mAsync = async;
    }

    void ClientTransport::associateWithIoService(AsioIoService & ioService)
    {
        RCF_UNUSED_VARIABLE(ioService);
        RCF_ASSERT_ALWAYS("Asynchronous operations not implemented for this transport.");
    }

    bool ClientTransport::isAssociatedWithIoService()
    {
        return false;
    }

    void ClientTransport::cancel()
    {
        RCF_ASSERT_ALWAYS("cancel() not implemented for this transport");
    }

    bool ClientTransport::supportsTransportFilters()
    {
        return true;
    }

    void ClientTransport::getProgressInfo(RemoteCallProgressInfo & info)
    {
        RCF_UNUSED_VARIABLE(info);
    }

} // namespace RCF
