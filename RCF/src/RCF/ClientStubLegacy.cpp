
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

#include <RCF/ClientStub.hpp>
#include <RCF/Config.hpp>
#include <RCF/Filter.hpp>
#include <RCF/Idl.hpp>
#include <RCF/RcfServer.hpp>

#if RCF_FEATURE_LEGACY==0

namespace RCF {

    void ClientStub::requestTransportFilters_Legacy(const std::vector<FilterPtr> &filters)
    {
        RCF_UNUSED_VARIABLE(filters);

        RCF_THROW( Exception( 
            RcfError_NotSupportedInThisBuild, "Legacy transport filter request") );
    }

    void ClientStub::requestTransportFiltersAsync_Legacy(
        const std::vector<FilterPtr> &filters,
        std::function<void()> onCompletion)
    {
        RCF_UNUSED_VARIABLE(filters);
        RCF_UNUSED_VARIABLE(onCompletion);

        std::unique_ptr<Exception> ePtr( new Exception(
            RcfError_NotSupportedInThisBuild, "Legacy transport filter request") );

        setAsyncException(std::move(ePtr));
    }

} // namespace RCF

#else

#include <RCF/ClientStubLegacy.hpp>

namespace RCF {

    void ClientStub::requestTransportFilters_Legacy(const std::vector<FilterPtr> &filters)
    {
        // TODO: the current message filter sequence is not being used,
        // when making the filter request call to the server.

        //using namespace boost::multi_index::detail; // for scope_guard

        std::vector<std::int32_t> filterIds;

        for (std::size_t i=0; i<filters.size(); ++i)
        {
            filterIds.push_back( filters[i]->getFilterId());
        }

        if (!isConnected())
        {
            connect();
        }
        RCF::RcfClient<RCF::I_RequestTransportFilters> client(*this);
        client.getClientStub().setTransport( releaseTransport());
        //client.getClientStub().setTargetToken( Token());

        RestoreClientTransportGuard guard(*this, client.getClientStub());
        RCF_UNUSED_VARIABLE(guard);

        client.getClientStub().setRemoteCallTimeoutMs( getRemoteCallTimeoutMs() );
        int ret = client.RequestTransportFilters(RCF::Twoway, filterIds);
        RCF_VERIFY(ret == RcfError_Ok_Id, RemoteException(ErrorMsg(ret)));;

        for (std::size_t i=0; i<filters.size(); ++i)
        {
            filters[i]->resetState();
        }

        client.getClientStub().getTransport().setTransportFilters(filters);
    }

    class Handler
    {
    public:

        virtual ~Handler()
        {
        }

        void handle(
            Future<std::int32_t>      fRet,
            I_RcfClient &               rcfClient,
            ClientStub &                clientStubOrig,
            std::function<void()>      onCompletion)
        {
            ClientStubPtr clientStubPtr = 
                rcfClient.getClientStub().shared_from_this();

            ClientStubPtr clientStubOrigPtr = clientStubOrig.shared_from_this();

            clientStubOrigPtr->setTransport( 
                clientStubPtr->releaseTransport() );

            clientStubOrigPtr->setSubRcfClientPtr( RcfClientPtr() );

            std::unique_ptr<Exception> ape(clientStubPtr->getAsyncException());

            bool failed = (ape.get() != NULL);

            clientStubOrigPtr->setAsyncException(std::move(ape));

            if (failed)
            {
                onCompletion();
            }
            else
            {
                mClientStubPtr = clientStubOrigPtr;

                std::int32_t ret = fRet;
                if (ret == RcfError_Ok_Id)
                {
                    handleOk();
                    onCompletion();
                }
                else
                {
                    std::unique_ptr<Exception> apException(
                        new RemoteException( ErrorMsg(ret) ));

                    clientStubOrigPtr->setAsyncException(std::move(apException));

                    handleFail();

                    onCompletion();
                }
            }
        }

        virtual void handleOk()
        {
        }

        virtual void handleFail()
        {
        }

    protected:
        ClientStubPtr mClientStubPtr;
    };

    typedef std::shared_ptr<Handler> HandlerPtr;

    class RequestTransportFiltersHandler : public Handler
    {
    public :
        RequestTransportFiltersHandler(
            std::shared_ptr< std::vector<FilterPtr> > filtersPtr) :
        mFiltersPtr(filtersPtr)
        {
        }

    private:
        void handleOk()
        {
            for (std::size_t i=0; i<mFiltersPtr->size(); ++i)
            {
                (*mFiltersPtr)[i]->resetState();
            }
            mClientStubPtr->getTransport().setTransportFilters(*mFiltersPtr);
        }

        std::shared_ptr< std::vector<FilterPtr> > mFiltersPtr;
    };

    void ClientStub::requestTransportFiltersAsync_Legacy(
        const std::vector<FilterPtr> &filters,
        std::function<void()> onCompletion)
    {
        std::vector<std::int32_t> filterIds;

        for (std::size_t i=0; i<filters.size(); ++i)
        {
            filterIds.push_back( filters[i]->getFilterId() );
        }

        std::shared_ptr<std::vector<FilterPtr> > filtersPtr(
            new std::vector<FilterPtr>(filters) );

        typedef RcfClient<I_RequestTransportFilters> RtfClient;
        typedef std::shared_ptr<RtfClient> RtfClientPtr;

        RtfClientPtr rtfClientPtr( new RtfClient(*this) );

        rtfClientPtr->getClientStub().setTransport( releaseTransport());
        //rtfClientPtr->getClientStub().setTargetToken( Token());

        setSubRcfClientPtr(rtfClientPtr);

        setAsync(true);

        Future<std::int32_t> fRet;

        HandlerPtr handlerPtr( new RequestTransportFiltersHandler(filtersPtr));

        fRet = rtfClientPtr->RequestTransportFilters(

            RCF::AsyncTwoway( std::bind(
                &Handler::handle, 
                handlerPtr,
                fRet,
                std::ref(*rtfClientPtr),
                std::ref(*this),
                onCompletion)),

            filterIds);
    }

} // namespace RCF

#endif
