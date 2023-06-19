
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

#include <RCF/Future.hpp>

#include <RCF/OverlappedAmi.hpp>
#include <RCF/Log.hpp>

namespace RCF {

    LogEntryExit::LogEntryExit(ClientStub & clientStub) :
        mClientStub(clientStub),
        mMsg(clientStub.mCurrentCallDesc)
    {
        if (mClientStub.mCallInProgress)
        {
            RCF_THROW(RcfError_ConcurrentCalls);
        }

        mClientStub.mCallInProgress = true;
        RCF_LOG_2() << "RcfClient - begin remote call. " << mMsg;
    }

    LogEntryExit::~LogEntryExit()
    {
        if (!mClientStub.getAsync())
        {
            RCF_LOG_2() << "RcfClient - end remote call. " << mMsg;
            mClientStub.mCallInProgress = false;
        }
    }

    FutureConverterBase::FutureConverterBase(
        ClientStub &clientStub, 
        int fnId,
        RemoteCallMode rcs,
        const char * szFunc,
        const char * szArity) :
            mpClientStub(&clientStub),
            mFnId(fnId),
            mRcs(rcs),
            mSzFunc(szFunc),
            mSzArity(szArity),
            mOwn(true)
    {
        // TODO: put this in the initializer list instead?
        clientStub.init(fnId, rcs);
    }

    FutureConverterBase::FutureConverterBase(const FutureConverterBase& rhs) :
        mpClientStub(rhs.mpClientStub),
        mFnId(rhs.mFnId),
        mRcs(rhs.mRcs),
        mSzFunc(rhs.mSzFunc),
        mSzArity(rhs.mSzArity),
        mOwn(rhs.mOwn)
    {
        rhs.mOwn = false;
    }

    FutureConverterBase & FutureConverterBase::operator=(const FutureConverterBase &rhs)
    {
        mpClientStub = rhs.mpClientStub;
        mFnId = rhs.mFnId;
        mRcs = rhs.mRcs;
        mSzFunc = rhs.mSzFunc;
        mSzArity = rhs.mSzArity;

        mOwn = rhs.mOwn;
        rhs.mOwn = false;
        return *this;
    }

    void FutureConverterBase::call() const
    {

#if RCF_FEATURE_FILETRANSFER==1

        // File uploads are done before the call itself.
        mpClientStub->processUploadStreams();

#endif

        // TODO
        bool async = mpClientStub->getAsync();

        mpClientStub->setTries(0);

        setCurrentCallDesc(mpClientStub->mCurrentCallDesc, mpClientStub->mRequest, mSzFunc, mSzArity);
        mpClientStub->mCurrentCallMethodName = mSzFunc;

        if (async)
        {
            callAsync();
        }
        else
        {
            callSync();
        }
    }

    void FutureConverterBase::callSync() const
    {
        // ClientStub::onConnectCompleted() uses the contents of mEncodedByteBuffers
        // to determine what stage the current call is in. So mEncodedByteBuffers
        // needs to be cleared after a remote call, even if an exception is thrown.

        // Error handling code here will generally also need to be present in 
        // ClientStub::onError().

        LogEntryExit logEntryExit(*mpClientStub);

        RCF_LOG_3()(mpClientStub)(mpClientStub->mRequest) 
            << "RcfClient - sending synchronous request.";

        bool shouldRetry = true;
        while ( shouldRetry )
        {
            shouldRetry = false;
            try
            {
                mpClientStub->call(mRcs);
            }
            catch ( const RCF::RemoteException & e )
            {
                mpClientStub->mEncodedByteBuffers.resize(0);
                if ( shouldDisconnectOnRemoteError(e.getErrorId()) )
                {
                    mpClientStub->disconnect();
                }
                throw;
            }
            catch ( const RCF::Exception & e )
            {
                mpClientStub->disconnect();
                if ( e.getShouldRetry() )
                {
                    shouldRetry = true;
                }
                else
                {
                    mpClientStub->mEncodedByteBuffers.resize(0);
                    mpClientStub->disconnect();
                    throw;
                }
            }
            catch ( ... )
            {
                mpClientStub->mEncodedByteBuffers.resize(0);
                mpClientStub->disconnect();
                throw;
            }
        }
    }

    void FutureConverterBase::callAsync() const
    {
        LogEntryExit logEntryExit(*mpClientStub);

        RCF_LOG_3()(mpClientStub)(mpClientStub->mRequest) 
            << "RcfClient - sending asynchronous request.";

        std::unique_ptr<RCF::Exception> ape;

        try
        {
            mpClientStub->call(mRcs);
        }
        catch(const RCF::Exception & e)
        {
            ape.reset( e.clone().release() );
        }
        catch(...)
        {
            ape.reset( new Exception(RcfError_NonStdException) );
        }

        if (ape.get())
        {
            mpClientStub->onError(*ape);
        }

        getTlsAmiNotification().run();
    }

} // namespace RCF
