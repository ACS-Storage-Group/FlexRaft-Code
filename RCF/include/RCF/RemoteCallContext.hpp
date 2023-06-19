
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

#ifndef INCLUDE_RCF_REMOTECALLCONTEXT_HPP
#define INCLUDE_RCF_REMOTECALLCONTEXT_HPP

#include <memory> 
#include <type_traits>

#include <RCF/Export.hpp>
#include <RCF/Marshal.hpp>
#include <RCF/Tools.hpp>

namespace RCF {

    class I_Parameters;
    class RcfSession;
    class AsioNetworkSession;

    typedef std::shared_ptr<RcfSession> RcfSessionPtr;
    typedef std::shared_ptr<AsioNetworkSession> AsioNetworkSessionPtr;

    /// Base class of RemoteCallContext.
    class RCF_EXPORT RemoteCallContextImpl
    {
    public:

        RemoteCallContextImpl(RCF::RcfSession & session);

        /// Signals that processing of a remote call has completed, and that the remote call response should now be sent back to the client.
        void commit();

        /// Signals that the processing of a remote call has resulted in an exception, and that the remote call response should now be sent back to the client.
        void commit(const std::exception &e);

        /// Returns true if commit() has been called on this remote call context.
        bool isCommitted() const;

        /// Returns the RcfSession associated with this remote call context.
        RcfSession& getRcfSession();

    private:
        RcfSessionPtr           mRcfSessionPtr;
        AsioNetworkSessionPtr   mNetworkSessionPtr;
        bool                    mCommitted;

    protected:
        I_Parameters *          mpParametersUntyped;

    };

    /// Represents a server-side remote call context. By creating a RemoteCallContext within the server-side
    /// implementation of a remote call, the execution of the remote call can be transferred to other threads.
    template<
        typename R, 
        typename A1  = Void,
        typename A2  = Void,
        typename A3  = Void,
        typename A4  = Void,
        typename A5  = Void,
        typename A6  = Void,
        typename A7  = Void,
        typename A8  = Void,
        typename A9  = Void,
        typename A10 = Void,
        typename A11 = Void,
        typename A12 = Void,
        typename A13 = Void,
        typename A14 = Void,
        typename A15 = Void>
    class RemoteCallContext : public RemoteCallContextImpl
    {
    public:

        // If return type is void, change it to RCF::Void.
        typedef typename If<
            std::is_same<R, void>,
            Void,
            R>::type R_;

        typedef ServerParameters<
            R_, 
            A1, A2, A3, A4, A5, A6, A7, A8,
            A9, A10, A11, A12, A13, A14, A15> ParametersT;

        /// Constructs a remote call context.
        RemoteCallContext(RCF::RcfSession & session) : RemoteCallContextImpl(session)
        {
            RCF_ASSERT( dynamic_cast<ParametersT *>(mpParametersUntyped) );
        }

        /// Provides access to the parameters of a remote call context.
        ParametersT &parameters()
        {
            return * static_cast<ParametersT *>(mpParametersUntyped);;
        }
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_REMOTECALLCONTEXT_HPP
