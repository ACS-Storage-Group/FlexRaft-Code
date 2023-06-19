
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

/// @file

#ifndef INCLUDE_RCF_RCFCLIENT_HPP
#define INCLUDE_RCF_RCFCLIENT_HPP

#include <functional>
#include <map>
#include <memory>

#include <RCF/Export.hpp>
#include <RCF/RcfFwd.hpp>
#include <RCF/Exception.hpp>

namespace RCF {


    RCF_EXPORT void setCurrentCallDesc(std::string& desc, RCF::MethodInvocationRequest& request, const char * szFunc, const char * szArity);

    // Returns the runtime name of the given RCF interface.
    template<typename Interface>
    inline std::string getInterfaceName(Interface * = 0)
    {
        return Interface::getInterfaceName();
    }

    /// Base class of all RcfClient<> templates.
    class RCF_EXPORT I_RcfClient
    {
    public:

        virtual ~I_RcfClient();

        I_RcfClient(const std::string & interfaceName);

        I_RcfClient(
            const std::string &     interfaceName, 
            ServerBindingPtr        serverStubPtr);

        I_RcfClient(
            const std::string &     interfaceName, 
            const Endpoint &        endpoint, 
            const std::string &     serverBindingName = "");

        I_RcfClient(
            const std::string &     interfaceName, 
            ClientTransportUniquePtr  clientTransportUniquePtr, 
            const std::string &     serverBindingName = "");

        I_RcfClient(
            const std::string &     interfaceName, 
            const ClientStub &      clientStub, 
            const std::string &     serverBindingName = "");

        // Copy construction/assignment.

        I_RcfClient(
            const std::string &     interfaceName, 
            const I_RcfClient &     rhs);

        I_RcfClient & operator=(const I_RcfClient & rhs);

        // Move construction/assignment.

        I_RcfClient(
            const std::string &     interfaceName,
            I_RcfClient &&     rhs);

        I_RcfClient & operator=(I_RcfClient && rhs);

        void                swap(I_RcfClient & rhs);

        void                setClientStubPtr(ClientStubPtr clientStubPtr);

        /// Returns the ClientStub of this RcfClient.
        ClientStub &        getClientStub();

        /// Returns the ClientStub of this RcfClient.
        const ClientStub &  getClientStub() const;

        ClientStubPtr       getClientStubPtr() const;
        ServerBindingPtr    getServerStubPtr() const;
        ServerBinding &     getServerStub();

    protected:

        void checkClientInitialized();

        ClientStubPtr                   mClientStubPtr;
        ServerBindingPtr                mServerStubPtr;

        // These are preserved even when swapping or moving.
        std::string                     mInterfaceName;
        mutable std::string             mServerBindingName;

        typedef Void                    V;
    };

    typedef std::shared_ptr<I_RcfClient> RcfClientPtr;

    // some meta-programming functionality needed by the macros in IDL.hpp

    typedef char (&yes_type)[1];
    typedef char (&no_type)[2];

    template<typename U> static yes_type RCF_hasRcfClientTypedef(typename U::RcfClientT *);
    template<typename U> static no_type RCF_hasRcfClientTypedef(...);

    template<typename T>
    struct GetRcfClient
    {
        typedef typename T::RcfClientT type;
    };

    template<typename T>
    struct Identity
    {
        typedef T type;
    };

    template<typename T>
    struct GetInterface
    {
        // tried eval_if here, but got some weird errors with vc71
        typedef typename If<
            Bool< sizeof(yes_type) == sizeof(RCF_hasRcfClientTypedef<T>(0)) >,
            GetRcfClient<T>,
            Identity<T> >::type type0;

        typedef typename type0::type type;
    };

    class default_ { char a[1]; };
    class defined_ { char a[2]; };
    template<typename T> class Dummy {};

} // namespace RCF

#endif // ! INCLUDE_RCF_RCFCLIENT_HPP
