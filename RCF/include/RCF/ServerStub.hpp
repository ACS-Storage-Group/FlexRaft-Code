
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

#ifndef INCLUDE_RCF_SERVERSTUB_HPP
#define INCLUDE_RCF_SERVERSTUB_HPP

#include <map>
#include <memory>
#include <vector>

#include <RCF/Config.hpp>
#include <RCF/Export.hpp>
#include <RCF/RcfClient.hpp>
#include <RCF/RcfSession.hpp>
#include <RCF/ThreadLibrary.hpp>
#include <RCF/Tools.hpp>

namespace RCF {

    class                                   I_RcfClient;
    typedef std::shared_ptr<I_RcfClient>  RcfClientPtr;
    class RcfSession;

    class MethodInvocationRequest;

    class ServerMethodBase
    {
    public:
        virtual void callMethod(int fnId, RcfSession& session) = 0;
    };
    typedef std::shared_ptr<ServerMethodBase> ServerMethodPtr;

    class ClientStub;
    typedef std::shared_ptr<ClientStub> ClientStubPtr;

    RCF_EXPORT void setCurrentCallDesc(
        std::string &                   desc, 
        RCF::MethodInvocationRequest &  request, 
        const char *                    szFunc, 
        const char *                    szArity);

    class RCF_EXPORT StubAccess
    {
    public:

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable: 4702 )
#endif

        void logBeginRemoteCall(const std::string& callDesc);

        template<typename RcfClientT, typename IdT, typename ImplementationT>
        void callMethod(
            RcfClientT &                rcfClient,
            const IdT &                 id,
            RcfSession &                session,
            ImplementationT &           t)
        {
            setCurrentCallDesc(
                session.mCurrentCallDesc, 
                session.mRequest, 
                rcfClient.getFunctionName(id),
                rcfClient.getArity(id));

            logBeginRemoteCall(session.mCurrentCallDesc);

            rcfClient.callMethod(id, session, t);
        }

#ifdef _MSC_VER
#pragma warning( pop )
#endif

    };

    template<typename RcfClientT, typename ImplementationT>
    inline void callMethod(
        RcfClientT &                    rcfClient,
        ImplementationT &               t,
        int                             fnId,
        RcfSession  &                   session)
    {
        switch (fnId) {
        case   0: StubAccess().callMethod(rcfClient, Int<  0>(), session, t); break;
        case   1: StubAccess().callMethod(rcfClient, Int<  1>(), session, t); break;
        case   2: StubAccess().callMethod(rcfClient, Int<  2>(), session, t); break;
        case   3: StubAccess().callMethod(rcfClient, Int<  3>(), session, t); break;
        case   4: StubAccess().callMethod(rcfClient, Int<  4>(), session, t); break;
        case   5: StubAccess().callMethod(rcfClient, Int<  5>(), session, t); break;
        case   6: StubAccess().callMethod(rcfClient, Int<  6>(), session, t); break;
        case   7: StubAccess().callMethod(rcfClient, Int<  7>(), session, t); break;
        case   8: StubAccess().callMethod(rcfClient, Int<  8>(), session, t); break;
        case   9: StubAccess().callMethod(rcfClient, Int<  9>(), session, t); break;
        case  10: StubAccess().callMethod(rcfClient, Int< 10>(), session, t); break;
        case  11: StubAccess().callMethod(rcfClient, Int< 11>(), session, t); break;
        case  12: StubAccess().callMethod(rcfClient, Int< 12>(), session, t); break;
        case  13: StubAccess().callMethod(rcfClient, Int< 13>(), session, t); break;
        case  14: StubAccess().callMethod(rcfClient, Int< 14>(), session, t); break;
        case  15: StubAccess().callMethod(rcfClient, Int< 15>(), session, t); break;
        case  16: StubAccess().callMethod(rcfClient, Int< 16>(), session, t); break;
        case  17: StubAccess().callMethod(rcfClient, Int< 17>(), session, t); break;
        case  18: StubAccess().callMethod(rcfClient, Int< 18>(), session, t); break;
        case  19: StubAccess().callMethod(rcfClient, Int< 19>(), session, t); break;
        case  20: StubAccess().callMethod(rcfClient, Int< 20>(), session, t); break;
        case  21: StubAccess().callMethod(rcfClient, Int< 21>(), session, t); break;
        case  22: StubAccess().callMethod(rcfClient, Int< 22>(), session, t); break;
        case  23: StubAccess().callMethod(rcfClient, Int< 23>(), session, t); break;
        case  24: StubAccess().callMethod(rcfClient, Int< 24>(), session, t); break;
        case  25: StubAccess().callMethod(rcfClient, Int< 25>(), session, t); break;
        case  26: StubAccess().callMethod(rcfClient, Int< 26>(), session, t); break;
        case  27: StubAccess().callMethod(rcfClient, Int< 27>(), session, t); break;
        case  28: StubAccess().callMethod(rcfClient, Int< 28>(), session, t); break;
        case  29: StubAccess().callMethod(rcfClient, Int< 29>(), session, t); break;
        case  30: StubAccess().callMethod(rcfClient, Int< 30>(), session, t); break;
        case  31: StubAccess().callMethod(rcfClient, Int< 31>(), session, t); break;
        case  32: StubAccess().callMethod(rcfClient, Int< 32>(), session, t); break;
        case  33: StubAccess().callMethod(rcfClient, Int< 33>(), session, t); break;
        case  34: StubAccess().callMethod(rcfClient, Int< 34>(), session, t); break;
        case  35: StubAccess().callMethod(rcfClient, Int< 35>(), session, t); break;
        case  36: StubAccess().callMethod(rcfClient, Int< 36>(), session, t); break;
        case  37: StubAccess().callMethod(rcfClient, Int< 37>(), session, t); break;
        case  38: StubAccess().callMethod(rcfClient, Int< 38>(), session, t); break;
        case  39: StubAccess().callMethod(rcfClient, Int< 39>(), session, t); break;
        case  40: StubAccess().callMethod(rcfClient, Int< 40>(), session, t); break;
        case  41: StubAccess().callMethod(rcfClient, Int< 41>(), session, t); break;
        case  42: StubAccess().callMethod(rcfClient, Int< 42>(), session, t); break;
        case  43: StubAccess().callMethod(rcfClient, Int< 43>(), session, t); break;
        case  44: StubAccess().callMethod(rcfClient, Int< 44>(), session, t); break;
        case  45: StubAccess().callMethod(rcfClient, Int< 45>(), session, t); break;
        case  46: StubAccess().callMethod(rcfClient, Int< 46>(), session, t); break;
        case  47: StubAccess().callMethod(rcfClient, Int< 47>(), session, t); break;
        case  48: StubAccess().callMethod(rcfClient, Int< 48>(), session, t); break;
        case  49: StubAccess().callMethod(rcfClient, Int< 49>(), session, t); break;
        case  50: StubAccess().callMethod(rcfClient, Int< 50>(), session, t); break;
        case  51: StubAccess().callMethod(rcfClient, Int< 51>(), session, t); break;
        case  52: StubAccess().callMethod(rcfClient, Int< 52>(), session, t); break;
        case  53: StubAccess().callMethod(rcfClient, Int< 53>(), session, t); break;
        case  54: StubAccess().callMethod(rcfClient, Int< 54>(), session, t); break;
        case  55: StubAccess().callMethod(rcfClient, Int< 55>(), session, t); break;
        case  56: StubAccess().callMethod(rcfClient, Int< 56>(), session, t); break;
        case  57: StubAccess().callMethod(rcfClient, Int< 57>(), session, t); break;
        case  58: StubAccess().callMethod(rcfClient, Int< 58>(), session, t); break;
        case  59: StubAccess().callMethod(rcfClient, Int< 59>(), session, t); break;
        case  60: StubAccess().callMethod(rcfClient, Int< 60>(), session, t); break;
        case  61: StubAccess().callMethod(rcfClient, Int< 61>(), session, t); break;
        case  62: StubAccess().callMethod(rcfClient, Int< 62>(), session, t); break;
        case  63: StubAccess().callMethod(rcfClient, Int< 63>(), session, t); break;
        case  64: StubAccess().callMethod(rcfClient, Int< 64>(), session, t); break;
        case  65: StubAccess().callMethod(rcfClient, Int< 65>(), session, t); break;
        case  66: StubAccess().callMethod(rcfClient, Int< 66>(), session, t); break;
        case  67: StubAccess().callMethod(rcfClient, Int< 67>(), session, t); break;
        case  68: StubAccess().callMethod(rcfClient, Int< 68>(), session, t); break;
        case  69: StubAccess().callMethod(rcfClient, Int< 69>(), session, t); break;
        case  70: StubAccess().callMethod(rcfClient, Int< 70>(), session, t); break;
        case  71: StubAccess().callMethod(rcfClient, Int< 71>(), session, t); break;
        case  72: StubAccess().callMethod(rcfClient, Int< 72>(), session, t); break;
        case  73: StubAccess().callMethod(rcfClient, Int< 73>(), session, t); break;
        case  74: StubAccess().callMethod(rcfClient, Int< 74>(), session, t); break;
        case  75: StubAccess().callMethod(rcfClient, Int< 75>(), session, t); break;
        case  76: StubAccess().callMethod(rcfClient, Int< 76>(), session, t); break;
        case  77: StubAccess().callMethod(rcfClient, Int< 77>(), session, t); break;
        case  78: StubAccess().callMethod(rcfClient, Int< 78>(), session, t); break;
        case  79: StubAccess().callMethod(rcfClient, Int< 79>(), session, t); break;
        case  80: StubAccess().callMethod(rcfClient, Int< 80>(), session, t); break;
        case  81: StubAccess().callMethod(rcfClient, Int< 81>(), session, t); break;
        case  82: StubAccess().callMethod(rcfClient, Int< 82>(), session, t); break;
        case  83: StubAccess().callMethod(rcfClient, Int< 83>(), session, t); break;
        case  84: StubAccess().callMethod(rcfClient, Int< 84>(), session, t); break;
        case  85: StubAccess().callMethod(rcfClient, Int< 85>(), session, t); break;
        case  86: StubAccess().callMethod(rcfClient, Int< 86>(), session, t); break;
        case  87: StubAccess().callMethod(rcfClient, Int< 87>(), session, t); break;
        case  88: StubAccess().callMethod(rcfClient, Int< 88>(), session, t); break;
        case  89: StubAccess().callMethod(rcfClient, Int< 89>(), session, t); break;
        case  90: StubAccess().callMethod(rcfClient, Int< 90>(), session, t); break;
        case  91: StubAccess().callMethod(rcfClient, Int< 91>(), session, t); break;
        case  92: StubAccess().callMethod(rcfClient, Int< 92>(), session, t); break;
        case  93: StubAccess().callMethod(rcfClient, Int< 93>(), session, t); break;
        case  94: StubAccess().callMethod(rcfClient, Int< 94>(), session, t); break;
        case  95: StubAccess().callMethod(rcfClient, Int< 95>(), session, t); break;
        case  96: StubAccess().callMethod(rcfClient, Int< 96>(), session, t); break;
        case  97: StubAccess().callMethod(rcfClient, Int< 97>(), session, t); break;
        case  98: StubAccess().callMethod(rcfClient, Int< 98>(), session, t); break;
        case  99: StubAccess().callMethod(rcfClient, Int< 99>(), session, t); break;
        case 100: StubAccess().callMethod(rcfClient, Int<100>(), session, t); break;
        case 101: StubAccess().callMethod(rcfClient, Int<101>(), session, t); break;
        case 102: StubAccess().callMethod(rcfClient, Int<102>(), session, t); break;
        case 103: StubAccess().callMethod(rcfClient, Int<103>(), session, t); break;
        case 104: StubAccess().callMethod(rcfClient, Int<104>(), session, t); break;
        case 105: StubAccess().callMethod(rcfClient, Int<105>(), session, t); break;
        case 106: StubAccess().callMethod(rcfClient, Int<106>(), session, t); break;
        case 107: StubAccess().callMethod(rcfClient, Int<107>(), session, t); break;
        case 108: StubAccess().callMethod(rcfClient, Int<108>(), session, t); break;
        case 109: StubAccess().callMethod(rcfClient, Int<109>(), session, t); break;
        case 110: StubAccess().callMethod(rcfClient, Int<110>(), session, t); break;
        case 111: StubAccess().callMethod(rcfClient, Int<111>(), session, t); break;
        case 112: StubAccess().callMethod(rcfClient, Int<112>(), session, t); break;
        case 113: StubAccess().callMethod(rcfClient, Int<113>(), session, t); break;
        case 114: StubAccess().callMethod(rcfClient, Int<114>(), session, t); break;
        case 115: StubAccess().callMethod(rcfClient, Int<115>(), session, t); break;
        case 116: StubAccess().callMethod(rcfClient, Int<116>(), session, t); break;
        case 117: StubAccess().callMethod(rcfClient, Int<117>(), session, t); break;
        case 118: StubAccess().callMethod(rcfClient, Int<118>(), session, t); break;
        case 119: StubAccess().callMethod(rcfClient, Int<119>(), session, t); break;
        case 120: StubAccess().callMethod(rcfClient, Int<120>(), session, t); break;
        case 121: StubAccess().callMethod(rcfClient, Int<121>(), session, t); break;
        case 122: StubAccess().callMethod(rcfClient, Int<122>(), session, t); break;
        case 123: StubAccess().callMethod(rcfClient, Int<123>(), session, t); break;
        case 124: StubAccess().callMethod(rcfClient, Int<124>(), session, t); break;
        case 125: StubAccess().callMethod(rcfClient, Int<125>(), session, t); break;
        case 126: StubAccess().callMethod(rcfClient, Int<126>(), session, t); break;
        case 127: StubAccess().callMethod(rcfClient, Int<127>(), session, t); break;
        case 128: StubAccess().callMethod(rcfClient, Int<128>(), session, t); break;
        case 129: StubAccess().callMethod(rcfClient, Int<129>(), session, t); break;
        case 130: StubAccess().callMethod(rcfClient, Int<130>(), session, t); break;
        case 131: StubAccess().callMethod(rcfClient, Int<131>(), session, t); break;
        case 132: StubAccess().callMethod(rcfClient, Int<132>(), session, t); break;
        case 133: StubAccess().callMethod(rcfClient, Int<133>(), session, t); break;
        case 134: StubAccess().callMethod(rcfClient, Int<134>(), session, t); break;
        case 135: StubAccess().callMethod(rcfClient, Int<135>(), session, t); break;
        case 136: StubAccess().callMethod(rcfClient, Int<136>(), session, t); break;
        case 137: StubAccess().callMethod(rcfClient, Int<137>(), session, t); break;
        case 138: StubAccess().callMethod(rcfClient, Int<138>(), session, t); break;
        case 139: StubAccess().callMethod(rcfClient, Int<139>(), session, t); break;
        case 140: StubAccess().callMethod(rcfClient, Int<140>(), session, t); break;
        case 141: StubAccess().callMethod(rcfClient, Int<141>(), session, t); break;
        case 142: StubAccess().callMethod(rcfClient, Int<142>(), session, t); break;
        case 143: StubAccess().callMethod(rcfClient, Int<143>(), session, t); break;
        case 144: StubAccess().callMethod(rcfClient, Int<144>(), session, t); break;
        case 145: StubAccess().callMethod(rcfClient, Int<145>(), session, t); break;
        case 146: StubAccess().callMethod(rcfClient, Int<146>(), session, t); break;
        case 147: StubAccess().callMethod(rcfClient, Int<147>(), session, t); break;
        case 148: StubAccess().callMethod(rcfClient, Int<148>(), session, t); break;
        case 149: StubAccess().callMethod(rcfClient, Int<149>(), session, t); break;
        case 150: StubAccess().callMethod(rcfClient, Int<150>(), session, t); break;
        case 151: StubAccess().callMethod(rcfClient, Int<151>(), session, t); break;
        case 152: StubAccess().callMethod(rcfClient, Int<152>(), session, t); break;
        case 153: StubAccess().callMethod(rcfClient, Int<153>(), session, t); break;
        case 154: StubAccess().callMethod(rcfClient, Int<154>(), session, t); break;
        case 155: StubAccess().callMethod(rcfClient, Int<155>(), session, t); break;
        case 156: StubAccess().callMethod(rcfClient, Int<156>(), session, t); break;
        case 157: StubAccess().callMethod(rcfClient, Int<157>(), session, t); break;
        case 158: StubAccess().callMethod(rcfClient, Int<158>(), session, t); break;
        case 159: StubAccess().callMethod(rcfClient, Int<159>(), session, t); break;
        case 160: StubAccess().callMethod(rcfClient, Int<160>(), session, t); break;
        case 161: StubAccess().callMethod(rcfClient, Int<161>(), session, t); break;
        case 162: StubAccess().callMethod(rcfClient, Int<162>(), session, t); break;
        case 163: StubAccess().callMethod(rcfClient, Int<163>(), session, t); break;
        case 164: StubAccess().callMethod(rcfClient, Int<164>(), session, t); break;
        case 165: StubAccess().callMethod(rcfClient, Int<165>(), session, t); break;
        case 166: StubAccess().callMethod(rcfClient, Int<166>(), session, t); break;
        case 167: StubAccess().callMethod(rcfClient, Int<167>(), session, t); break;
        case 168: StubAccess().callMethod(rcfClient, Int<168>(), session, t); break;
        case 169: StubAccess().callMethod(rcfClient, Int<169>(), session, t); break;
        case 170: StubAccess().callMethod(rcfClient, Int<170>(), session, t); break;
        case 171: StubAccess().callMethod(rcfClient, Int<171>(), session, t); break;
        case 172: StubAccess().callMethod(rcfClient, Int<172>(), session, t); break;
        case 173: StubAccess().callMethod(rcfClient, Int<173>(), session, t); break;
        case 174: StubAccess().callMethod(rcfClient, Int<174>(), session, t); break;
        case 175: StubAccess().callMethod(rcfClient, Int<175>(), session, t); break;
        case 176: StubAccess().callMethod(rcfClient, Int<176>(), session, t); break;
        case 177: StubAccess().callMethod(rcfClient, Int<177>(), session, t); break;
        case 178: StubAccess().callMethod(rcfClient, Int<178>(), session, t); break;
        case 179: StubAccess().callMethod(rcfClient, Int<179>(), session, t); break;
        case 180: StubAccess().callMethod(rcfClient, Int<180>(), session, t); break;
        case 181: StubAccess().callMethod(rcfClient, Int<181>(), session, t); break;
        case 182: StubAccess().callMethod(rcfClient, Int<182>(), session, t); break;
        case 183: StubAccess().callMethod(rcfClient, Int<183>(), session, t); break;
        case 184: StubAccess().callMethod(rcfClient, Int<184>(), session, t); break;
        case 185: StubAccess().callMethod(rcfClient, Int<185>(), session, t); break;
        case 186: StubAccess().callMethod(rcfClient, Int<186>(), session, t); break;
        case 187: StubAccess().callMethod(rcfClient, Int<187>(), session, t); break;
        case 188: StubAccess().callMethod(rcfClient, Int<188>(), session, t); break;
        case 189: StubAccess().callMethod(rcfClient, Int<189>(), session, t); break;
        case 190: StubAccess().callMethod(rcfClient, Int<190>(), session, t); break;
        case 191: StubAccess().callMethod(rcfClient, Int<191>(), session, t); break;
        case 192: StubAccess().callMethod(rcfClient, Int<192>(), session, t); break;
        case 193: StubAccess().callMethod(rcfClient, Int<193>(), session, t); break;
        case 194: StubAccess().callMethod(rcfClient, Int<194>(), session, t); break;
        case 195: StubAccess().callMethod(rcfClient, Int<195>(), session, t); break;
        case 196: StubAccess().callMethod(rcfClient, Int<196>(), session, t); break;
        case 197: StubAccess().callMethod(rcfClient, Int<197>(), session, t); break;
        case 198: StubAccess().callMethod(rcfClient, Int<198>(), session, t); break;
        case 199: StubAccess().callMethod(rcfClient, Int<199>(), session, t); break;
        case 200: StubAccess().callMethod(rcfClient, Int<200>(), session, t); break;

        default: RCF_THROW(Exception(RcfError_FnId, fnId));
        }
    }

    template<typename RcfClientT, typename RefWrapperT>
    class ServerMethod : public ServerMethodBase
    {
    public:
        ServerMethod(RcfClientT &rcfClient, RefWrapperT refWrapper) :
            mRcfClient(rcfClient),
            mRefWrapper(refWrapper)
        {}

        void callMethod(int fnId, RcfSession & session)
        {
            RCF::callMethod(mRcfClient, mRefWrapper.get(), fnId, session);
        }

    private:
        RcfClientT &    mRcfClient;
        RefWrapperT     mRefWrapper;
    };

    /// Represents the binding of a server-side servant object to a RCF interface.
    class RCF_EXPORT ServerBinding
    {
    public:

        /// Sets a callback that will be called each time a client connections attempts to execute
        /// a remote method on this server binding.
        void setAccessControl(AccessControlCallback cbAccessControl);

        template<typename RcfClientT, typename RefWrapperT>
        void addServerMethods(RcfClientT &rcfClient, RefWrapperT refWrapper)
        {
            mServerMethodPtr.reset(
                new ServerMethod<RcfClientT, RefWrapperT>(
                    rcfClient,
                    refWrapper));
        }

    private:

        friend class RcfServer;
        friend class RcfSession;

        void callMethod(
            int                         fnId,
            RcfSession &                session);

    private:
        Mutex                           mMutex;
        ServerMethodPtr                 mServerMethodPtr;
        AccessControlCallback           mCbAccessControl;
    };

    template<typename InterfaceT, typename ImplementationT, typename ImplementationPtrT>
    RcfClientPtr createServerStub(
        InterfaceT *,
        ImplementationT *,
        ImplementationPtrT px)
    {
        typedef typename InterfaceT::RcfClientT RcfClientT;
        return RcfClientPtr( new RcfClientT(
            ServerBindingPtr(new ServerBinding()),
            px,
            (TrueType *)NULL));
    }
    
} // namespace RCF

#endif // ! INCLUDE_RCF_SERVERSTUB_HPP
