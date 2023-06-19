
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

#include <RCF/Exception.hpp>

#include <RCF/ClientStub.hpp>
#include <RCF/Config.hpp>
#include <RCF/RcfSession.hpp>
#include <RCF/ThreadLocalData.hpp>
#include <RCF/Tools.hpp>
#include <RCF/Version.hpp>
#include <RCF/Log.hpp>

#include <RCF/BsdSockets.hpp> // GetErrorString()

#if RCF_FEATURE_SF==1
#include <SF/Archive.hpp>
#include <SF/string.hpp>
#include <SF/vector.hpp>
#endif

namespace RCF {

    std::string doReplacements(const std::string& msg, const std::vector<std::string>& args)
    {
        std::string s = msg;

        // Replace placeholders with arguments.
        for (std::size_t i=0; i<args.size(); ++i)
        {
            if (!args[i].empty())
            {
                std::string placeHolder = "%";
                placeHolder += char('1' + i);
                placeHolder += "%";

                std::size_t pos = s.find(placeHolder);
                if (pos != std::string::npos)
                {
                    s.replace(pos, placeHolder.length(), args[i]);
                }
            }
        }

        return s;
    }

    bool shouldDisconnectOnRemoteError(int errorId)
    {
        std::vector<int> msgs = { 
            RcfError_ConnectionLimitExceeded.getErrorId() , 
            RcfError_ServerMessageLength.getErrorId(),
            RcfError_ZlibDeflate.getErrorId(),
            RcfError_ZlibInflate.getErrorId() };

        return std::find(msgs.begin(), msgs.end(), errorId) != msgs.end();
    }

    std::string osError(int osError)
    {
        return Platform::OS::GetErrorString(osError);
    }

    // Exception

    void Exception::init(
        const ErrorMsg&         msg,
        const std::string &     arg1,
        const std::string &     arg2,
        const std::string &     arg3)
    {
        mErrorId = msg.getErrorId();
        std::string errorMessageLiteral = msg.getLiteral();
        std::vector < std::string > args;
        if ( arg1.size() > 0 )
        {
            args.push_back(arg1);
        }
        if ( arg2.size() )
        {
            args.push_back(arg2);
        }
        if ( arg3.size() )
        {
            args.push_back(arg3);
        }
        mErrorString = doReplacements(errorMessageLiteral, args);
    }

    Exception::Exception() :
        mErrorId(RcfError_Ok.getErrorId()),
        mErrorString(),
        mShouldRetry(false)
    {}

    Exception::Exception(const std::string & errorMsg):
        mErrorId(RcfError_User.getErrorId()),
        mErrorString(errorMsg)
    {
    }

    Exception::Exception(
        const ErrorMsg& msg,
        const std::string &     arg1,
        const std::string &     arg2,
        const std::string &     arg3) :
            mShouldRetry(false)
    {
        init(msg, arg1, arg2, arg3);
    }

    Exception::~Exception() throw()
    {}

    std::unique_ptr<Exception> Exception::clone() const
    {
        return std::unique_ptr<Exception>(
            new Exception(*this));
    }

    bool Exception::good() const
    {
        return mErrorId == RcfError_Ok.getErrorId();
    }

    bool Exception::bad() const
    {
        return !good();
    }

    void Exception::clear()
    {
        *this = Exception();
    }

    const char *Exception::what() const throw()
    {
        return mErrorString.c_str();
    }

    int Exception::getErrorId() const
    {
        return mErrorId;
    }

    std::string Exception::getErrorMessage() const
    {
        return mErrorString;
    }

    // Deprecated.
    std::string Exception::getErrorString() const
    {
        return getErrorMessage();
    }

    bool Exception::getShouldRetry() const
    {
        return mShouldRetry;
    }

    void Exception::setShouldRetry(bool shouldRetry)
    {
        mShouldRetry = shouldRetry;
    }

    void Exception::throwSelf() const
    {
        {
            // If your code traps on this assert, check that you have overridden throwSelf() in 
            // all classes derived from Exception.

            RCF_ASSERT(
                typeid(*this) == typeid(Exception))
                (typeid(*this).name());
        }

        throw *this;
    }

    // RemoteException

    RemoteException::RemoteException()
    {}

    RemoteException::RemoteException(const Exception& e) :
        Exception(e)
    {
    }

    RemoteException::RemoteException(
        const std::string & errorMsg) : 
            Exception(errorMsg)
    {}

    RemoteException::RemoteException(
        const ErrorMsg& msg,
        const std::string &     arg1,
        const std::string &     arg2,
        const std::string &     arg3) :
            Exception(msg, arg1, arg2, arg3)
    {}

    RemoteException::~RemoteException() throw()
    {}

    std::unique_ptr<Exception> RemoteException::clone() const
    {
        // If your code traps on this assert, check that you have overridden 
        // clone() in all classes derived from RemoteException.

        RCF_ASSERT(
            typeid(*this) == typeid(RemoteException))
            (typeid(*this).name());

        return std::unique_ptr<Exception>(
            new RemoteException(*this));
    }

    void RemoteException::throwSelf() const
    {
        // If your code traps on this assert, check that you have overridden 
        // throwSelf() in all classes derived from RemoteException.

        RCF_ASSERT(
            typeid(*this) == typeid(RemoteException))
            (typeid(*this).name());

        throw *this;
    }

    // VersioningException

    VersioningException::VersioningException(
        std::uint32_t runtimeVersion, 
        std::uint32_t archiveVersion) :
            RemoteException(RcfError_VersionMismatch),
            mRuntimeVersion(runtimeVersion),
            mArchiveVersion(archiveVersion)
    {}

    VersioningException::~VersioningException() throw()
    {}

    std::uint32_t VersioningException::getRuntimeVersion() const
    {
        return mRuntimeVersion;
    }

    std::uint32_t VersioningException::getArchiveVersion() const
    {
        return mArchiveVersion;
    }

    std::unique_ptr<Exception> VersioningException::clone() const
    {
        return std::unique_ptr<Exception>(
            new VersioningException(*this));
    }

    void VersioningException::throwSelf() const
    {
        throw *this;
    }

#if RCF_FEATURE_SF==1

    class LegacyError
    {
    public:
        void serialize(SF::Archive& ar)
        {
            ar & mErrorId & mArgs;
        }
        int                             mErrorId = 0;
        std::vector<std::string>        mArgs;
    };

    void RemoteException::serialize(SF::Archive &ar)
    {
        int runtimeVersion = ar.getRuntimeVersion();

        if ( runtimeVersion <= 12 )
        {
            bool shouldRetry = false;
            SF::IStream * pIs = ar.getIstream();
            std::size_t archivePos = pIs ? pIs->tell() : -1;

            try
            {
                std::string what;
                std::string context;
                LegacyError error;
                int subSystem = 0;
                int subSystemError = 0;
                std::string remoteExceptionType;

                if ( ar.isWrite() )
                {
                    what = mErrorString;
                    error.mErrorId = mErrorId;
                }

                ar
                    & what
                    & context
                    & error
                    & subSystemError
                    & subSystem
                    & remoteExceptionType;

                if ( ar.isRead() )
                {
                    mErrorId = error.mErrorId;
                    std::string errorMessageLiteral = ErrorMsg(mErrorId).getLiteral();
                    mErrorString = doReplacements(errorMessageLiteral, error.mArgs);
                }
            }
            catch ( const Exception& e )
            {
                if ( ar.isRead() )
                {
                    RCF_LOG_1()(e.getErrorMessage())(runtimeVersion) << "Exception while attempting to deserialize RemoteException.";
                    RCF_LOG_1() << "Retrying deserialization with newer runtime version.";
                    shouldRetry = true;
                }
                else
                {
                    throw;
                }
            }

            if ( shouldRetry )
            {
                pIs->seek(archivePos);
                ar
                    & mErrorId
                    & mErrorString;
            }
        }
        else
        {
            ar
                & mErrorId
                & mErrorString;
        }
    }

#endif

    int getRuntimeVersionOfThisRemoteCall()
    {
        int runtimeVersion = 0;
        RcfSession * pRcfSession = getTlsRcfSessionPtr();
        ClientStub * pClientStub = getTlsClientStubPtr();
        if (pRcfSession)
        {
            runtimeVersion = pRcfSession->getRuntimeVersion();
        }
        else if (pClientStub)
        {
            runtimeVersion = pClientStub->getRuntimeVersion();
        }
        else
        {
            // This function must be called from within the client-side
            // or server-side portion of a remote call.
            RCF_ASSERT_ALWAYS("");
            runtimeVersion = getRuntimeVersion();
        }

        return runtimeVersion;
    }

} // namespace RCF
