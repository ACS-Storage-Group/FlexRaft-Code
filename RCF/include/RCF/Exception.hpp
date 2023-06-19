
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

#ifndef INCLUDE_RCF_EXCEPTION_HPP
#define INCLUDE_RCF_EXCEPTION_HPP

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <cstdint>

#include <RCF/Export.hpp>
#include <RCF/Config.hpp>

#include <RCF/ErrorMsg.hpp>
#include <RCF/MemStream.hpp>

namespace SF {
    class Archive;
}

namespace RCF {

    RCF_EXPORT std::string osError(int err);

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable:4100)
#pragma warning(disable:4267)
#endif

    template<typename T>
    std::string convertToString(T t)
    {
        MemOstream os;
        os << t;
        return os.string();
    }

#ifdef _MSC_VER
#pragma warning(pop)
#endif

    RCF_EXPORT bool shouldDisconnectOnRemoteError(int errorId);
    RCF_EXPORT int getRuntimeVersionOfThisRemoteCall();

    /// Base class for all RCF exceptions.
    class RCF_EXPORT Exception : public std::exception
    {
    public:
        Exception();

        Exception(const std::string & errorMsg);

        Exception(
            const ErrorMsg& msg,
            const std::string &     arg1 = "",
            const std::string &     arg2 = "",
            const std::string &     arg3 = "");

        template<typename T1>
        Exception(
            const ErrorMsg& msg,
            const T1& arg1)
        {
            init(msg, convertToString(arg1), "", "");
        }

        template<typename T1, typename T2>
        Exception(
            const ErrorMsg& msg,
            const T1& arg1,
            const T2& arg2)
        {
            init(msg, convertToString(arg1), convertToString(arg2), "");
        }

        template<typename T1, typename T2, typename T3>
        Exception(
            const ErrorMsg& msg,
            const T1& arg1,
            const T2& arg2,
            const T3& arg3)
        {
            init(msg, convertToString(arg1), convertToString(arg2), convertToString(arg3));
        }

        void init(
            const ErrorMsg&         msg,
            const std::string &     arg1 = "",
            const std::string &     arg2 = "",
            const std::string &     arg3 = "");


        ~Exception() throw();

        /// Deep clone of the exception.
        virtual std::unique_ptr<Exception> clone() const;

        /// Returns true if there is an error condition. Otherwise returns false.
        bool            good() const;

        /// Returns true if there is not an error condition. Otherwise returns false.
        bool            bad() const;

        /// Clears the error condition.
        void            clear();

        /// Returns an error message.
        const char *    what()                  const throw();

        /// Returns the RCF error ID of the error.
        int             getErrorId()            const;

        /// Returns an error message.
        std::string     getErrorMessage()        const;

        bool            getShouldRetry()        const;
        void            setShouldRetry(bool shouldRetry);

        virtual void    throwSelf() const;

        // Deprecated.
        std::string     getErrorString()        const;

        // protected to make serialization of RemoteException simpler
    protected:

        int                     mErrorId = 0;
        std::string             mErrorString;
        bool                    mShouldRetry = false;
    };

    typedef std::shared_ptr<Exception> ExceptionPtr;

    /// Represents an error that occurs on a RCF server and is transmitted back to the client.
    class RCF_EXPORT RemoteException : public Exception
    {
    public:

        RemoteException();
        RemoteException(const Exception& e);
        RemoteException(const std::string & errorMsg);

        RemoteException(
            const ErrorMsg& msg,
            const std::string &     arg1 = "",
            const std::string &     arg2 = "",
            const std::string &     arg3 = "");

        ~RemoteException() throw();

#if RCF_FEATURE_SF==1

        void serialize(SF::Archive & ar);

#endif

#if RCF_FEATURE_BOOST_SERIALIZATION==1
        template<typename Archive>
        void serialize(Archive &ar, const unsigned int)
        {
            ar
                & mErrorId
                & mErrorString;
        }
#endif

        std::unique_ptr<Exception> clone() const;

        void throwSelf() const;

    private:
        std::string mRemoteExceptionType;
    };

    /// Represents a versioning error raised by a server. Allows the server to specify runtime and archive versions.
    class RCF_EXPORT VersioningException : public RemoteException
    {
    public:
        VersioningException(
            std::uint32_t runtimeVersion, 
            std::uint32_t archiveVersion);

        ~VersioningException() throw();
        
        /// Returns the runtime version requested by the server.
        std::uint32_t               getRuntimeVersion() const;

        /// Returns the archive version requested by the server.
        std::uint32_t               getArchiveVersion() const;

        std::unique_ptr<Exception>  clone() const;
        void                        throwSelf() const;

    private:
        std::uint32_t mRuntimeVersion;
        std::uint32_t mArchiveVersion;
    };

} // namespace RCF

#endif // ! INCLUDE_RCF_EXCEPTION_HPP
