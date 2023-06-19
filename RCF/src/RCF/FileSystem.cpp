
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

#include <RCF/FileSystem.hpp>

#include <RCF/BsdSockets.hpp>
#include <RCF/ErrorMsg.hpp>

#include <sys/stat.h>
#ifdef RCF_WINDOWS
#include <sys/utime.h>
#else
#include <utime.h>
#endif

namespace RCF
{

    namespace fs = RCF_FILESYSTEM_NS;

    Path makeCanonical(const Path& p)
    {
        //return fs::canonical(p);

        bool isUncPath = false;
        if ( p.u8string().substr(0, 2) == "//" )
        {
            isUncPath = true;
        }

        Path abs_p = p;

        Path result;
        for ( Path::iterator it = abs_p.begin();
        it != abs_p.end();
            ++it )
        {
            if ( *it == ".." )
            {
                // /a/b/.. is not necessarily /a if b is a symbolic link
                if ( fs::is_symlink(result) )
                {
                    result /= *it;
                }
                // /a/b/../.. is not /a/b/.. under most circumstances
                // We can end up with ..s in our result because of symbolic links
                else if ( result.filename() == ".." )
                {
                    result /= *it;
                }
                // Otherwise it should be safe to resolve the parent
                else
                {
                    result = result.parent_path();
                }
            }
            else if ( *it == "." )
            {
                // Ignore
            }
            else
            {
                // Just cat other path entries
                result /= *it;
            }
        }

        // Code above collapses the leading double slash for a UNC path. So here we put it back in.
        if ( isUncPath )
        {
            result = Path(*result.begin()) / result;
        }

        return result;
    }

    // C++ standard is very hazy on file modification times, so we just use the old C functions.

#ifdef RCF_WINDOWS

#ifdef _UNICODE

// Windows unicode build
#define RCF_st      _stat
#define RCF_stat    _wstat
#define RCF_ut      _utimbuf
#define RCF_utime   _wutime

#else

// Windows non-Unicode build
#define RCF_st      _stat
#define RCF_stat    _stat
#define RCF_ut      _utimbuf
#define RCF_utime   _utime

#endif

#else

// Unix build
#define RCF_st      stat
#define RCF_stat    stat
#define RCF_ut      utimbuf
#define RCF_utime   utime

#endif

    void setLastWriteTime(const Path& p, std::uint64_t writeTime)
    {
        struct RCF_st buf = { 0 };
        int ret = RCF_stat(RCF::toTstring(p).c_str(), &buf);
        if (ret != 0)
        {
            int err = Platform::OS::BsdSockets::GetLastError();
            throw RCF::Exception(RcfError_SetFileModTime, p.u8string(), Platform::OS::GetErrorString(err));
        }

        std::uint64_t createTime = buf.st_atime;

        struct RCF_ut ut = { 0 };
        ut.actime = createTime;
        ut.modtime = writeTime;
        if (0 != RCF_utime(RCF::toTstring(p).c_str(), &ut))
        {
            int err = Platform::OS::BsdSockets::GetLastError();
            throw RCF::Exception(RcfError_SetFileModTime, p.u8string(), Platform::OS::GetErrorString(err));
        }
    }

    std::uint64_t getLastWriteTime(const Path& p)
    {
        struct RCF_st buf = { 0 };
        int ret = RCF_stat(RCF::toTstring(p).c_str(), &buf);
        if (ret != 0)
        {
            int err = Platform::OS::BsdSockets::GetLastError();
            throw RCF::Exception(RcfError_GetFileModTime, p.u8string(), Platform::OS::GetErrorString(err));
        }

        std::uint64_t writeTime = (std::uint64_t) buf.st_mtime;
        return writeTime;
    }
    
}
