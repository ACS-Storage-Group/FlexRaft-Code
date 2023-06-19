
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

#ifndef INCLUDE_SF_ARCHIVE_HPP
#define INCLUDE_SF_ARCHIVE_HPP

#include <RCF/Export.hpp>
#include <SF/DataPtr.hpp>
#include <RCF/Tools.hpp>

namespace SF {

    class IStream;
    class OStream;

    /// Represents an archive, in which serialized objects are stored.
    class RCF_EXPORT Archive : Noncopyable
    {
    public:

        enum Direction
        {
            READ,
            WRITE
        };

        enum Flag
        {
            PARENT              = 1 << 0,
            POINTER             = 1 << 1,
            NODE_ALREADY_READ   = 1 << 2,
            NO_BEGIN_END        = 1 << 3,
            POLYMORPHIC         = 1 << 4
        };

        Archive(Direction dir, IStream *stream);
        Archive(Direction dir, OStream *stream);

        Archive &   operator&(Flag flag);

        /// Returns true if this archive is being read from.
        bool        isRead() const;

        /// Returns true if this archive is being written to.
        bool        isWrite() const;

        IStream *   getIstream() const;
        OStream *   getOstream() const;
        bool        isFlagSet(Flag flag) const;
        void        setFlag(Flag flag, bool bEnable = true);
        void        clearFlag(Flag flag);
        void        clearState();
        DataPtr &   getLabel();
        bool        verifyAgainstArchiveSize(std::size_t bytesToRead);

        /// Gets the RCF runtime version associated with this archive.
        int         getRuntimeVersion();

        /// Gets the archive version associated with this archive.
        int         getArchiveVersion();

    private:

        Direction       mDir;
        IStream *       mIstream;
        OStream *       mOstream;
        DataPtr         mLabel;
        unsigned int    mFlags;
    };

}

#include <SF/Serializer.hpp>

#endif // ! INCLUDE_SF_ARCHIVE_HPP
