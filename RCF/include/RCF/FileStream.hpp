
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

#ifndef INCLUDE_RCF_FILESTREAM_HPP
#define INCLUDE_RCF_FILESTREAM_HPP

#include <RCF/Config.hpp>

#if RCF_FEATURE_FILETRANSFER==0
#error RCF_FEATURE_FILETRANSFER=1 must be defined
#endif

#include <RCF/ByteBuffer.hpp>
#include <RCF/Exception.hpp>
#include <RCF/FileSystem.hpp>
#include <RCF/Export.hpp>
#include <RCF/RcfFwd.hpp>
#include <RCF/ThreadLibrary.hpp>
#include <RCF/Tools.hpp>

#include <functional>
#include <set>
#include <stdio.h>

#if RCF_FEATURE_BOOST_SERIALIZATION==1
#include <boost/serialization/vector.hpp>
#include <boost/serialization/shared_ptr.hpp>
#endif

namespace SF {

    class Archive;

} // namespace SF

namespace RCF {

    class RCF_EXPORT FileHandle
    {
    public:

        enum OpenMode
        {
            Read,
            WriteTruncate,
            WriteAppend,
        };

        FileHandle();
        FileHandle(const Path& filePath, OpenMode mode);
        ~FileHandle();

        void            open(const Path& filePath, OpenMode mode);
        void            close();
        void            flush();
        void            seek(std::uint64_t newPos);
        std::uint64_t   tell();

        std::size_t     read(const ByteBuffer& buffer);
        std::size_t     write(const ByteBuffer& buffer);    

        Exception       err() const;

        Path            getFilePath();

    private:

        FILE *          mpFile = NULL;
        Path            mFilePath;
        OpenMode        mOpenMode = Read;
        std::size_t     mBeginPos = 0;
        Exception       mErr;
    };

    typedef std::shared_ptr< FileHandle > FileHandlePtr;
    
    class FileUploadInfo;
    typedef std::shared_ptr<FileUploadInfo> FileUploadInfoPtr;

    class FileInfo;
    class FileManifest;
    class FileStreamImpl;

    typedef std::shared_ptr<FileStreamImpl> FileStreamImplPtr;

    class ClientStub;

    class RCF_EXPORT FileInfo
    {
    public:
        FileInfo();

        bool                        mIsDirectory;
        Path                        mFilePath;
        std::uint64_t               mFileStartPos;
        std::uint64_t               mFileSize;
        std::uint32_t               mFileCrc;
        std::string                 mRenameFile;
        std::uint64_t               mLastWriteTime;

#if RCF_FEATURE_SF==1
        void serialize(SF::Archive & ar);
#endif

#if RCF_FEATURE_BOOST_SERIALIZATION==1
        template<typename Archive>
        void serialize(Archive & ar, const unsigned int)
        {
            RCF_UNUSED_VARIABLE(ar);
            RCF_THROW(Exception(RcfError_BSerFileTransferNotSupported));
        }
#endif

    };

    class RCF_EXPORT FileManifest
    {
    public:
        typedef std::vector< FileInfo > Files;
        Files mFiles;

        Path mManifestBase;

        FileManifest();

        FileManifest(const Path& pathToFiles);

        std::uint64_t getTotalByteSize() const;

#if RCF_FEATURE_SF==1
        void serialize(SF::Archive & ar);
#endif

#if RCF_FEATURE_BOOST_SERIALIZATION==1
        template<typename Archive>
        void serialize(Archive & ar, const unsigned int)
        {
            RCF_UNUSED_VARIABLE(ar);
            RCF_THROW(Exception(RcfError_BSerFileTransferNotSupported));
        }
#endif

    };

    class RCF_EXPORT FileStreamImpl : public std::enable_shared_from_this<FileStreamImpl>
    {
    public:
        
        enum Direction
        {
            Unspecified,
            Upload,
            Download
        };


        FileStreamImpl();
        FileStreamImpl(const std::string & filePath);
        FileStreamImpl(const FileManifest & manifest);

        ~FileStreamImpl();

        void serializeGeneric(
            bool isWriting,
            std::function<void(std::uint32_t &, Direction &)> serializeImpl);

#if RCF_FEATURE_SF==1
        void serializeImplSf(SF::Archive & ar, std::uint32_t & transferId, Direction & dir);
        void serialize(SF::Archive & ar);
#endif

#if RCF_FEATURE_BOOST_SERIALIZATION==1

        template<typename Archive>
        void serializeImplBser(Archive & ar, std::uint32_t & transferId, Direction & dir)
        {
            ar & transferId & dir;
        }

        template<typename Archive>
        void serialize(Archive & ar, const unsigned int)
        {
            RCF_UNUSED_VARIABLE(ar);
            RCF_THROW(Exception(RcfError_BSerFileTransferNotSupported));
        }

#endif

        std::string                 mUploadId;
        Path                        mDownloadToPath;
        FileManifest                mManifest;
        std::uint32_t               mTransferRateBps;
        std::uint32_t               mSessionLocalId;

        Direction                   mDirection;
    };

    class RCF_EXPORT FileStream
    {
    protected:

        FileStream();
        FileStream(FileStreamImplPtr implPtr);
        FileStream(const std::string & filePath);
        FileStream(const FileManifest & manifest);
  
    public:

        // Made this inline as it was not being linked in, in DLL builds.
        FileStream & operator=(const FileStream & rhs)
        {
            *mImplPtr = *rhs.mImplPtr;
            return *this;
        }

        // FileStream recipient calls these.
        std::string getLocalPath() const;
        FileManifest & getManifest() const;

        // Client calls these.
        void setDownloadToPath(const std::string & downloadToPath);
        std::string getDownloadToPath() const;

        void setTransferRateBps(std::uint32_t transferRateBps);
        std::uint32_t getTransferRateBps();
        
        void upload(RCF::ClientStub & clientStub);
        void download(RCF::ClientStub & clientStub);


        FileStreamImplPtr mImplPtr;

#if RCF_FEATURE_SF==1
        void serialize(SF::Archive & ar);
#endif

#if RCF_FEATURE_BOOST_SERIALIZATION==1
        template<typename Archive>
        void serialize(Archive & ar, const unsigned int)
        {
            RCF_UNUSED_VARIABLE(ar);
            RCF_THROW(Exception(RcfError_BSerFileTransferNotSupported));
        }
#endif
        
    };

    /// Describes progress of a file download or upload.
    class FileTransferProgress
    {
    public:

        FileTransferProgress();

        /// Local path a file is being downloaded to. Only relevant to downloads.
        Path            mDownloadPath;

        /// Total number of bytes that are to be transferred.
        std::uint64_t   mBytesTotalToTransfer;

        /// Bytes that have been transferred so far.
        std::uint64_t   mBytesTransferredSoFar;

        /// Bandwidth limit (bytes per second) imposed by server, if any.
        std::uint32_t   mServerLimitBps;
    };

    class FileChunk
    {
    public:

        FileChunk();

        bool isEmpty() const;

        std::uint32_t mFileIndex;
        std::uint64_t mOffset;
        ByteBuffer mData;

#if RCF_FEATURE_SF==1
        void serialize(SF::Archive & ar);
#endif

#if RCF_FEATURE_BOOST_SERIALIZATION==1
        template<typename Archive>
        void serialize(Archive & ar, const unsigned int)
        {
            RCF_UNUSED_VARIABLE(ar);
            RCF_THROW(Exception(RcfError_BSerFileTransferNotSupported));
        }
#endif

    };

    class FileTransferRequest
    {
    public:
        FileTransferRequest();

        std::uint32_t mFile;
        std::uint64_t mPos;
        std::uint32_t mChunkSize;

#if RCF_FEATURE_SF==1
        void serialize(SF::Archive & ar);
#endif

#if RCF_FEATURE_BOOST_SERIALIZATION==1
        template<typename Archive>
        void serialize(Archive & ar, const unsigned int)
        {
            RCF_UNUSED_VARIABLE(ar);
            RCF_THROW(Exception(RcfError_BSerFileTransferNotSupported));
        }
#endif

    };

    /// Describes a unit of bandwidth, to be used by downloads or uploads, for a single connection or a group of connections.
    class RCF_EXPORT BandwidthQuota : Noncopyable
    {
    public:
        
        BandwidthQuota();

        /// Creates a bandwidth quota object controlling up to quotaBps bytes per second.
        BandwidthQuota(std::uint32_t quotaBps);

        /// Sets the amount of bandwidth this object controls. Internally synchronized and can be called at any time.
        void setQuota(std::uint32_t quotaBps);

        std::uint32_t calculateLineSpeedLimit();

    private:

        friend class FileUploadInfo;
        friend class FileDownloadInfo;

        void addUpload(FileUploadInfo * pUpload);
        void removeUpload(FileUploadInfo * pUpload);
        void addDownload(FileDownloadInfo * pDownload);
        void removeDownload(FileDownloadInfo * pDownload);       

    private:

        Mutex                           mMutex;
        std::uint32_t                   mQuotaBps;

        std::set<FileDownloadInfo *>    mDownloadsInProgress;
        std::set<FileUploadInfo *>      mUploadsInProgress;
    };

    /// Client side options for downloading and uploading files.
    class FileTransferOptions
    {
    public:
            
        /// Bandwidth limit (bytes/sec).
        std::uint32_t       mBandwidthLimitBps = 0;

        /// Bandwidth quota associated with the transfer.
        BandwidthQuotaPtr   mBandwidthQuotaPtr;

        /// Starting position within the file. Only applicable to downloads.
        std::uint64_t       mStartPos = 0;

        /// Ending position within the file. Only applicable to downloads.
        std::uint64_t       mEndPos = 0;

        /// Progress callback. Called during the transfer to provide progress information.
        FileProgressCallback    mProgressCallback;

        // For test purposes.
        std::uint32_t       mChunkSize = 0;
    };


} // namespace RCF

#endif // ! INCLUDE_RCF_FILESTREAM_HPP
