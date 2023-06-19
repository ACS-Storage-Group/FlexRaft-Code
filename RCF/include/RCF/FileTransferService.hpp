
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

#ifndef INCLUDE_RCF_FILETRANSFERSERVICE_HPP
#define INCLUDE_RCF_FILETRANSFERSERVICE_HPP

#include <map>
#include <set>

#include <RCF/FileStream.hpp>
#include <RCF/RcfFwd.hpp>
#include <RCF/Service.hpp>

#include <SF/vector.hpp>
#include <SF/map.hpp>

#include <RCF/FileSystem.hpp>

#if RCF_FEATURE_SF==1

namespace SF {

    RCF_EXPORT void serialize(SF::Archive &ar, RCF::Path &p);

} // namespace SF

#endif

namespace RCF {

    class FileTransferService;

    class FileUploadInfo;
    class FileDownloadInfo;

    class RcfSession;

    class FileUploadInfo;
    class FileDownloadInfo;

    typedef std::shared_ptr<FileUploadInfo>                   FileUploadInfoPtr;
    typedef std::shared_ptr<FileDownloadInfo>                 FileDownloadInfoPtr;

    typedef std::function<bool(const FileUploadInfo &)>      UploadAccessCallback;
    typedef std::function<bool(const FileDownloadInfo &)>    DownloadAccessCallback;

    class FileIoRequest;
    typedef std::shared_ptr<FileIoRequest> FileIoRequestPtr;
    
    /// Server-side information about a file upload taking place to a RcfServer.
    class FileUploadInfo : Noncopyable
    {
    public:
        FileUploadInfo(BandwidthQuotaPtr quotaPtr);
        ~FileUploadInfo();

        std::string             getUploadId() const;

        FileManifest            mManifest;
        std::uint32_t           mCurrentFile;
        std::uint64_t           mCurrentPos;
        Path                    mUploadDir;

    private:

        friend class FileTransferService;
        
        FileHandlePtr           mFileHandle;
        Path                    mFinalFilePath;
        FileIoRequestPtr        mWriteOp;

        bool                    mCompleted;
        bool                    mResume;
        std::uint32_t           mTimeStampMs;
        
        std::uint32_t           mSessionLocalId;
        std::string             mUploadId;

        BandwidthQuotaPtr       mQuotaPtr;
    };

    /// Server-side information about a file download taking place from a RcfServer.
    class FileDownloadInfo : Noncopyable
    {
    public:
        FileDownloadInfo(BandwidthQuotaPtr quotaPtr);
        ~FileDownloadInfo();
        
        Path                    mDownloadPath;
        FileManifest            mManifest;
        std::uint32_t           mCurrentFile;
        std::uint64_t           mCurrentPos;
        bool                    mCancel;

    private:

        friend class FileTransferService;

        FileHandlePtr           mFileHandle;
        Path                    mFileHandlePath;
        FileIoRequestPtr        mReadOp;
        ByteBuffer              mReadBuffer;
        ByteBuffer              mSendBuffer;
        ByteBuffer              mSendBufferRemaining;

        bool                    mResume;

        Timer                   mTransferWindowTimer;
        std::uint32_t           mTransferWindowBytesSoFar;
        std::uint32_t           mTransferWindowBytesTotal;

        std::uint32_t           mSessionLocalId;
        std::string             mServerDownloadId;

        BandwidthQuotaPtr       mQuotaPtr;
    };

    typedef std::shared_ptr<FileUploadInfo>   FileUploadInfoPtr;
    typedef std::shared_ptr<FileDownloadInfo> FileDownloadInfoPtr;

    class FileChunk;
    class FileTransferRequest;

    class TransferInfo
    {
    public:
        Path                mPath;
        BandwidthQuotaPtr   mBandwidthQuotaPtr;
    };

    class RCF_EXPORT FileTransferService : public I_Service
    {
    public:
        FileTransferService();

        typedef std::function<BandwidthQuotaPtr(RcfSession &)> BandwidthQuotaCallback;
        typedef BandwidthQuotaCallback UploadQuotaCallback;
        typedef BandwidthQuotaCallback DownloadQuotaCallback;

        // For testing.
        void                setTransferWindowS(std::uint32_t transferWindowS);
        std::uint32_t       getTransferWindowS();

        //----------------------------------------------------------------------
        // Remotely accessible.

        void                BeginUpload(
                                const FileManifest & manifest,
                                const std::vector<FileChunk> & chunks,
                                FileChunk & startPos,
                                std::uint32_t & maxMessageLength,
                                std::string & uploadId,
                                std::uint32_t & bps,
                                std::uint32_t sessionLocalId);

        void                UploadChunks(
                                const std::vector<FileChunk> & chunks,
                                std::uint32_t & bps);

        void                BeginDownload(
                                FileManifest & manifest,
                                const FileTransferRequest & request,
                                std::vector<FileChunk> & chunks,
                                std::uint32_t & maxMessageLength,
                                std::uint32_t & bps,
                                std::uint32_t sessionLocalId,
                                const std::string & serverDownloadId);

        void                TrimDownload(
                                const FileChunk & startPos);

        void                DownloadChunks(
                                const FileTransferRequest & request,
                                std::vector<FileChunk> & chunks,
                                std::uint32_t & adviseWaitMs,
                                std::uint32_t & bps);

        //----------------------------------------------------------------------

    private:

        friend class RcfServer;
        friend class RcfSession;

        void                addFileTransfer(const std::string & transferId, const TransferInfo & transferInfo);
        void                removeFileTransfer(const std::string & transferId);
        TransferInfo        findFileTransfer(const std::string & transferId);

        Mutex                               mFileTransfersInProgressMutex;
        std::map<std::string, TransferInfo> mFileTransfersInProgress;

        void                checkForUploadCompletion(FileUploadInfoPtr uploadInfoPtr);



        void                onServerStart(RcfServer & server);
        void                onServerStop(RcfServer & server);

        Path                        mUploadDirectory;

        DownloadProgressCallback    mDownloadProgressCb;
        UploadProgressCallback      mUploadProgressCb;

        std::uint32_t               mTransferWindowS;

        BandwidthQuotaPtr           mUploadQuota;
        BandwidthQuotaPtr           mDownloadQuota;

        BandwidthQuotaCallback      mUploadQuotaCallback;
        BandwidthQuotaCallback      mDownloadQuotaCallback;
    };

    typedef std::shared_ptr<FileTransferService> FileTransferServicePtr;

} // namespace RCF

#endif // ! INCLUDE_RCF_FILETRANSFERSERVICE_HPP
