
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

#include <RCF/FileTransferService.hpp>

#include <functional>

#include <cstdio>
#include <iomanip>

#include <sys/stat.h>

#include <RCF/Config.hpp>
#include <RCF/Exception.hpp>
#include <RCF/FileIoThreadPool.hpp>
#include <RCF/FileTransferInterface.hpp>
#include <RCF/ObjectPool.hpp>
#include <RCF/RcfServer.hpp>
#include <RCF/RcfSession.hpp>
#include <RCF/ServerTransport.hpp>
#include <RCF/ThreadLocalData.hpp>
#include <RCF/Log.hpp>
#include <RCF/Uuid.hpp>

#include <RCF/FileSystem.hpp>

namespace SF {

#if RCF_FEATURE_SF==1

    void serialize(SF::Archive &ar, RCF::Path &p)
    {
        if (ar.isWrite())
        {
            if ( ar.getRuntimeVersion() <= 12 )
            {
                std::string s = p.u8string();
                ar & s;
            }
            else
            {
                std::wstring s = p.wstring();
                ar & s;
            }
        }
        else
        {
            if ( ar.getRuntimeVersion() <= 12 )
            {
                std::string s;
                ar & s;
                p = RCF::Path(s);
            }
            else
            {
                std::wstring ws;
                ar & ws;
                p = RCF::Path(ws);
            }
        }
    }

#endif

}

namespace RCF {

    namespace fs = RCF_FILESYSTEM_NS;

    FileTransferService::FileTransferService() :
            mUploadDirectory(),
            mTransferWindowS(5)            
    {
    }

    namespace fs = RCF_FILESYSTEM_NS;

    void FileTransferService::checkForUploadCompletion(FileUploadInfoPtr uploadInfoPtr)
    {
        FileUploadInfo & uploadInfo = *uploadInfoPtr;

        if (uploadInfo.mCurrentFile == uploadInfo.mManifest.mFiles.size())
        {
            RCF_LOG_3()(uploadInfo.mCurrentFile) 
                << "FileTransferService - upload completed.";

            uploadInfo.mCompleted = true;

            if (uploadInfo.mSessionLocalId)
            {
                RcfSession& session = getTlsRcfSession();
                Lock lock(session.mMutex);
                session.mSessionUploads[uploadInfo.mSessionLocalId] = uploadInfoPtr;
                session.mUploadInfoPtr.reset();
            }

            if (uploadInfo.mUploadId.size() > 0)
            {
                removeFileTransfer(uploadInfo.mUploadId);
            }
        }
    }

    void getFilePathsForUpload(const Path& uploadDir, const std::string& uploadId, const FileManifest& manifest, Path & uploadFilePath, Path & finalFilePath)
    {

        FileInfo file = manifest.mFiles[0];
        Path uploadFileName = uploadId;
        uploadFileName += ".";
        uploadFileName += file.mFilePath.stem().wstring().substr(0, 50);
        uploadFileName += file.mFilePath.extension().wstring().substr(0,10);
        finalFilePath = uploadDir / uploadFileName;

        // Add ".tmp" extension while it is being uploaded.
        uploadFilePath = finalFilePath;
        uploadFilePath += ".tmp";
    }

    void FileTransferService::BeginUpload(
        const FileManifest & manifest,
        const std::vector<FileChunk> & chunks,
        FileChunk & startPos,
        std::uint32_t & maxMessageLength,
        std::string & uploadId,
        std::uint32_t & bps,
        std::uint32_t sessionLocalId)
    {

        RCF_LOG_3()(sessionLocalId)(uploadId) << "FileTransferService::BeginUpload() - entry.";

        RCF_UNUSED_VARIABLE(chunks);

        namespace fs = RCF_FILESYSTEM_NS;

        NetworkSession & networkSession = getTlsRcfSession().getNetworkSession();
        maxMessageLength = (std::uint32_t) networkSession.getServerTransport().getMaxIncomingMessageLength();

        RCF::BandwidthQuotaPtr quotaPtr = mUploadQuotaCallback ? 
            mUploadQuotaCallback(RCF::getCurrentRcfSession()) : 
            mUploadQuota;

        FileUploadInfoPtr uploadInfoPtr( new FileUploadInfo(quotaPtr) );
        uploadInfoPtr->mManifest = manifest;
        uploadInfoPtr->mTimeStampMs = RCF::getCurrentTimeMs();
        uploadInfoPtr->mSessionLocalId = sessionLocalId;

        if (uploadId.size() > 0)
        {
            Path uploadPath = findFileTransfer(uploadId).mPath;
            if (!uploadPath.empty())
            {
                uploadInfoPtr->mUploadDir = uploadPath;

                // Trim the manifest to account for already uploaded fragments.
                const_cast<FileManifest &>(manifest).mManifestBase = uploadPath;

                std::uint64_t bytesAlreadyTransferred = 0;

                Path uploadTempPath;
                Path finalPath;
                getFilePathsForUpload(mUploadDirectory, uploadId, manifest, uploadTempPath, finalPath);
                if ( fs::exists(uploadTempPath) )
                {
                    std::uint64_t fileSize = fs::file_size(uploadTempPath);
                    if ( fileSize <= manifest.mFiles[0].mFileSize )
                    {
                        bytesAlreadyTransferred = fileSize;
                    }
                }

                startPos.mOffset = bytesAlreadyTransferred;
                uploadInfoPtr->mResume = true;
            }
        }

        if (uploadInfoPtr->mUploadDir.empty())
        {
            if ( mUploadDirectory.empty() )
            {
                RCF_THROW(Exception(RcfError_UploadDirectory));
            }

            uploadId = generateUuid();

            Path uploadTempPath;
            Path finalPath;
            getFilePathsForUpload(mUploadDirectory, uploadId, manifest, uploadTempPath, finalPath);

            TransferInfo info;
            info.mPath = finalPath;
            addFileTransfer(uploadId, info);
            startPos = FileChunk();
        }

        uploadInfoPtr->mUploadId = uploadId;
        uploadInfoPtr->mCurrentFile = startPos.mFileIndex;
        uploadInfoPtr->mCurrentPos = startPos.mOffset;

        {
            RcfSession& session = getTlsRcfSession();
            Lock lock(session.mMutex);
            session.mUploadInfoPtr = uploadInfoPtr;
        }

        bps = uploadInfoPtr->mQuotaPtr->calculateLineSpeedLimit();

        checkForUploadCompletion(uploadInfoPtr);

        if (mUploadProgressCb)
        {
            mUploadProgressCb(getCurrentRcfSession(), *uploadInfoPtr);
        }

        RCF_LOG_3()(startPos.mFileIndex)(startPos.mOffset)(maxMessageLength)(uploadId)(bps)(uploadId)(uploadInfoPtr->mUploadDir.u8string())
            << "FileTransferService::BeginUpload() - exit.";
    }

    void FileTransferService::UploadChunks(
        const std::vector<FileChunk> & chunks,
        std::uint32_t & bps)
    {
        RCF_LOG_3()(chunks.size()) 
            << "FileTransferService::UploadChunks() - entry.";

        namespace fs = RCF_FILESYSTEM_NS;

        // Find the upload.
        FileUploadInfoPtr uploadInfoPtr = getTlsRcfSession().mUploadInfoPtr;
        
        if (!uploadInfoPtr)
        {
            RCF_THROW( Exception(RcfError_NoUpload) );
        }

        FileUploadInfo & uploadInfo = * uploadInfoPtr;

        if (uploadInfo.mCompleted)
        {
            RCF_THROW( Exception(RcfError_UploadAlreadyCompleted) );
        }
        
        RCF::BandwidthQuotaPtr quotaPtr = uploadInfo.mQuotaPtr;
        bps = quotaPtr->calculateLineSpeedLimit();

        if ( chunks.size() != 1 )
        {
            Exception e("UploadChunks() no longer supports more than 1 chunk.");
            RCF_THROW(e);
        }

        const FileChunk & chunk = chunks[0];

        if (chunk.mFileIndex != uploadInfo.mCurrentFile)
        {
            if (chunk.mFileIndex != uploadInfo.mCurrentFile)
            {
                RCF_THROW( Exception(RcfError_FileIndex, uploadInfo.mCurrentFile, chunk.mFileIndex) );
            }
        }

        RCF_ASSERT( chunk.mFileIndex == uploadInfo.mCurrentFile );

        FileInfo file = uploadInfo.mManifest.mFiles[uploadInfo.mCurrentFile];

        // Initial open of file.
        if (uploadInfo.mCurrentPos == 0 || uploadInfo.mResume)
        {
            RCF_ASSERT(uploadInfo.mWriteOp->isCompleted());

            Path filePath;
            Path finalFilePath;
            getFilePathsForUpload(mUploadDirectory, uploadInfo.mUploadId, uploadInfo.mManifest, filePath, finalFilePath);
            uploadInfo.mFinalFilePath = finalFilePath;

            RCF::Path folder = filePath.parent_path();
            if ( !fs::exists( folder ) )
            {
                
                RCF_LOG_3()(filePath.u8string())(folder.u8string()) << "FileTransferService::UploadChunks() - creating folder.";
                fs::create_directories( folder );
            }

            RCF_LOG_3()(uploadInfo.mCurrentFile)(filePath.u8string())
                << "FileTransferService::UploadChunks() - opening file.";

            if (uploadInfo.mResume)
            {
                uploadInfo.mFileHandle->open(filePath, FileHandle::WriteAppend);
            }
            else
            {
                uploadInfo.mFileHandle->open(filePath, FileHandle::WriteTruncate);
            }

            if (uploadInfo.mResume && uploadInfo.mCurrentPos > 0)
            {
                RCF_LOG_3()(uploadInfo.mCurrentFile)(uploadInfo.mCurrentPos) 
                    << "FileTransferService::UploadChunks() - seeking in file.";

                uploadInfo.mFileHandle->seek(uploadInfo.mCurrentPos);
            }

            uploadInfo.mResume = false;
        }

        FileHandlePtr fout = uploadInfoPtr->mFileHandle;

        // Wait for previous write to complete.
        if (uploadInfo.mWriteOp->isInitiated())
        {
            uploadInfo.mWriteOp->complete();

            std::uint64_t bytesWritten = uploadInfo.mWriteOp->getBytesTransferred();
            if (bytesWritten == 0)
            {
                Exception e = fout->err();
                fout->close();
                RCF_ASSERT(e.bad());
                RCF_THROW(e);
            }
        }

        // Initiate next write.


        // Check the offset position.
        uploadInfo.mCurrentPos = fout->tell();
        if (chunk.mOffset != uploadInfo.mCurrentPos)
        {
            RCF_THROW( Exception(RcfError_FileOffset, uploadInfo.mCurrentPos, chunk.mOffset) );
        }

        // Check the chunk size.
        std::uint64_t fileSize = file.mFileSize;
        std::uint64_t remainingFileSize = fileSize - uploadInfo.mCurrentPos;
        if (chunk.mData.getLength() > remainingFileSize)
        {
            RCF_THROW( Exception(RcfError_UploadFileSize) );
        }

        uploadInfo.mWriteOp->initateWrite(uploadInfo.mFileHandle, chunk.mData);

        uploadInfoPtr->mTimeStampMs = RCF::getCurrentTimeMs();

        // Check if last chunk.
        uploadInfo.mCurrentPos += chunk.mData.getLength();
        if (uploadInfo.mCurrentPos == fileSize)
        {
            RCF_LOG_3()(uploadInfo.mCurrentFile) 
                << "FileTransferService::UploadChunks() - closing file.";

            uploadInfo.mWriteOp->complete();
            fout->close();

            // Rename to drop the ".tmp" extension.
            fs::rename(fout->getFilePath(), uploadInfo.mFinalFilePath);

            setLastWriteTime(uploadInfo.mFinalFilePath, file.mLastWriteTime);
                
            ++uploadInfo.mCurrentFile;
            uploadInfo.mCurrentPos = 0;

            checkForUploadCompletion(uploadInfoPtr);                
        }

        if (mUploadProgressCb)
        {
            mUploadProgressCb(getCurrentRcfSession(), uploadInfo);
        }

        RCF_LOG_3() << "FileTransferService::UploadChunks() - exit.";
    }

    namespace fs = RCF_FILESYSTEM_NS;

    void FileTransferService::addFileTransfer(const std::string & transferId, const TransferInfo & transferInfo)
    {
        RCF_LOG_3()(transferId)(transferInfo.mPath.u8string()) << "FileTransferService::addFileTransfer().";

        Lock lock(mFileTransfersInProgressMutex);
        mFileTransfersInProgress[transferId] = transferInfo;
    }

    void FileTransferService::removeFileTransfer(const std::string & transferId)
    {
        RCF_LOG_3()(transferId) << "FileTransferService::removeFileTransfer().";

        Lock lock(mFileTransfersInProgressMutex);
        auto iter = mFileTransfersInProgress.find(transferId);
        if (iter != mFileTransfersInProgress.end())
        {
            mFileTransfersInProgress.erase(iter);
        }
    }

    TransferInfo FileTransferService::findFileTransfer(const std::string & transferId)
    {
        Lock lock(mFileTransfersInProgressMutex);
        auto iter = mFileTransfersInProgress.find(transferId);
        if (iter != mFileTransfersInProgress.end())
        {
            return iter->second;
        }
        return TransferInfo();
    }

    void FileTransferService::BeginDownload(
        FileManifest & manifest,
        const FileTransferRequest & request,
        std::vector<FileChunk> & chunks,
        std::uint32_t & maxMessageLength,
        std::uint32_t & bps,
        std::uint32_t sessionLocalId,
        const std::string & serverDownloadId)
    {
        RCF_LOG_3()(sessionLocalId)(serverDownloadId) << "FileTransferService::BeginDownload() - entry.";

        FileDownloadInfoPtr downloadInfoPtr;

        if ( sessionLocalId || serverDownloadId.size() > 0 )
        {
            BandwidthQuotaPtr quotaPtr = mDownloadQuotaCallback ? 
                mDownloadQuotaCallback(RCF::getCurrentRcfSession()) : 
                mDownloadQuota;

            RcfSession& session = getTlsRcfSession();
            Lock lock(session.mMutex);
            
            if ( sessionLocalId )
            {
                downloadInfoPtr.reset(new FileDownloadInfo(quotaPtr));
                FileStream & fs = session.mSessionDownloads[sessionLocalId];
                downloadInfoPtr->mManifest = fs.mImplPtr->mManifest;
            }
            else if ( serverDownloadId.size() > 0 )
            {
                TransferInfo info = findFileTransfer(serverDownloadId);
                Path downloadPath = info.mPath;
                if ( info.mBandwidthQuotaPtr )
                {
                    quotaPtr = info.mBandwidthQuotaPtr;
                }
                downloadInfoPtr.reset(new FileDownloadInfo(quotaPtr));
                downloadInfoPtr->mManifest.mManifestBase = downloadPath.parent_path();
                FileInfo fileInfo;
                fileInfo.mFilePath = downloadPath.filename();
                fileInfo.mIsDirectory = false;
                fileInfo.mLastWriteTime = getLastWriteTime(downloadPath);
                fileInfo.mFileSize = fs::file_size(downloadPath);
                downloadInfoPtr->mManifest.mFiles.push_back(fileInfo);
            }
            downloadInfoPtr->mDownloadPath = downloadInfoPtr->mManifest.mManifestBase;
            downloadInfoPtr->mSessionLocalId = sessionLocalId;
            downloadInfoPtr->mServerDownloadId = serverDownloadId;
            session.mDownloadInfoPtr = downloadInfoPtr;
        }
        else
        {
            RCF_THROW( Exception(RcfError_NoDownload) );
        }
        
        FileDownloadInfo & di = * downloadInfoPtr;

        manifest = di.mManifest;
        di.mCurrentFile = 0;
        di.mCurrentPos = 0;
        if (!di.mManifest.mFiles.empty())
        {
            di.mCurrentPos = di.mManifest.mFiles[0].mFileStartPos;
        }

        bps = di.mQuotaPtr->calculateLineSpeedLimit();

        // TODO: optional first chunks.
        RCF_UNUSED_VARIABLE(request);
        chunks.clear();

        NetworkSession & networkSession = getTlsRcfSession().getNetworkSession();
        maxMessageLength = (std::uint32_t) networkSession.getServerTransport().getMaxIncomingMessageLength();

        if (mDownloadProgressCb)
        {
            mDownloadProgressCb(getCurrentRcfSession(), di);
        }

        RCF_LOG_3()(manifest.mFiles.size())(maxMessageLength) 
            << "FileTransferService::BeginDownload() - exit.";
    }

    void FileTransferService::TrimDownload(
        const FileChunk & startPos)
    {
        RCF_LOG_3()(startPos.mFileIndex)(startPos.mOffset) 
            << "FileTransferService::TrimDownload() - entry.";

        FileDownloadInfoPtr downloadInfoPtr = getTlsRcfSession().mDownloadInfoPtr;

        if (!downloadInfoPtr)
        {
            RCF_THROW( Exception(RcfError_NoDownload) );
        }

        FileDownloadInfo & di = * downloadInfoPtr;

        RCF_ASSERT(startPos.mFileIndex <= di.mManifest.mFiles.size());
        if (startPos.mFileIndex < di.mManifest.mFiles.size())
        {
            RCF_ASSERT(startPos.mOffset <= di.mManifest.mFiles[startPos.mFileIndex].mFileSize);
        }
        else
        {
            RCF_ASSERT(startPos.mOffset == 0);
        }

        di.mCurrentFile = startPos.mFileIndex;
        di.mCurrentPos = startPos.mOffset;
        di.mResume = true;

        RCF_LOG_3() << "FileTransferService::TrimDownload() - exit.";
    }

    void FileTransferService::DownloadChunks(
        const FileTransferRequest & request,
        std::vector<FileChunk> & chunks,
        std::uint32_t & adviseWaitMs,
        std::uint32_t & bps)
    {
        RCF_LOG_3()(request.mFile)(request.mPos)(request.mChunkSize) 
            << "FileTransferService::DownloadChunks() - entry.";

        // Find the download.
        FileDownloadInfoPtr & diPtr = getTlsRcfSession().mDownloadInfoPtr;

        if (!diPtr)
        {
            RCF_THROW( Exception(RcfError_NoDownload) );
        }

        FileDownloadInfo & di = *diPtr;

        if (di.mCancel)
        {
            // TODO: reset mDownloadInfoPtr?
            RCF_THROW( Exception(RcfError_DownloadCancelled) );
        }

        adviseWaitMs = 0;

        if (request.mPos != di.mCurrentPos)
        {
            RCF_THROW( Exception(RcfError_FileOffset, di.mCurrentPos, request.mPos) );
        }

        chunks.clear();

        std::uint32_t chunkSize = request.mChunkSize;

        // Trim the chunk size, according to throttle settings.
        bps = di.mQuotaPtr->calculateLineSpeedLimit();

        if (bps)
        {
            RCF_LOG_3()(bps)(mTransferWindowS)(di.mTransferWindowBytesTotal)(di.mTransferWindowBytesSoFar) 
                << "FileTransferService::DownloadChunks() - checking throttle setting.";

            if (di.mTransferWindowTimer.elapsed(mTransferWindowS*1000))
            {
                RCF_ASSERT(di.mTransferWindowBytesTotal >= di.mTransferWindowBytesSoFar);

                std::uint32_t carryOver = 
                    di.mTransferWindowBytesTotal - di.mTransferWindowBytesSoFar;

                di.mTransferWindowTimer.restart();

                di.mTransferWindowBytesTotal = bps * mTransferWindowS;
                di.mTransferWindowBytesTotal += carryOver;

                di.mTransferWindowBytesSoFar = 0;

                RCF_LOG_3()(mTransferWindowS)(di.mTransferWindowBytesTotal)(di.mTransferWindowBytesSoFar)(carryOver) 
                    << "FileTransferService::DownloadChunks() - new throttle transfer window.";
            }

            if (di.mTransferWindowBytesTotal == 0)
            {
                di.mTransferWindowBytesTotal = bps * mTransferWindowS;
            }

            std::uint32_t bytesWindowRemaining = 
                di.mTransferWindowBytesTotal - di.mTransferWindowBytesSoFar;

            if (bytesWindowRemaining < chunkSize)
            {
                std::uint32_t windowStartMs = di.mTransferWindowTimer.getStartTimeMs();
                std::uint32_t windowEndMs = windowStartMs + 1000*mTransferWindowS;
                std::uint32_t nowMs = getCurrentTimeMs();
                if (nowMs < windowEndMs)
                {
                    adviseWaitMs = windowEndMs - nowMs;

                    RCF_LOG_3()(adviseWaitMs) 
                        << "FileTransferService::DownloadChunks() - advising client wait.";
                }
            }

            RCF_LOG_3()(chunkSize)(bytesWindowRemaining)(di.mTransferWindowBytesTotal) 
                << "FileTransferService::DownloadChunks() - trimming chunk size to transfer window.";

            chunkSize = RCF_MIN(chunkSize, bytesWindowRemaining);
        }

        std::uint32_t totalBytesRead = 0;

        FileInfo & currentFileInfo = di.mManifest.mFiles[di.mCurrentFile];

        // Initial open of file.
        if (di.mCurrentPos == 0 || di.mResume)
        {
            di.mResume = false;

            Path manifestBase = di.mDownloadPath;
            Path filePath = currentFileInfo.mFilePath;
            Path totalPath = manifestBase / filePath;
            std::uint64_t fileSize = currentFileInfo.mFileSize;

            RCF_LOG_3()(di.mCurrentFile)(fileSize)(totalPath.u8string())
                << "FileTransferService::DownloadChunks() - opening file.";

            di.mFileHandlePath = totalPath;

            di.mFileHandle->open(totalPath, FileHandle::Read);

            if (di.mCurrentPos != 0)
            {
                RCF_LOG_3()(di.mCurrentFile)(di.mCurrentPos) 
                    << "FileTransferService::DownloadChunks() - seeking in file.";

                di.mFileHandle->seek(di.mCurrentPos);
            }
        }
            
        std::uint64_t fileSize = currentFileInfo.mFileSize;
        std::uint64_t bytesRemainingInFile =  fileSize - di.mCurrentPos;
        std::uint64_t bytesRemainingInChunk = chunkSize - totalBytesRead;

        // Check if an async read is in progress.
        if (di.mReadOp->isInitiated())
        {
            RCF_LOG_3() 
                << "FileTransferService::DownloadChunks() - completing read.";

            // Wait for async read to complete.
            di.mReadOp->complete();

            std::size_t bytesRead = static_cast<std::size_t>(
                di.mReadOp->getBytesTransferred());

            if (bytesRead == 0)
            {
                Exception e = di.mFileHandle->err();
                RCF_ASSERT(e.bad());
                RCF_THROW(e);
            }
            di.mSendBuffer.swap(di.mReadBuffer);
            di.mSendBufferRemaining = ByteBuffer(di.mSendBuffer, 0, bytesRead);

            RCF_LOG_3()(bytesRead) 
                << "FileTransferService::DownloadChunks() - read completed.";
        }

        ByteBuffer byteBuffer;
        FileHandlePtr fin = di.mFileHandle;

        if (di.mSendBufferRemaining)
        {
            // Asynchronously read data available.
            std::size_t bytesToRead = RCF_MIN(
                di.mSendBufferRemaining.getLength(), 
                static_cast<std::size_t>(bytesRemainingInChunk));

            byteBuffer = ByteBuffer(di.mSendBufferRemaining, 0, bytesToRead);
            di.mSendBufferRemaining = ByteBuffer(di.mSendBufferRemaining, bytesToRead);
        }
        else
        {
            // No asynchronously read data available. Do a synchronous read.
            byteBuffer = ByteBuffer( static_cast<std::size_t>( 
                RCF_MIN(bytesRemainingInChunk, bytesRemainingInFile) ));

            RCF_LOG_3()(di.mCurrentFile)(byteBuffer.getLength()) 
                << "FileTransferService::DownloadChunks() - reading from file.";

            std::size_t bytesRead = fin->read(byteBuffer);

            byteBuffer = ByteBuffer(byteBuffer, 0, bytesRead);
        }

        // Setup the chunk to return to the client.
        FileChunk fileChunk;
        fileChunk.mFileIndex = di.mCurrentFile;
        fileChunk.mOffset = di.mCurrentPos;
        fileChunk.mData = byteBuffer;
        chunks.push_back(fileChunk);

        totalBytesRead += (std::uint32_t) byteBuffer.getLength();
        diPtr->mCurrentPos += byteBuffer.getLength();

        // Close the file if we got to the end.
        if (diPtr->mCurrentPos == currentFileInfo.mFileSize)
        {
            RCF_LOG_3()(diPtr->mCurrentFile) 
                << "FileTransferService::DownloadChunks() - closing file.";

            fin->close();
            ++di.mCurrentFile;
            di.mCurrentPos = 0;
        }

        di.mTransferWindowBytesSoFar += totalBytesRead;

        // If we got to the end of the file, clean up the download.
        if (di.mCurrentFile == di.mManifest.mFiles.size())
        {
            RCF_LOG_3()(di.mCurrentFile) 
                << "FileTransferService::DownloadChunks() - download completed.";

            // TODO: this is broken if there is more than one FileStream.
            if (diPtr->mSessionLocalId)
            {
                std::map<std::uint32_t, FileDownload> & downloads = 
                    getTlsRcfSession().mSessionDownloads;

                std::map<std::uint32_t, FileDownload>::iterator iter = 
                    downloads.find(diPtr->mSessionLocalId);

                RCF_ASSERT(iter != downloads.end());

                downloads.erase(iter);
            }
            else if ( diPtr->mServerDownloadId.size() > 0 )
            {
                removeFileTransfer(diPtr->mServerDownloadId);
            }
            diPtr.reset();
        }

        // Initiate read for next chunk.
        if (    diPtr.get()
            &&  di.mSendBufferRemaining.isEmpty()
            &&  di.mCurrentFile < di.mManifest.mFiles.size()
            &&  0 < di.mCurrentPos
            &&  ! di.mReadOp->isInitiated())
        {
            fileSize = di.mManifest.mFiles[di.mCurrentFile].mFileSize;
            if (di.mCurrentPos < fileSize)
            {
                if (di.mReadBuffer.isEmpty())
                {
                    RCF_ASSERT(di.mSendBuffer.isEmpty());
                    RCF_ASSERT(di.mSendBufferRemaining.isEmpty());

                    di.mReadBuffer = ByteBuffer(request.mChunkSize);
                    di.mSendBuffer = ByteBuffer(request.mChunkSize);
                }

                std::size_t bytesToRead = static_cast<std::size_t>(
                    fileSize - di.mCurrentPos);

                bytesToRead = RCF_MIN(bytesToRead, di.mReadBuffer.getLength());

                RCF_LOG_3()(di.mCurrentFile)(di.mCurrentPos)(fileSize)(bytesToRead) 
                    << "FileTransferService::DownloadChunks() - initiate read for next chunk.";

                di.mReadOp->initiateRead(di.mFileHandle, ByteBuffer(di.mReadBuffer, 0, bytesToRead));
            }
        }

        // Progress notifications.
        if (mDownloadProgressCb)
        {
            mDownloadProgressCb(getCurrentRcfSession(), di);
        }

        RCF_LOG_3()(chunks.size()) 
            << "FileTransferService::DownloadChunks() - exit.";
    }

    void FileTransferService::onServerStart(RcfServer & server)
    {
        mUploadDirectory = server.getUploadDirectory();

        mUploadQuota.reset(new BandwidthQuota( server.getUploadBandwidthLimit() ));
        mDownloadQuota.reset(new BandwidthQuota( server.getDownloadBandwidthLimit() ));

        mUploadQuotaCallback = server.mFileUploadQuotaCb;
        mDownloadQuotaCallback = server.mFileDownloadQuotaCb;

        mDownloadProgressCb = server.mOnFileDownloadProgress;
        mUploadProgressCb = server.mOnFileUploadProgress;

        server.bind<I_FileTransferService>(*this);
    }

    void FileTransferService::onServerStop(RcfServer & server)
    {
        server.unbind<I_FileTransferService>();
    }

    FileTransferProgress::FileTransferProgress() :
        mBytesTotalToTransfer(0),
        mBytesTransferredSoFar(0),
        mServerLimitBps(0)
    {

    }

    FileChunk::FileChunk() : mFileIndex(0), mOffset(0)
    {}

    FileTransferRequest::FileTransferRequest() : mFile(0), mPos(0), mChunkSize(0)
    {}

#if RCF_FEATURE_SF==1

    void FileManifest::serialize(SF::Archive & ar) 
    {
        ar & mFiles;
    }

    FileInfo::FileInfo() :
        mIsDirectory(false),
        mFileStartPos(0),
        mFileSize(0),
        mFileCrc(0),
        mLastWriteTime(0)
    {}

    void FileInfo::serialize(SF::Archive & ar) 
    {
        ar 
            & mIsDirectory 
            & mFilePath 
            & mFileStartPos 
            & mFileSize 
            & mFileCrc 
            & mRenameFile;

        if (ar.getRuntimeVersion() >= 11)
        {
            ar & mLastWriteTime;
        }
    }

    void FileChunk::serialize(SF::Archive & ar)
    {
        ar & mFileIndex & mOffset & mData;
    }

    void FileTransferRequest::serialize(SF::Archive & ar)
    {
        ar & mFile & mPos & mChunkSize;
    }

#endif

    bool FileChunk::isEmpty() const
    {
        return mData.isEmpty();
    }

    FileManifest::FileManifest() 
    {}

    FileManifest::FileManifest(const Path& pathToFiles)
    {
        if (!fs::exists(pathToFiles))
        {
            RCF_THROW( RCF::Exception( RcfError_FileOpen, pathToFiles.u8string(), "Path does not exist."));
        }

        if (fs::is_directory(pathToFiles))
        {
            RCF::Exception e("FileManifest no longer supports directories.");
            RCF_THROW(e);
        }
        else
        {
            FileInfo fileInfo;
            fileInfo.mFileSize = fs::file_size(pathToFiles);
            fileInfo.mFileCrc = 0;
            fileInfo.mLastWriteTime = getLastWriteTime(pathToFiles);
            fileInfo.mFilePath = pathToFiles.filename();

            mFiles.push_back(fileInfo);
        }

        mManifestBase = pathToFiles.parent_path();
    }

    std::uint64_t FileManifest::getTotalByteSize() const
    {
        std::uint64_t totalByteSize = 0;
        for (std::size_t i=0; i<mFiles.size(); ++i)
        {
            const FileInfo & fi = mFiles[i];
            std::uint64_t fileSize = fi.mFileSize - fi.mFileStartPos;
            totalByteSize += fileSize;
        }
        return totalByteSize;
    }

    FileUploadInfo::FileUploadInfo(BandwidthQuotaPtr quotaPtr) : 
        mFileHandle( new FileHandle() ),
        mWriteOp( new FileIoRequest() ),
        mCompleted(false),
        mResume(false),
        mTimeStampMs(0),
        mCurrentFile(0),
        mCurrentPos(0),
        mSessionLocalId(0),
        mQuotaPtr(quotaPtr)
    {
        mQuotaPtr->addUpload(this);
    }

    FileUploadInfo::~FileUploadInfo()
    {
        mFileHandle->close();

        mQuotaPtr->removeUpload(this);
        mQuotaPtr.reset();

        // Best effort only.
        try
        {
            if (!mCompleted && mUploadId.empty())
            {
                fs::remove_all(mUploadDir);
            }
        }
        catch(const std::exception & e)
        {
            std::string error = e.what();
        }
        catch(...)
        {
        }
    }

    std::string FileUploadInfo::getUploadId() const
    {
        return mUploadId;
    }

    FileDownloadInfo::FileDownloadInfo(BandwidthQuotaPtr quotaPtr) :
        mFileHandle( new FileHandle() ),
        mReadOp( new FileIoRequest() ),
        mCurrentFile(0),
        mCurrentPos(0),
        mResume(false),
        mTransferWindowBytesSoFar(0),
        mTransferWindowBytesTotal(0),
        mCancel(false),
        mSessionLocalId(0),
        mQuotaPtr(quotaPtr)
    {
        mQuotaPtr->addDownload(this);
    }

    FileDownloadInfo::~FileDownloadInfo()
    {
        mQuotaPtr->removeDownload(this);
        mQuotaPtr.reset();
    }

    void FileTransferService::setTransferWindowS(std::uint32_t transferWindowS)
    {
        mTransferWindowS = transferWindowS;
    }

    std::uint32_t FileTransferService::getTransferWindowS()
    {
        return mTransferWindowS;
    }

    BandwidthQuota::BandwidthQuota() : mQuotaBps(0)
    {
    }

    BandwidthQuota::BandwidthQuota(std::uint32_t quotaBps) : mQuotaBps(quotaBps)
    {
    }

    void BandwidthQuota::addUpload(FileUploadInfo * pUpload)
    {
        Lock lock(mMutex);
        mUploadsInProgress.insert(pUpload);
    }
    void BandwidthQuota::removeUpload(FileUploadInfo * pUpload)
    {
        Lock lock(mMutex);
        mUploadsInProgress.erase(pUpload);
    }

    void BandwidthQuota::addDownload(FileDownloadInfo * pDownload)
    {
        Lock lock(mMutex);
        mDownloadsInProgress.insert(pDownload);
    }

    void BandwidthQuota::removeDownload(FileDownloadInfo * pDownload)
    {
        Lock lock(mMutex);
        mDownloadsInProgress.erase(pDownload);
    }

    void BandwidthQuota::setQuota(std::uint32_t quotaBps)
    {
        Lock lock(mMutex);
        mQuotaBps = quotaBps;
    }

    std::uint32_t BandwidthQuota::calculateLineSpeedLimit()
    {
        Lock lock(mMutex);
        std::size_t transfers = mUploadsInProgress.size() + mDownloadsInProgress.size();
        RCF_ASSERT(transfers > 0);
        return mQuotaBps / (std::uint32_t) transfers;
    }

} // namespace RCF
