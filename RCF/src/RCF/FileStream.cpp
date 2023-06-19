
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

#include <RCF/ClientStub.hpp>

#include <sys/stat.h>
#include <RCF/FileSystem.hpp>
#include <RCF/FileIoThreadPool.hpp>
#include <RCF/FileTransferInterface.hpp>
#include <RCF/FileTransferService.hpp>
#include <RCF/Globals.hpp>
#include <RCF/ObjectPool.hpp>
#include <RCF/RcfSession.hpp>
#include <RCF/ThreadLocalData.hpp>
#include <RCF/Uuid.hpp>

#include <RCF/Log.hpp>

#include <RCF/BsdSockets.hpp>

namespace fs = RCF_FILESYSTEM_NS;

namespace RCF {

    // FileHandle

    FileHandle::FileHandle()
    {
    }

    FileHandle::FileHandle(const Path & filePath, OpenMode mode)
    {
        open(filePath, mode);
    }

    FileHandle::~FileHandle()
    {
        close();
    }

    Exception FileHandle::err() const
    {
        return mErr;
    }

    Path FileHandle::getFilePath()
    {
        return mFilePath;
    }

    void FileHandle::open(const Path & filePath, OpenMode mode)
    {
        close();

        mFilePath = filePath;
        mOpenMode = mode;
        mBeginPos = 0;

        std::wstring wFilePath = filePath.wstring();
        std::string uFilePath = filePath.u8string();
        int err = 0;
        if ( mode == OpenMode::Read )
        {
#ifdef RCF_WINDOWS
            mpFile = _wfsopen(wFilePath.c_str(), L"rb", _SH_DENYNO);
#else
            mpFile = fopen(uFilePath.c_str(), "rb");
#endif
        }
        else if ( mode == OpenMode::WriteTruncate )
        {
#ifdef RCF_WINDOWS
            mpFile = _wfsopen(wFilePath.c_str(), L"wb", _SH_DENYWR);
#else
            mpFile = fopen(uFilePath.c_str(), "wb");
#endif
        }
        else if ( mode == OpenMode::WriteAppend )
        {
#ifdef RCF_WINDOWS
            mpFile = _wfsopen(wFilePath.c_str(), L"ab", _SH_DENYWR);            
#else
            mpFile = fopen(uFilePath.c_str(), "ab");
#endif
        }
        if ( !mpFile )
        {
            err = Platform::OS::BsdSockets::GetLastError();
            RCF_THROW(RCF::Exception(RcfError_FileOpen, filePath.u8string(), Platform::OS::GetErrorString(err)));
        }

        if ( mode == OpenMode::WriteAppend )
        {
            // Do an explicit seek to the end so that ftell() returns predictable results.
            // Without this ftell() returns 0 initially, but after writing, returns results relative to beginning of file.

#ifdef RCF_WINDOWS
            struct _stat64i32 st = { 0 };
            int ret = _wstat(wFilePath.c_str(), &st);
            RCF_ASSERT(ret == 0);
            RCF_UNUSED_VARIABLE(ret);
            mBeginPos = st.st_size;
#else
            struct stat st;
            int ret = stat(uFilePath.c_str(), &st);
            RCF_ASSERT(ret == 0);
            RCF_UNUSED_VARIABLE(ret);
            mBeginPos = st.st_size;
#endif

            seek(mBeginPos);
        }

        RCF_ASSERT(mpFile);
    }

    void FileHandle::close()
    {
        if ( mpFile )
        {
            int ret = fclose(mpFile);
            RCF_ASSERT(ret == 0);
            RCF_UNUSED_VARIABLE(ret);
            mpFile = NULL;
        }
    }

    void FileHandle::flush()
    {
        if ( mpFile )
        {
            int ret = fflush(mpFile);
            RCF_ASSERT(ret == 0);
            RCF_UNUSED_VARIABLE(ret);
        }
    }

    void FileHandle::seek(std::uint64_t newPos)
    {

#ifdef RCF_WINDOWS
        int ret = _fseeki64(mpFile, newPos, SEEK_SET);
#else
        int ret = fseeko64(mpFile, newPos, SEEK_SET);
#endif

        if ( ret != 0 )
        {
            int err = Platform::OS::BsdSockets::GetLastError();
            RCF_THROW(RCF::Exception(RcfError_FileSeek, mFilePath.u8string(), newPos, Platform::OS::GetErrorString(err)));
        }
    }

    std::uint64_t FileHandle::tell()
    {

#ifdef RCF_WINDOWS
        std::uint64_t ret = _ftelli64(mpFile);
#else
        std::uint64_t ret = ftello64(mpFile);
#endif

        return ret;
    }

    std::size_t FileHandle::read(const ByteBuffer& buffer)
    {
        std::size_t ret = 0;
        if ( mpFile && buffer.getLength() > 0)
        {
            ret = fread(buffer.getPtr(), 1, buffer.getLength(), mpFile);
            if ( ret == 0 )
            {
                int err = Platform::OS::BsdSockets::GetLastError();
                mErr = RCF::Exception(RcfError_FileRead, mFilePath.u8string(), Platform::OS::GetErrorString(err));
            }
        }
        return ret;
    }

    std::size_t FileHandle::write(const ByteBuffer& buffer)
    {
        std::size_t ret = 0;
        if ( mpFile && buffer.getLength() > 0)
        {
            ret = fwrite(buffer.getPtr(), 1, buffer.getLength(), mpFile);
            if ( ret == 0 )
            {
                int err = Platform::OS::BsdSockets::GetLastError();
                mErr = RCF::Exception(RcfError_FileWrite, mFilePath.u8string(), Platform::OS::GetErrorString(err));
            }
        }
        return ret;
    }

    // FileStream

    FileStream::FileStream() : mImplPtr( new FileStreamImpl() )
    {
    }

    FileStream::FileStream(FileStreamImplPtr implPtr) : 
        mImplPtr(implPtr)
    {
    }

    FileStream::FileStream(const std::string & filePath) : 
        mImplPtr(new FileStreamImpl(filePath) )
    {
    }

    FileStream::FileStream(const FileManifest & manifest) : 
        mImplPtr(new FileStreamImpl(manifest) )
    {
    }

    std::string FileStream::getLocalPath() const
    {
        RCF_ASSERT(mImplPtr);
        if ( mImplPtr->mUploadId.size() > 0 )
        {
            Path localPath = RCF::getCurrentRcfSession().getUploadPath(mImplPtr->mUploadId);
            return localPath.u8string();
        }
        FileManifest & manifest = mImplPtr->mManifest;
        Path localPath = manifest.mManifestBase;
        RCF_ASSERT(manifest.mFiles.size() > 0);
        localPath /= (*manifest.mFiles[0].mFilePath.begin());
        return localPath.u8string();
    }

    FileManifest & FileStream::getManifest() const
    {
        RCF_ASSERT(mImplPtr);
        return mImplPtr->mManifest;
    }

    void FileStream::setDownloadToPath(const std::string & downloadToPath)
    {
        RCF_ASSERT(mImplPtr);
        mImplPtr->mDownloadToPath = downloadToPath;
    }

    std::string FileStream::getDownloadToPath() const
    {
        RCF_ASSERT(mImplPtr);
        return mImplPtr->mDownloadToPath.u8string();
    }

    void FileStream::setTransferRateBps(std::uint32_t transferRateBps)
    {
        RCF_ASSERT(mImplPtr);
        mImplPtr->mTransferRateBps = transferRateBps;
    }

    std::uint32_t FileStream::getTransferRateBps()
    {
        RCF_ASSERT(mImplPtr);
        return mImplPtr->mTransferRateBps;
    }

#if RCF_FEATURE_SF==1

    void FileStream::serialize(SF::Archive & ar)
    {
        ar & *mImplPtr;
    }

    void FileStreamImpl::serializeImplSf(
        SF::Archive & ar, 
        std::uint32_t & transferId, 
        Direction & dir)
    {
        ar & transferId & dir;
    }

    void FileStreamImpl::serialize(SF::Archive & ar)
    {
        bool isSaving = ar.isWrite(); 

        serializeGeneric( 
            isSaving,
            std::bind( 
                &FileStreamImpl::serializeImplSf, 
                this, 
                std::ref(ar),
                std::placeholders::_1,
                std::placeholders::_2) );
    }

#endif

    void FileStream::upload(RCF::ClientStub & clientStub)
    {
        std::uint32_t chunkSize = 1024*1024;
        
        clientStub.uploadFiles(
            mImplPtr->mManifest, 
            mImplPtr->mUploadId, 
            chunkSize, 
            mImplPtr->mTransferRateBps,
            mImplPtr->mSessionLocalId);

        mImplPtr->mSessionLocalId = 0;
    }

    void FileStream::download(RCF::ClientStub & clientStub)
    {
        std::uint32_t chunkSize = 1024*1024;
        
        if (mImplPtr->mDownloadToPath.empty())
        {
            std::string downloadId;
            Path downloadPath;
            while ( downloadId.empty() || fs::exists(downloadPath) )
            {
                downloadId = generateUuid();
                downloadPath = Path(globals().getFileStreamDefaultDownloadDirectory()) / downloadId;
            }

            mImplPtr->mDownloadToPath = downloadPath;
            fs::create_directories(mImplPtr->mDownloadToPath);
        }

        clientStub.downloadFiles(
            mImplPtr->mDownloadToPath, 
            mImplPtr->mManifest, 
            chunkSize, 
            mImplPtr->mTransferRateBps,
            mImplPtr->mSessionLocalId,
            "");

        mImplPtr->mManifest.mManifestBase = mImplPtr->mDownloadToPath;
        mImplPtr->mSessionLocalId = 0;
    }

    // FileStreamImpl

    FileStreamImpl::FileStreamImpl() : 
        mTransferRateBps(0),
        mSessionLocalId(0), 
        mDirection(Unspecified)
    {
    }

    FileStreamImpl::FileStreamImpl(const std::string & filePath) : 
        mManifest(filePath),
        mTransferRateBps(0),
        mSessionLocalId(0),
        mDirection(Unspecified)
    {
    }

    FileStreamImpl::FileStreamImpl(const FileManifest & manifest) :
        mManifest(manifest),
        mTransferRateBps(0),
        mSessionLocalId(0),
        mDirection(Unspecified)
    {
    }

    FileStreamImpl::~FileStreamImpl()
    {
    }

    void FileStreamImpl::serializeGeneric(
        bool isWriting,
        std::function<void(std::uint32_t &, Direction &)> serializeImpl)
    {
        // Determine if we are client side or server side.
        // TODO: what if both of these are non-zero?
        RCF::ClientStub * pClientStub = RCF::getTlsClientStubPtr();
        RCF::RcfSession * pSession = RCF::getTlsRcfSessionPtr();

        // Client side.
        // Append a ref to ourselves the the current ClientStub.
        // After serializing regular parameters, we'll regain control.
        if (pClientStub)
        {
            if (isWriting)
            {
                if (mDirection == Upload)
                {
                    mSessionLocalId = pClientStub->addUploadStream(
                        FileUpload(shared_from_this()));
                }
                else if (mDirection == Download)
                {
                    mSessionLocalId = pClientStub->addDownloadStream(
                        FileDownload(shared_from_this()));
                }

                serializeImpl(mSessionLocalId, mDirection);
            }
            else
            {
                // Shouldn't really be in here.
                std::uint32_t sessionLocalId = 0;
                Direction dir = Unspecified;
                serializeImpl(sessionLocalId, dir);
            }
        }
        else if (pSession)
        {
            if (isWriting)
            {
                // Shouldn't really be in here.
                std::uint32_t sessionLocalId = 0;
                Direction dir = Unspecified;
                serializeImpl(sessionLocalId, dir);
            }
            else
            {
                serializeImpl(mSessionLocalId, mDirection);

                if (mDirection == Upload && mSessionLocalId)
                {
                    RcfSession::SessionUploads::iterator iter = 
                        pSession->mSessionUploads.find(mSessionLocalId);

                    if (iter != pSession->mSessionUploads.end())
                    {
                        FileUploadInfoPtr uploadPtr = iter->second;
                        mUploadId = uploadPtr->getUploadId();
                        mManifest = uploadPtr->mManifest;
                        mManifest.mManifestBase = uploadPtr->mUploadDir;

                        pSession->mSessionUploads.erase(iter);
                    }
                    else
                    {
                        // Couldn't find the upload.
                    }
                }
                else if (mDirection == Download && mSessionLocalId)
                {
                    pSession->addDownloadStream(
                        mSessionLocalId,
                        FileDownload(shared_from_this()));
                }
            }
        }
    }

    // FileUpload

    FileUpload::FileUpload()
    {
        mImplPtr->mDirection = FileStreamImpl::Upload;
    }

    FileUpload::FileUpload(const std::string & filePath) : FileStream(filePath)
    {
        mImplPtr->mDirection = FileStreamImpl::Upload;
    }

    FileUpload::FileUpload(const FileManifest & manifest) : FileStream(manifest)
    {
        mImplPtr->mDirection = FileStreamImpl::Upload;
    }

    FileUpload::FileUpload(FileStreamImplPtr implPtr) : FileStream(implPtr)
    {
        mImplPtr->mDirection = FileStreamImpl::Upload;
    }

    // FileDownload

    FileDownload::FileDownload()
    {
        mImplPtr->mDirection = FileStreamImpl::Download;
    }

    FileDownload::FileDownload(const std::string & filePath) : FileStream(filePath)
    {
        mImplPtr->mDirection = FileStreamImpl::Download;
    }

    FileDownload::FileDownload(const FileManifest & manifest) : FileStream(manifest)
    {
        mImplPtr->mDirection = FileStreamImpl::Download;
    }

    FileDownload::FileDownload(FileStreamImplPtr implPtr) : FileStream(implPtr)
    {
        mImplPtr->mDirection = FileStreamImpl::Download;
    }

    // ClientStub

    std::uint32_t ClientStub::addUploadStream(FileUpload fileStream)
    {
        mUploadStreams.push_back(fileStream);
        return static_cast<std::uint32_t>(mUploadStreams.size());
    }

    void ClientStub::processUploadStreams()
    {
        ThreadLocalCached< std::vector<FileUpload> > tlcFileStreams;
        std::vector<FileUpload> & fileStreams = tlcFileStreams.get();
        RCF_ASSERT(fileStreams.empty());
        fileStreams.swap(mUploadStreams);
        for (std::size_t i=0; i<fileStreams.size(); ++i)
        {
            fileStreams[i].upload(*this);
        }
    }

    std::uint32_t ClientStub::addDownloadStream(FileDownload fileStream)
    {
        mDownloadStreams.push_back(fileStream);
        return static_cast<std::uint32_t>(mDownloadStreams.size());
    }

    void ClientStub::setFileProgressCallback(FileProgressCallback fileProgressCb)
    {
        mFileProgressCb = fileProgressCb;
    }

    void ClientStub::runFileProgressNotifications(FileTransferProgress& progressInfo)
    {
        if ( mFileProgressCb )
        {
            RemoteCallAction action = RemoteCallAction::Rca_Continue;
            mFileProgressCb(progressInfo, action);

            RCF_VERIFY(
                action == RemoteCallAction::Rca_Continue,
                Exception(RcfError_ClientCancel));
        }
    }

    void ClientStub::setTransferWindowS(std::uint32_t transferWindowS)
    {
        mTransferWindowS = transferWindowS;
    }

    std::uint32_t ClientStub::getTransferWindowS()
    {
        return mTransferWindowS;
    }

    // TODO: resuming of failed transfers, in either direction.

    std::uint32_t calculateEffectiveBps(
        std::uint32_t serverBps, 
        std::uint32_t clientBps)
    {
        std::uint32_t effectiveBps = 0;
        if (serverBps == 0)
        {
            effectiveBps = clientBps;
        }
        else if (clientBps == 0)
        {
            effectiveBps = serverBps;
        }
        else
        {
            effectiveBps = (std::min)(serverBps, clientBps);
        }
        return effectiveBps;
    }

    void ClientStub::uploadFile(
        std::string&            uploadId,
        const Path&             uploadPath,
        FileTransferOptions *   pOptions)
    {
        FileManifest manifest( uploadPath );
        
        std::uint32_t chunkSize = 1024 * 1024;
        std::uint32_t transferRateBps = 0;
        std::uint32_t sessionLocalId = 0;

        if ( pOptions )
        {
            if ( pOptions->mChunkSize )
            {
                chunkSize = pOptions->mChunkSize;
            }
            if ( pOptions->mBandwidthLimitBps )
            {
                transferRateBps = pOptions->mBandwidthLimitBps;
            }
        }
        
        uploadFiles(manifest, uploadId, chunkSize, transferRateBps, sessionLocalId);
    }

    void ClientStub::uploadFiles(
        const FileManifest & manifest,
        std::string & uploadId,
        std::uint32_t chunkSize,
        std::uint32_t transferRateBps,
        std::uint32_t sessionLocalId)
    {
        RCF_LOG_3()(manifest.mFiles.size())(chunkSize)(sessionLocalId) 
            << "ClientStub::uploadFiles() - entry.";

        ClientStub & clientStub = *this;

        if (! clientStub.isConnected())
        {
            clientStub.connect();
        }

        RcfClient<I_FileTransferService> ftsClient(clientStub);
        ftsClient.getClientStub().setTransport( clientStub.releaseTransport() );

        RestoreClientTransportGuard guard(clientStub, ftsClient.getClientStub());
        RCF_UNUSED_VARIABLE(guard);

        // 1) Send manifest to server, and an optimistic first chunk. 
        // 2) Server replies, with index, pos and CRC of next chunk to transfer.
        // --> CRC only passed if pos != chunk length
        // 3) Client goes into a loop, sending chunks until all files are transferred.

        namespace fs = RCF_FILESYSTEM_NS;

        Path manifestBase = manifest.mManifestBase;

        FileChunk startPos;
        std::uint32_t maxMessageLength    = 0;
        std::uint32_t clientBps           = transferRateBps;
        std::uint32_t serverBps           = 0;
        std::uint32_t effectiveBps        = 0;

        RCF_LOG_3()(manifest.mFiles.size())(chunkSize)(sessionLocalId) 
            << "ClientStub::uploadFiles() - calling BeginUpload().";

        ftsClient.BeginUpload(
            manifest, 
            std::vector<FileChunk>(), 
            startPos, 
            maxMessageLength,
            uploadId,
            serverBps,
            sessionLocalId);

        RCF_LOG_3()(startPos.mFileIndex)(startPos.mOffset)(maxMessageLength)(uploadId)(serverBps) 
            << "ClientStub::uploadFiles() - BeginUpload() returned.";
        
        std::uint64_t totalByteSize = manifest.getTotalByteSize();

        std::uint64_t totalBytesUploadedSoFar = 0;
        for (std::size_t i=0; i<startPos.mFileIndex; ++i)
        {
            totalBytesUploadedSoFar += manifest.mFiles[i].mFileSize;
        }
        totalBytesUploadedSoFar += startPos.mOffset;

        // Progress callback.
        FileTransferProgress progressInfo;
        progressInfo.mBytesTotalToTransfer = totalByteSize;
        progressInfo.mBytesTransferredSoFar = totalBytesUploadedSoFar;
        progressInfo.mServerLimitBps = serverBps;
        runFileProgressNotifications(progressInfo);
        
        std::uint32_t firstFile = startPos.mFileIndex;
        std::uint64_t firstPos = startPos.mOffset;

        // Limit the chunk size to 80 % of max message length.
        if (maxMessageLength == 0)
        {
            chunkSize = 1024*1024;
        }
        else
        {
            if ( chunkSize )
            {
                chunkSize = RCF_MIN(chunkSize, maxMessageLength * 8 / 10);
            }
            else
            {
                chunkSize = maxMessageLength * 8 / 10;
            }
        }

        effectiveBps = calculateEffectiveBps(serverBps, clientBps);
        
        Timer windowTimer;
        std::uint32_t windowBytesTotal    = mTransferWindowS*effectiveBps;
        std::uint32_t windowBytesSoFar    = 0;

        std::vector<FileChunk> chunks;

        // Async file reading.
        FileIoRequestPtr readOp( new FileIoRequest() );
        ByteBuffer bufferRead(chunkSize);
        ByteBuffer bufferSend(chunkSize);

        std::size_t bufferReadPos = 0;

        std::size_t currentFile = firstFile;

        while (currentFile != manifest.mFiles.size())
        {
            const FileInfo & info = manifest.mFiles[currentFile];
            Path filePath = manifestBase / info.mFilePath;

            // Upload chunks to the server until we're done.
            
            RCF_LOG_3()(filePath.u8string())
                << "ClientStub::uploadFiles() - opening file.";

            FileHandlePtr fin( new FileHandle(filePath, FileHandle::Read));

            std::uint64_t filePos = 0;        
            if (currentFile == firstFile)
            {
                filePos = firstPos;

                RCF_LOG_3()(filePos)
                    << "ClientStub::uploadFiles() - seeking in file.";

                fin->seek( (std::size_t) filePos );
            }

            readOp->complete();

            const std::uint64_t FileSize = fs::file_size(filePath);

            if ( FileSize == 0 )
            {
                FileChunk chunk;
                chunk.mData = ByteBuffer();
                chunk.mFileIndex = 0;
                chunk.mOffset = 0;
                chunks.push_back(chunk);

                ftsClient.UploadChunks(chunks, serverBps);

                ++currentFile;
            }

            while (filePos < FileSize)
            {
                std::size_t bytesRead = 0;

                bool shouldSend = false;
                bool shouldReadAhead = true;

                // Wait for current read to complete.
                if (readOp->isInitiated())
                {
                    readOp->complete();
                    
                    bytesRead = static_cast<std::size_t>(
                        readOp->getBytesTransferred());

                    RCF_LOG_3()(bytesRead)
                        << "ClientStub::uploadFiles() - completing read from file.";
                    
                    if ( bytesRead == 0 )
                    {
                        Exception e = fin->err();
                        RCF_ASSERT(e.bad());
                        RCF_THROW(e);
                    }

                    FileChunk chunk;
                    chunk.mFileIndex = (std::uint32_t) currentFile;
                    chunk.mOffset = filePos;
                    chunk.mData = ByteBuffer(bufferRead, bufferReadPos, bytesRead);

                    RCF_LOG_3()(chunk.mFileIndex)(chunk.mOffset)(chunk.mData.getLength())
                        << "ClientStub::uploadFiles() - adding chunk.";

                    chunks.push_back( chunk );

                    bufferReadPos += bytesRead;
                    filePos += bytesRead;
                    windowBytesSoFar += (std::uint32_t) bytesRead;

                    if (bufferReadPos == bufferRead.getLength())
                    {
                        shouldSend = true;
                    }

                    if (effectiveBps && windowBytesSoFar >= windowBytesTotal)
                    {
                        shouldSend = true;
                    }

                    if (filePos == FileSize)
                    {
                        ++currentFile;

                        // Have we done all the files?
                        if (currentFile == manifest.mFiles.size())
                        {
                            shouldSend = true;
                        }

                        // No read ahead if we are transitioning from one file to another.
                        shouldReadAhead = false;
                    }

                    if (shouldSend)
                    {
                        bufferSend.swap(bufferRead);
                        bufferReadPos = 0;
                    }
                }
                
                // Read ahead.
                if (shouldReadAhead)
                {
                    RCF_LOG_3()(bufferRead.getLength())(bufferReadPos)
                        << "ClientStub::uploadFiles() - initiating read from file.";

                    RCF_ASSERT(bufferReadPos < bufferRead.getLength());

                    std::size_t bytesToRead = 
                        static_cast<std::size_t>(bufferRead.getLength() - bufferReadPos);

                    // Trim to throttle settings.
                    if (effectiveBps)
                    {
                        if (windowBytesSoFar < windowBytesTotal)
                        {
                            std::uint32_t windowBytesRemaining = 
                                windowBytesTotal - windowBytesSoFar;

                            bytesToRead = RCF_MIN(
                                static_cast<std::uint32_t>(bytesToRead), 
                                windowBytesRemaining);
                        }
                        else
                        {
                            bytesToRead = 0;
                        }

                        RCF_LOG_3()(bytesToRead)(windowBytesSoFar)(windowBytesTotal)
                            << "ClientStub::uploadFiles() - trimming chunk size to throttle setting.";
                    }

                    if (bytesToRead)
                    {
                        readOp->initiateRead(fin, ByteBuffer(bufferRead, bufferReadPos, bytesToRead));
                    }
                }

                // Upload accumulated file chunks.
                if (shouldSend)
                {
                    RCF_LOG_3()(chunks.size())
                        << "ClientStub::uploadFiles() - calling UploadChunks().";

                    ftsClient.UploadChunks(chunks, serverBps);

                    effectiveBps = calculateEffectiveBps(serverBps, clientBps);

                    RCF_LOG_3()(serverBps)
                        << "ClientStub::uploadFiles() - UploadChunks() returned.";

                    std::size_t bytesSent = 0;
                    for (std::size_t i=0; i<chunks.size(); ++i)
                    {
                        bytesSent += chunks[i].mData.getLength();
                    }
                    totalBytesUploadedSoFar += bytesSent;

                    chunks.clear();

                    // Progress callback.
                    progressInfo.mBytesTotalToTransfer = totalByteSize;
                    progressInfo.mBytesTransferredSoFar = totalBytesUploadedSoFar;
                    progressInfo.mServerLimitBps = serverBps;
                    runFileProgressNotifications(progressInfo);

                    if (effectiveBps)
                    {
                        // Recalculate window size based on possibly updated bps setting.
                        windowBytesTotal = mTransferWindowS * effectiveBps;
                        if (windowBytesSoFar >= windowBytesTotal)
                        {
                            RCF_LOG_3()(windowBytesSoFar)(windowBytesTotal)
                                << "ClientStub::uploadFiles() - window capacity reached.";

                            // Exceeded window capacity. Wait for window to expire.
                            std::uint32_t windowMsSoFar = windowTimer.getDurationMs();
                            if (windowMsSoFar < mTransferWindowS*1000)
                            {
                                std::uint32_t waitMs = mTransferWindowS*1000 - windowMsSoFar;

                                RCF_LOG_3()(waitMs)
                                    << "ClientStub::uploadFiles() - waiting for next window.";

                                sleepMs(waitMs);
                                while (!windowTimer.elapsed(mTransferWindowS*1000))
                                {
                                    sleepMs(100);
                                }
                            }
                        }

                        // If window has expired, open a new one.
                        if (windowTimer.elapsed(mTransferWindowS*1000))
                        {
                            windowTimer.restart();

                            // Carry over balance from previous window.
                            if (windowBytesSoFar > windowBytesTotal)
                            {
                                windowBytesSoFar = windowBytesSoFar - windowBytesTotal;
                            }
                            else
                            {
                                windowBytesSoFar = 0;
                            }

                            RCF_LOG_3()(mTransferWindowS)(windowBytesSoFar)
                                << "ClientStub::uploadFiles() - new transfer window.";
                        }
                    }
                }
            }
        }
        RCF_LOG_3()(totalByteSize)(totalBytesUploadedSoFar) 
            << "ClientStub::uploadFiles() - exit.";
    }

    void ClientStub::downloadFile(
        const std::string&      downloadId,
        const Path&             downloadToPath,
        FileTransferOptions *   pOptions)
    {
        RCF_UNUSED_VARIABLE(downloadId);

        RCF::FileManifest manifest;
        std::uint32_t chunkSize = 1024 * 1024;
        std::uint32_t transferRateBps = 0;
        std::uint64_t startPosition = 0;
        std::uint64_t endPosition = std::uint64_t(-1);

        if ( pOptions )
        {
            if ( pOptions->mChunkSize )
            {
                chunkSize = pOptions->mChunkSize;
            }
            if ( pOptions->mBandwidthLimitBps )
            {
                transferRateBps = pOptions->mBandwidthLimitBps;
            }
            if ( pOptions->mStartPos )
            {
                startPosition = pOptions->mStartPos;
            }
            if ( pOptions->mEndPos )
            {
                endPosition = pOptions->mEndPos;
            }
        }

        std::uint32_t sessionLocalDownloadId = 0;
        downloadFiles(
            downloadToPath, 
            manifest, 
            chunkSize, 
            transferRateBps, 
            sessionLocalDownloadId, 
            downloadId, 
            startPosition, 
            endPosition);
    }

    void ClientStub::downloadFiles(
        const Path& downloadToPath,
        FileManifest & totalManifest,
        std::uint32_t chunkSize,
        std::uint32_t transferRateBps,
        std::uint32_t sessionLocalId,
        const std::string & downloadId,
        std::uint64_t startPosition,
        std::uint64_t endPosition)
    {
        RCF_LOG_3()(downloadToPath.u8string())(chunkSize)(transferRateBps)(sessionLocalId)(downloadId)(startPosition)(endPosition)
            << "ClientStub::downloadFiles() - entry.";

        ClientStub & clientStub = *this;

        if (! clientStub.isConnected())
        {
            clientStub.connect();
        }

        RCF::RcfClient<RCF::I_FileTransferService> ftsClient(clientStub);
        ftsClient.getClientStub().setTransport( clientStub.releaseTransport() );

        RestoreClientTransportGuard guard(clientStub, ftsClient.getClientStub());
        RCF_UNUSED_VARIABLE(guard);

        // Initial call to server to setup download.
        FileInfo                    fileInfo;
        std::uint32_t               serverMaxMessageLength = 0;
        std::uint32_t               serverBps = 0;

        RCF_LOG_3()(downloadToPath.u8string())(chunkSize)(transferRateBps)(sessionLocalId)
            << "ClientStub::downloadFiles() - calling BeginDownload().";

        {
            FileManifest                manifest;
            FileTransferRequest         request;
            std::vector<FileChunk>      chunks;

            ftsClient.BeginDownload(
                manifest,
                request,
                chunks,
                serverMaxMessageLength,
                serverBps,
                sessionLocalId,
                downloadId);

            totalManifest = manifest;
            fileInfo = manifest.mFiles[0];

            if ( endPosition == -1 || endPosition > fileInfo.mFileSize )
            {
                endPosition = fileInfo.mFileSize;
            }
        }

        std::uint32_t clientMaxMessageLength = static_cast<std::uint32_t>(
            getTransport().getMaxIncomingMessageLength());

        RCF_LOG_3()(serverMaxMessageLength)(clientMaxMessageLength)(serverBps)
            << "ClientStub::downloadFiles() - BeginDownload() returned.";

        // Adjust chunk size.
        chunkSize = RCF_MIN(chunkSize, serverMaxMessageLength*8/10);
        chunkSize = RCF_MIN(chunkSize, clientMaxMessageLength*8/10);

        // Determine file path to download to.
        Path filePath = downloadToPath;
        if ( fs::is_directory(filePath) )
        {
            // Tack on server-suggested file name.
            filePath = filePath / fileInfo.mFilePath;
        }
        fs::create_directories(filePath.parent_path());

        // Set download position.
        std::uint64_t currentPos = 0;
        if ( startPosition )
        {
            currentPos = startPosition;
        }
        
        // Adjust download position for any previously downloaded fragment.
        bool resumeExisting = false;
        if ( fs::exists(filePath) )
        {
            std::uint64_t fileSize = fs::file_size(filePath);
            if ( fileSize )
            {
                currentPos += fileSize;
                currentPos = RCF_MIN(currentPos, endPosition);
                resumeExisting = true;
            }
        }

        // Progress callback.
        FileTransferProgress progressInfo;
        progressInfo.mDownloadPath              = filePath;
        progressInfo.mBytesTotalToTransfer      = endPosition - startPosition;
        progressInfo.mBytesTransferredSoFar     = currentPos - startPosition;
        progressInfo.mServerLimitBps            = serverBps;        

        if ( currentPos == endPosition )
        {
            // No downloading needed.
            if ( !fs::exists(filePath) )
            {
                // If it's a zero-length file, we need to create it.
                FileHandle().open(filePath, FileHandle::WriteTruncate);
                setLastWriteTime(filePath, fileInfo.mLastWriteTime);
            }

            runFileProgressNotifications(progressInfo);
            return;
        }

        // Inform server of download position.
        if ( currentPos )
        {
            FileChunk startPos;
            startPos.mOffset = currentPos;

            RCF_LOG_3()(startPos.mFileIndex)(startPos.mOffset)
                << "ClientStub::downloadFiles() - calling TrimDownload().";

            ftsClient.TrimDownload(startPos);

            RCF_LOG_3()(startPos.mFileIndex)(startPos.mOffset)
                << "ClientStub::downloadFiles() - TrimDownload() returned.";
        }

        FileHandlePtr           fout( new FileHandle() );
        FileIoRequestPtr    writeOp( new FileIoRequest() );
        std::uint32_t       adviseWaitMs            = 0;
        std::uint64_t       totalBytesReadSoFar     = currentPos;

        if ( resumeExisting )
        {
            // Append to existing file.
            RCF_LOG_3()(filePath.u8string())
                << "ClientStub::downloadFiles() - opening file (appending).";

            fout->open(filePath, FileHandle::WriteAppend);
        }
        else
        {
            // Create new file.
            RCF_LOG_3()(filePath.u8string())
                << "ClientStub::downloadFiles() - opening file (truncating).";

            fout->open(filePath, FileHandle::WriteTruncate);
        }

        runFileProgressNotifications(progressInfo);

        const std::uint32_t     TransferWindowS = 5;
        RCF::Timer              transferWindowTimer;
        std::uint32_t           transferWindowBytes = 0;
        bool                    localWait = false;
        FileChunk               chunk;

        while (true)
        {
            RCF_ASSERT(chunk.isEmpty() || currentPos == 0);

            if (chunk.isEmpty())
            {
                // Round trip to the server for the next chunk.

                FileTransferRequest request;
                request.mFile       = 0;
                request.mPos        = currentPos;
                request.mChunkSize  = chunkSize;

                if ( endPosition - currentPos < chunkSize )
                {
                    request.mChunkSize = (std::uint32_t) (endPosition - currentPos);
                }

                // Respect server throttle settings.
                if (adviseWaitMs)
                {
                    RCF_LOG_3()(adviseWaitMs)
                        << "ClientStub::downloadFiles() - waiting on server throttle.";

                    sleepMs(adviseWaitMs);
                    adviseWaitMs = 0;
                }

                // Respect local throttle setting.
                if (localWait)
                {
                    std::uint32_t startTimeMs = transferWindowTimer.getStartTimeMs();
                    std::uint32_t nowMs = getCurrentTimeMs();
                    if (nowMs < startTimeMs + 1000*TransferWindowS)
                    {
                        std::uint32_t waitMs = startTimeMs + 1000*TransferWindowS - nowMs;

                        RCF_LOG_3()(waitMs)(mTransferWindowS)
                            << "ClientStub::downloadFiles() - waiting on client throttle.";
                        
                        // Existing behavior - if we're going to wait, wait at least a second.
                        waitMs = RCF_MAX(std::uint32_t(1000), waitMs);
                        RCF::sleepMs(waitMs);
                        localWait = false;
                    }
                }

                // Trim chunk size according to transfer rate.                
                if (transferRateBps)
                {
                    if (transferWindowTimer.elapsed(TransferWindowS*1000))
                    {
                        transferWindowTimer.restart();
                        transferWindowBytes = 0;
                    }

                    std::uint32_t bytesTotal = transferRateBps * TransferWindowS;
                    std::uint32_t bytesRemaining =  bytesTotal - transferWindowBytes;

                    if (bytesRemaining < request.mChunkSize)
                    {
                        localWait = true;
                    }

                    RCF_LOG_3()(request.mChunkSize)(bytesRemaining)
                        << "ClientStub::downloadFiles() - trimming request chunk size to client throttle.";

                    request.mChunkSize = RCF_MIN(request.mChunkSize, bytesRemaining);
                }

                RCF_LOG_3()(request.mFile)(request.mPos)(request.mChunkSize)
                    << "ClientStub::downloadFiles() - calling DownloadChunks().";

                {
                    std::vector<FileChunk> chunks;
                    ftsClient.DownloadChunks(request, chunks, adviseWaitMs, serverBps);
                    RCF_ASSERT(chunks.size() == 1);
                    if ( chunks.size() > 0 )
                    {
                        chunk = chunks[0];
                    }
                }

                RCF_LOG_3()(chunk.mData.getLength())(adviseWaitMs)(serverBps)
                    << "ClientStub::downloadFiles() - DownloadChunks() returned.";

                // Update byte totals.
                totalBytesReadSoFar += chunk.mData.getLength();
                transferWindowBytes += (std::uint32_t) chunk.mData.getLength();
            }

            RCF_ASSERT(chunk.mOffset == currentPos);

            // Wait for previous write to complete.
            if (writeOp->isInitiated())
            {
                writeOp->complete();
                std::uint64_t bytesWritten = writeOp->getBytesTransferred();
                progressInfo.mBytesTransferredSoFar += bytesWritten;
                runFileProgressNotifications(progressInfo);

                RCF_LOG_3()(bytesWritten)
                    << "ClientStub::downloadFiles() - file write completed.";
                    
                if (bytesWritten == 0)
                {
                    fout->close();
                    setLastWriteTime(filePath, fileInfo.mLastWriteTime);
                    Exception e = fout->err();
                    RCF_ASSERT(e.bad());
                    RCF_THROW(e);
                }
            }

            // Initiate write of current chunk.
            RCF_ASSERT(currentPos + chunk.mData.getLength() <= endPosition);

            RCF_LOG_3()(chunk.mData.getLength())
                << "ClientStub::downloadFiles() - file write initiated.";

            if ( chunk.mData.getLength() > 0 )
            {
                writeOp->initateWrite(fout, chunk.mData);
                currentPos += chunk.mData.getLength();
            }

            // Check if this was the last chunk.
            if ( currentPos == endPosition )
            {
                writeOp->complete();
                std::uint64_t bytesWritten = writeOp->getBytesTransferred();
                progressInfo.mBytesTransferredSoFar += bytesWritten;
                runFileProgressNotifications(progressInfo);
                    
                RCF_LOG_3()(bytesWritten)
                    << "ClientStub::downloadFiles() - file write completed.";

                RCF_LOG_3()
                    << "ClientStub::downloadFiles() - closing file.";
                    
                if (bytesWritten == 0)
                {
                    fout->close();
                    setLastWriteTime(filePath, fileInfo.mLastWriteTime);
                    Exception e = fout->err();
                    RCF_ASSERT(e.bad());
                    RCF_THROW(e);
                }

                fout->close();
                setLastWriteTime(filePath, fileInfo.mLastWriteTime);
                break;
            }

            chunk = FileChunk();
        }

        RCF_LOG_3()
            << "ClientStub::downloadFiles() - exit.";

    }
} // namespace RCF
