
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

#ifndef INCLUDE_RCF_FILETRANSFERINTERFACE_HPP
#define INCLUDE_RCF_FILETRANSFERINTERFACE_HPP

#include <RCF/Idl.hpp>

#include <cstdint>

namespace RCF {

    //--------------------------------------------------------------------------
    // I_FileTransferService

    RCF_BEGIN(I_FileTransferService, "I_FileTransferService")

        RCF_METHOD_V7(
            void,
                BeginUpload,
                    const FileManifest &,           // upload manifest
                    const std::vector<FileChunk> &, // optional first chunks
                    FileChunk &,                    // where to start uploading
                    std::uint32_t &,                // max message length
                    std::string &,                  // upload id
                    std::uint32_t &,                // bps
                    std::uint32_t)                  // session local id

        RCF_METHOD_V2(
            void,
                UploadChunks,
                    const std::vector<FileChunk> &, // file chunks to upload
                    std::uint32_t &)                // bps

        RCF_METHOD_V7(
            void,
                BeginDownload,
                    FileManifest &,                 // download manifest
                    const FileTransferRequest &,    // transfer request
                    std::vector<FileChunk> &,       // optional first chunks
                    std::uint32_t &,                // max message length
                    std::uint32_t &,                // bps
                    std::uint32_t,                  // session local id
                    const std::string &)            // server download id

        RCF_METHOD_V1(
            void,
                TrimDownload,
                    const FileChunk &)              // where to start downloading

        RCF_METHOD_V4(
            void,
                DownloadChunks,
                    const FileTransferRequest &,    // transfer request
                    std::vector<FileChunk> &,       // file chunks to download
                    std::uint32_t &,                // advised wait for next call
                    std::uint32_t &)                // bps

    RCF_END(I_FileTransferService)

} // namespace RCF

#endif // ! INCLUDE_RCF_FILETRANSFERINTERFACE_HPP

