#include "storage.h"

#include <sys/fcntl.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <ios>

#include "util.h"

static inline size_t alignment(size_t size, size_t align) {
  return ((size - 1) / align + 1) * align;
}

namespace raft {

FileStorage *FileStorage::Open(const std::string &filename) {
  int fd = ::open(filename.c_str(), O_CREAT | O_RDWR, 0644);
  if (fd < 0) {
    return nullptr;
  }
  auto ret = new FileStorage();
  ret->AllocateNewInternalBuffer(FileStorage::kInitBufSize);
  ret->fd_ = fd;
  if (auto size = ::read(fd, &(ret->header_), kHeaderSize) < kHeaderSize) {
    // No complete header
    ret->InitializeHeaderOnCreation();
    ret->PersistHeader();
  }
  ret->ConstructExtents();
  return ret;
}

void FileStorage::Close(FileStorage *file) { delete file; }

void FileStorage::PersistEntries(raft_index_t lo, raft_index_t hi,
                                 const std::vector<LogEntry> &batch) {
#ifdef ENABLE_PERF_RECORDING
  util::PersistencePerfCounter perf_counter(0);
#endif

  if (lo > hi) {
    return;
  }
  auto ser = Serializer::NewSerializer();
  auto check_raft_index = lo;
  for (const auto &ent : batch) {
    auto write_size = ser.getSerializeSize(ent);

#ifdef ENABLE_PERF_RECORDING
    perf_counter.persist_size += write_size;
#endif

    if (!this->buf_ || write_size > this->buf_size_) {
      AllocateNewInternalBuffer(write_size);
    }
    ser.serialize_logentry_helper(&ent, this->buf_);
    Append(this->buf_, write_size);

    assert(check_raft_index == ent.Index());
    check_raft_index++;

    MaybeUpdateLastIndexAndTerm(ent.Index(), ent.Term());
  }
  PersistHeader();

#ifdef ENABLE_PERF_RECORDING
  perf_counter.Record();
  PERF_LOG(&perf_counter);
#endif
}

void FileStorage::LogEntries(std::vector<LogEntry> *entries) {
  auto der = Serializer::NewSerializer();
  auto last_index = header_.lastLogIndex;
  entries->clear();
  entries->resize(last_index + 1);

  auto read_off = kHeaderSize;
  // Read log entry one by one
  while (true) {
    if (read_off + sizeof(LogEntry) > header_.last_off) {
      break;
    }
    auto off = lseek(fd_, read_off, SEEK_SET);
    ::read(fd_, buf_, this->buf_size_);

    LogEntry ent;
    auto next = der.deserialize_logentry_helper(buf_, &ent);
    read_off += der.getSerializeSize(ent);
    if (ent.Index() <= header_.lastLogIndex) {
      (*entries)[ent.Index()] = ent;
    }
  }
}

void FileStorage::AppendEntry(const LogEntry &ent) {
  if (ent.Index() != header_.lastLogIndex + 1) {
    return;
  }
  auto ser = Serializer::NewSerializer();
  auto write_size = ser.getSerializeSize(ent);
  LOG(util::kRaft, "Storage: AppendEntry I%d, Size=%d", ent.Index(), write_size);
  if (!this->buf_ || write_size > this->buf_size_) {
    AllocateNewInternalBuffer(write_size);
  }
  auto extent = EntryExtent{header_.last_off, write_size};
  ser.serialize_logentry_helper(&ent, this->buf_);
  Append(this->buf_, write_size);
  MaybeUpdateLastIndexAndTerm(ent.Index(), ent.Term());
  UpdateExtents(ent.Index(), extent);
}

void FileStorage::DeleteEntriesFrom(raft_index_t raft_index) {
  LOG(util::kRaft, "Storage: Delete Entries From I%d", raft_index);
  if (raft_index > header_.lastLogIndex || extents_.count(raft_index) == 0) {
    return;
  }
  auto extent = extents_.at(raft_index);
  Truncate(extent.offset);
  // Update last index and size
  header_.lastLogIndex = raft_index - 1;
  header_.last_off = extent.offset;

  // Note that there is no need to update extents since last Log Index is
  // updated
}

void FileStorage::OverwriteEntry(raft_index_t raft_index, const LogEntry &ent) {
  LOG(util::kRaft, "Storage: Overwrite I%d", raft_index);
  DeleteEntriesFrom(raft_index);
  AppendEntry(ent);
}

// Only called in Open file
void FileStorage::ConstructExtents() {
  extents_.reserve(kInitExtentsCapacity);
  auto der = Serializer::NewSerializer();
  auto read_off = kHeaderSize;

  // Read log entry one by one
  while (true) {
    if (read_off + sizeof(LogEntry) > header_.last_off) {
      break;
    }
    auto off = lseek(fd_, read_off, SEEK_SET);
    ::read(fd_, buf_, this->buf_size_);

    LogEntry ent;
    auto next = der.deserialize_logentry_helper(buf_, &ent);
    UpdateExtents(ent.Index(), EntryExtent{read_off, der.getSerializeSize(ent)});
    read_off += der.getSerializeSize(ent);
  }
}

}  // namespace raft
