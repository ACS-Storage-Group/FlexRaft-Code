#pragma once
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <unordered_map>

#include "log_entry.h"
#include "raft_type.h"
#include "serializer.h"

namespace raft {

// Storage is an interface used for retriving (and store) persisted raft status
// including current term, voteFor, persisted log entries
class Storage {
public:
  struct PersistRaftState {
    // Indicating if this state is valid, for example, when creating
    // a persister on first bootstrap, there is no valid raft state yet
    bool valid;
    raft_term_t persisted_term;
    raft_node_id_t persisted_vote_for;
  };

public:
  // LastIndex returns the raft index of last persisted log entries, if
  // there is no valid log entry, returns 0
  virtual raft_index_t LastIndex() const = 0;

  // Return the persisted raft state. Mark valid field as false if there
  // is no valid raft state
  virtual PersistRaftState PersistState() const = 0;
  virtual void PersistState(const PersistRaftState &state) = 0;

  // Get all persisted but not discarded log entries and store them in a
  // temporary vector
  virtual void LogEntries(std::vector<LogEntry> *entries) = 0;

  // Persist log entries within specified range to storage, there might be old
  // version of data that has been persisted in storage, if so, old version of
  // these entries would be ignored when reading. However, upon reading, this
  // may require us to scan the whole log file
  virtual void PersistEntries(raft_index_t lo, raft_index_t hi,
                              const std::vector<LogEntry> &batch) = 0;

  // Persist the last index attribute would ignore any entries with higher index
  // in the next reading. It can be used when an entry deleting occurs
  virtual void SetLastIndex(raft_index_t raft_index) = 0;

  virtual void AppendEntry(const LogEntry& ent) = 0;
  virtual void OverwriteEntry(raft_index_t raft_index, const LogEntry &ent) = 0;
  virtual void DeleteEntriesFrom(raft_index_t raft_index) = 0;
  virtual void Sync() = 0;


  virtual ~Storage() = default;
};

// This class is only for unit test, it is used for simulating the behaviour
// of a persister by place them simply in memory. The test module may feel
// free to inherit this class and override corresponding methods
class MemStorage : public Storage {
public:
  raft_index_t LastIndex() const override {
    if (!persisted_entries_.size()) {
      return 0;
    } else {
      return (persisted_entries_.end() - 1)->Index();
    }
  }

  PersistRaftState PersistState() const override {
    return {true, persisted_term_, persisted_vote_for_};
  }

  void PersistState(const PersistRaftState &state) override {}

  void LogEntries(std::vector<LogEntry> *entries) override {
    *entries = persisted_entries_;
  }

  // Should do nothing
  void PersistEntries(raft_index_t lo, raft_index_t hi,
                      const std::vector<LogEntry> &batch) override {}

  void SetLastIndex(raft_index_t raft_index) override {}

  void AppendEntry(const LogEntry& ent) override {}
  void OverwriteEntry(raft_index_t raft_index, const LogEntry &ent) override {}
  void DeleteEntriesFrom(raft_index_t raft_index) override {}
  void Sync() override {}

protected:
  raft_term_t persisted_term_;
  raft_node_id_t persisted_vote_for_;
  std::vector<LogEntry> persisted_entries_;
};

class FileStorage : public Storage {
  struct Header {
    PersistRaftState raft_state;
    raft_index_t lastLogIndex;
    raft_term_t lastLogTerm;
    size_t last_off;
  };

  // This struct records a continous data fragment of an entry
  // that is stored within a file
  struct EntryExtent {
    uint64_t offset;
    uint64_t size;
  };

public:
  // Create or open an existed log file
  static FileStorage *Open(const std::string &name);
  static void Close(FileStorage *file);

  ~FileStorage() {
    delete[] buf_;
    ::close(fd_);
  }

  raft_index_t LastIndex() const override { return header_.lastLogIndex; };

  PersistRaftState PersistState() const override { return header_.raft_state; }

  void PersistState(const PersistRaftState &state) override {
    if (!state.valid) {
      return;
    }
    header_.raft_state = state;
    PersistHeader();
  };

  void LogEntries(std::vector<LogEntry> *entries) override;
  void PersistEntries(raft_index_t lo, raft_index_t hi,
                      const std::vector<LogEntry> &batch) override;

  void AppendEntry(const LogEntry &ent) override;

  // Overwrite an entry may truncate all data following it
  void OverwriteEntry(raft_index_t raft_index, const LogEntry &ent) override;

  void DeleteEntriesFrom(raft_index_t raft_index) override;

  void SetLastIndex(raft_index_t raft_index) override {
    header_.lastLogIndex = raft_index;
    PersistHeader();
  };

  void Sync() override { PersistHeader(); }

private:
  void AllocateNewInternalBuffer(size_t size) {
    if (this->buf_) {
      delete[] buf_;
    }
    this->buf_ = new char[size + 20];
    this->buf_size_ = size;
  }

  void PersistHeader() {
    lseek(fd_, 0, SEEK_SET);
    ::write(fd_, &header_, kHeaderSize);
    SyncFd(fd_);
  }

  void InitializeHeaderOnCreation() {
    header_.raft_state = PersistRaftState{false, 0, 0};
    header_.lastLogIndex = 0;
    header_.lastLogTerm = 0;
    header_.last_off = kHeaderSize;
  }

  // Append data of specified slice to the file
  void Append(const char *data, size_t size) {
    lseek(fd_, header_.last_off, SEEK_SET);
    while (size > 0) {
      auto write_size = ::write(fd_, data, size);
      size -= write_size;
      data += write_size;
      header_.last_off += write_size;
    }
  };

  void SyncFd(int fd) { ::fsync(fd); }

  void MaybeUpdateLastIndexAndTerm(raft::raft_index_t raft_index,
                                   raft::raft_term_t raft_term) {
    if (raft_index > header_.lastLogIndex) {
      header_.lastLogIndex = raft_index;
      header_.lastLogTerm = raft_term;
    }
  }

  void Truncate(uint64_t size) {
    ftruncate(fd_, size);
  }

  void UpdateExtents(raft_index_t raft_index, EntryExtent extent) {
    extents_.insert_or_assign(raft_index, extent);
  }

  void ConstructExtents();

private:
  Header header_;
  int fd_;
  char *buf_; // Internal buffer
  size_t buf_size_;

  std::unordered_map<raft_index_t, EntryExtent> extents_;

  static constexpr size_t kInitBufSize = 16 * 1024 * 1024; // 16MB
  static const size_t kHeaderSize = sizeof(Header);
  static const size_t kInitExtentsCapacity = 100000;
};

} // namespace raft
