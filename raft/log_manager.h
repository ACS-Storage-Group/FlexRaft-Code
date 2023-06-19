#pragma once

#include "log_entry.h"

namespace raft {

enum Status {
  kOk = 0,

  // LogManager related error code
  kMemAllocateFail = 1,
  kIndexBeyondRange = 2,
  kNeedSnapshot = 3,
};

class Storage;
// LogManager is a class responsible for managing log entries. It provides
// uniformly raft_index indexing scheme and is able to discard outdated log
// entries in log compaction LogManager uses a ring buffer to store log entries.
class LogManager {
  static constexpr int kExpandFactor = 4;
  // initialized capacity, to avoid frequent allocation
  static constexpr int kInitCapacity = 100000;

 public:
  // Callback function when an entry is deleted in log manager
  using Deleter = void (*)(LogEntry *);
  friend void ReadLogFromPersister(LogManager *lm, Storage *persister);

 public:
  // Default constructor is not allowed, one LogManager needs at least one
  // persister in order to guarantee crash safety
  LogManager() = delete;

  // Any copy semantics are not allowed
  LogManager(const LogManager &) = delete;
  LogManager &operator=(const LogManager &) = delete;

  LogManager(Storage *persister, int64_t initial_size = 1, Deleter deleter = nullptr);

  ~LogManager();

 public:
  // Allocate a LogManager with capacity of initial_size Use "delete" to release
  // this logmananger object
  static LogManager *NewLogManager(Storage *persister);

 public:
  int Capacity() const { return capacity_; }

  int Count() const { return count_; }

  // Return the index of the last log entry stored in this manager, if there is
  // no log entries(including snapshot), return 0
  raft_index_t LastLogEntryIndex() const;

  // Return the term of the last log entry stored in this manager, if there is
  // no log entries(including snapshot), return 0
  raft_term_t LastLogEntryTerm() const;

  raft_term_t TermAt(raft_index_t idx) const;

 public:
  /// Public interface
  Status AppendLogEntry(const LogEntry &entry);

  // Replace the entry at specified raft index using input entry
  // TODO: Proof of log matching property in flexibleK
  Status OverWriteLogEntry(const LogEntry &entry, raft_index_t idx);

  // Delete all log entries whose raft index >= idx up to the newest
  Status DeleteLogEntriesFrom(raft_index_t idx);

  // Get all log entries whose raft index >= idx up to the newest
  Status GetLogEntriesFrom(raft_index_t idx, std::vector<LogEntry> *vec);

  // Discard all log entries whose raft index < idx
  Status DiscardLogEntriesBefore(raft_index_t idx);

  // Get a log entry at specified raft index, return nullptr if requested index
  // is beyond range
  LogEntry *GetSingleLogEntry(raft_index_t idx);

  // Returns an entry object at specified raft index
  Status GetEntryObject(raft_index_t idx, LogEntry *ent);

  // Return the raft index and term of the last snapshot log entry
  raft_index_t LastSnapshotIndex() const { return last_snapshot_index_; }
  raft_term_t LastSnapshotTerm() const { return last_snapshot_term_; }

 private:
  // Check if there is still enough room for next a few log entries
  // If there is, returns 0 to indicate ok; otherwise do allocation
  // and movement. Return 1 if any error occurs
  Status ensureCapacity(int entry_cnt);

  // Convert raft index to array index for writing or retrieving
  // items
  int raftIndexToArrayIndex(raft_index_t idx) const;

  // Append an entry to in-memory log buffer without persistence
  Status appendEntryHelper(const LogEntry &entry);

 private:
  // Dealing with concurrency control, might be used
  std::mutex mtx_;

  // The capacity of container storing log entries
  int capacity_;

  // Two end-points of a ring buffer, represented as array index. front_/back_
  // are the array index of first/last log entry
  int front_, back_;

  // Number of log entries stored in this Log Manager
  int count_;

  // The base of in-memory log entries, i.e. The index of the first in-memory
  // log entry, if there is not an valid log entry yet, this value would be the
  // index of next log entry
  raft_index_t base_;

  // Persist raft log entries when needed
  Storage *persister_;

  // The index and term of the last snapshot entry
  raft_index_t last_snapshot_index_;
  raft_term_t last_snapshot_term_;

  // Pointer to the first log entries
  LogEntry *entries_;

  Deleter deleter_;
};

// NOTE: This function does not check if idx is valid. The caller
// of this function must ensure idx parameter is within the range
// of in-memory log entries
inline int LogManager::raftIndexToArrayIndex(raft_index_t idx) const {
  return static_cast<int>((idx - base_ + front_) % Capacity());
}

inline raft_index_t LogManager::LastLogEntryIndex() const {
  if (Count() == 0) {  // No available in-memory log entries
    return last_snapshot_index_;
  }
  // Remember to add Capacity in case this value is negative
  auto idx = (back_ - 1 + Capacity()) % Capacity();
  return entries_[idx].Index();
}

inline raft_index_t LogManager::LastLogEntryTerm() const {
  if (Count() == 0) {  // No available in-memory log entries
    return last_snapshot_term_;
  }
  auto idx = (back_ - 1 + Capacity()) % Capacity();
  return entries_[idx].Term();
}

// Reconstruct log entries from failure by reading all log entries
// from a persister.
// NOTE: Only use this function when a persister is initialized
void ReadLogFromPersister(LogManager *lm, Storage *persister);
}  // namespace raft
