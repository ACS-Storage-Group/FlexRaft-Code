#include "log_manager.h"

#include <cstdlib>
#include <new>
#include <vector>

#include "log_entry.h"
#include "storage.h"
#include "util.h"

namespace raft {
// A default deleter that release dynamically allocated memory
void default_Deleter(LogEntry *entry) {
  delete[] entry->NotEncodedSlice().data();
  delete[] entry->FragmentSlice().data();
  entry->~LogEntry();
}

LogManager::LogManager(Storage *persister, int64_t initial_cap, Deleter deleter)
    : capacity_(initial_cap),
      front_(0),
      back_(0),
      count_(0),
      base_(1),  // For empty log array, set base to be 1 instead of 0
      persister_(persister),
      last_snapshot_index_(0),
      last_snapshot_term_(0),
      deleter_(deleter) {
  entries_ = new LogEntry[Capacity()];
  if (deleter_ == nullptr) {
    deleter_ = default_Deleter;
  }
}

// Create a new log manager from persisted storage
LogManager *LogManager::NewLogManager(Storage *storage) {
  auto ret = new LogManager(storage, 100000, nullptr);
  if (storage != nullptr) {
    std::vector<LogEntry> vec;
    storage->LogEntries(&vec);
    // LOG(util::kRaft, "entry cnt=%d", vec.size());
    // The first entry of vec is of no means, it's basically for alignement.
    for (int raft_index = 1; raft_index < vec.size(); ++raft_index) {
      ret->AppendLogEntry(vec[raft_index]);
    }
    // LOG(util::kRaft, "Recover from storage LI%d", ret->LastLogEntryIndex());
    assert(storage->LastIndex() == ret->LastLogEntryIndex());
  }
  return ret;
}

// Destructor: Calling user provided deleter to release all dynamically
// allocated memory and release the array itself.
// Also needs to call persister to synchronize all changes
LogManager::~LogManager() {
  int start_idx = front_;
  for (int i = 0; i < Count(); ++i) {
    this->deleter_(&entries_[start_idx]);
    start_idx = (start_idx + 1) % Capacity();
  }
  delete[] entries_;
  // TODO(kqh): Call persister to persist all changes, maybe looks like:
  //  persister_->Sync();
}

Status LogManager::ensureCapacity(int entry_cnt) {
  if (Count() + entry_cnt <= Capacity()) {
    return kOk;
  }

  int request_cap = capacity_;
  while (request_cap < Capacity() + entry_cnt) {
    request_cap *= kExpandFactor;  // expand factor = 4
  }

  LogEntry *new_array = nullptr;
  try {
    new_array = new LogEntry[request_cap];
  } catch (const std::bad_alloc &) {
    return kMemAllocateFail;
  }

  int new_idx = 0, old_idx = front_;
  for (int i = 0; i < Count(); ++i) {
    new_array[new_idx] = entries_[old_idx];
    new_idx = (new_idx + 1) % request_cap;  // Update two indexes
    old_idx = (old_idx + 1) % Capacity();
  }

  // Update meta information
  capacity_ = request_cap;
  front_ = 0;
  back_ = new_idx;

  delete[] entries_;
  entries_ = new_array;

  return kOk;
}

Status LogManager::appendEntryHelper(const LogEntry &entry) {
  Status stat;
  if ((stat = ensureCapacity(1)) != 0) {
    return stat;
  }

  entries_[back_] = entry;
  back_ = (back_ + 1) % Capacity();
  ++count_;

  return kOk;
}

Status LogManager::AppendLogEntry(const LogEntry &entry) {
  std::lock_guard<std::mutex> lock(mtx_);
  auto stat = appendEntryHelper(entry);
  if (stat == kOk && persister_ != nullptr) {
    // persister_->PersistNewLogEntry(entry);
  }
  return stat;
}

Status LogManager::DeleteLogEntriesFrom(raft_index_t idx) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (Count() <= 0 || idx < base_ || idx > LastLogEntryIndex()) {
    return kIndexBeyondRange;
  }

  int start_idx = raftIndexToArrayIndex(LastLogEntryIndex());
  int delete_cnt = static_cast<int>(LastLogEntryIndex() - idx + 1);

  // Calculate the delete size for persister
  size_t delSize = 0;

  for (int i = 0; i < delete_cnt; ++i) {
    // delSize += (kLogEntryHdrSize + entries_[start_idx].Length());
    this->deleter_(&entries_[start_idx]);
    // Decrement start_idx to get the next item to be "delete"
    start_idx = (start_idx - 1 + Capacity()) % Capacity();
  }

  if (persister_ != nullptr) {
    // persister_->DeleteLogEntries(idx, delSize);
  }

  // start_idx points to the next entry to be deleted, thus
  // we increment it to get the true back_ answer
  back_ = (start_idx + 1) % Capacity();
  count_ -= delete_cnt;
  return kOk;
}

// Currently we simply use std::vector to return an array of log entries with
// shallow copy. The caller of this function must ensure vec is cleared.
Status LogManager::GetLogEntriesFrom(raft_index_t idx, std::vector<LogEntry> *vec) {
  // std::lock_guard<std::mutex> lock(mtx_);
  vec->clear();

  if (Count() <= 0 || idx > LastLogEntryIndex()) {
    return kIndexBeyondRange;
  }

  if (idx < base_) {
    return kNeedSnapshot;
  }

  // start_idx is initialized to the array index whose raft index is exactly
  // input "raft_index_t idx"
  int start_idx = raftIndexToArrayIndex(idx);
  int fetch_cnt = static_cast<int>(LastLogEntryIndex() - idx + 1);

  for (int i = 0; i < fetch_cnt; ++i) {
    vec->push_back(entries_[start_idx]);
    start_idx = (start_idx + 1) % Capacity();
  }
  return kOk;
}

Status LogManager::DiscardLogEntriesBefore(raft_index_t idx) {
  if (Count() <= 0 || idx < base_) {  // There is no entry to discard
    return kOk;
  }
  // Specified discard range might be beyond stored range
  raft_index_t dest = std::min(LastLogEntryIndex(), idx);
  int start_idx = front_, discard_cnt = static_cast<int>(dest - base_);

  for (int i = 0; i < discard_cnt; ++i) {
    this->deleter_(&entries_[start_idx]);
    start_idx = (start_idx + 1) % Capacity();
  }

  // Update Manager meta data
  base_ = dest;
  front_ = start_idx;
  count_ -= discard_cnt;

  // TODO: The persister must atomically persist this change
  return kOk;
}

LogEntry *LogManager::GetSingleLogEntry(raft_index_t idx) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (Count() <= 0 || idx < base_ || idx > LastLogEntryIndex()) {
    return nullptr;
  }
  // Maybe we only need to return term for prevLogTerm in AppendEntries
  // call?
  return &entries_[raftIndexToArrayIndex(idx)];
}

Status LogManager::GetEntryObject(raft_index_t idx, LogEntry *ent) {
  auto ptr = GetSingleLogEntry(idx);
  if (ptr == nullptr) {
    return kIndexBeyondRange;
  }
  *ent = *ptr;
  return kOk;
}

Status LogManager::OverWriteLogEntry(const LogEntry &entry, raft_index_t idx) {
  if (Count() <= 0 || idx < base_ || idx > LastLogEntryIndex()) {
    return kIndexBeyondRange;
  }
  entries_[raftIndexToArrayIndex(idx)] = entry;
  return kOk;
}

raft_term_t LogManager::TermAt(raft_index_t idx) const {
  if (idx < base_ - 1 || idx > LastLogEntryIndex()) {
    // base_ - 1 is the last snapshotted entries, we can still access
    // its term
    return -1;
  }
  if (idx == last_snapshot_index_) {
    return last_snapshot_term_;
  }
  return entries_[raftIndexToArrayIndex(idx)].Term();
}

void ReadLogFromPersister(LogManager *lm, Storage *persister) {
  if (persister == nullptr) {
    return;
  }
  LogEntry entry;
  /* while (persister->NextEntry(&entry)) { */
  /*   // NOTE: Do not call lm->AppendEntries since this interface */
  /*   // will persist this entry, incuring an infinite loop */
  /*   lm->appendEntryHelper(entry); */
  /* } */
}
}  // namespace raft
