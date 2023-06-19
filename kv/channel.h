#pragma once
#include "concurrent_queue.h"
#include "log_entry.h"
#include "rsm.h"
namespace kv {
class Channel : public raft::Rsm {
 public:
  static Channel* NewChannel(size_t capacity) { return new Channel(capacity); }

  Channel(size_t capacity) : queue_(capacity){};
  Channel() = default;
  ~Channel() = default;

  void ApplyLogEntry(raft::LogEntry entry) override { queue_.Push(entry); }
  raft::LogEntry Pop() { return queue_.Pop(); }
  bool TryPop(raft::LogEntry& ent) { return queue_.TryPop(ent); }

 private:
  ConcurrentQueue<raft::LogEntry> queue_;
};
}  // namespace kv
