#pragma once
#include "log_entry.h"

namespace raft {
// Rsm is short for Replicate State machine
class Rsm {
 public:
  virtual void ApplyLogEntry(LogEntry entry) = 0;
};
}  // namespace raft
