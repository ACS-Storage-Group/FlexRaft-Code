#include "log_manager.h"

#include <cstdlib>
#include <cstring>
#include <random>

#include "gtest/gtest.h"
#include "log_entry.h"
#include "raft_type.h"

namespace raft {

class LogManagerTest : public ::testing::Test {
 public:
  static constexpr raft_index_t kPutIndex = 1000000;

 public:
  LogManagerTest() : lm_(new LogManager(nullptr)) {}
  ~LogManagerTest() { delete lm_; }

 public:
  // A helper function that appends a single entry to log manager
  void Append(raft_index_t raft_index, raft_term_t raft_term) {
    LogEntry ent;
    ent.SetIndex(raft_index);
    ent.SetTerm(raft_term);
    ent.SetType(kNormal);
    lm_->AppendLogEntry(ent);
  }

  // Require: Index is within valid range
  LogEntry *Get(raft_index_t raft_index) { return lm_->GetSingleLogEntry(raft_index); }

  void Get(raft_index_t raft_index, std::vector<LogEntry> *vec) {
    lm_->GetLogEntriesFrom(raft_index, vec);
  }

  raft_index_t LastLogEntryIndex() const { return lm_->LastLogEntryIndex(); }
  raft_index_t LastLogEntryTerm() const { return lm_->LastLogEntryTerm(); }

  void CheckEntryVector(const std::vector<LogEntry> &vec, raft_index_t min_index,
                        raft_index_t max_index) {
    auto check_idx = min_index;
    for (const auto &ent : vec) {
      ASSERT_EQ(ent.Index(), check_idx);
      ++check_idx;
    }
  }

  // Check all entries within range [from..to] has specified term
  void CheckValidLogEntry(raft_index_t from, raft_index_t to, raft_term_t term) {
    for (auto check_index = from; check_index <= to; ++check_index) {
      auto ent = Get(check_index);
      ASSERT_NE(ent, nullptr);
      ASSERT_EQ(ent->Index(), check_index);
      ASSERT_EQ(ent->Term(), term);
      ASSERT_EQ(ent->Type(), kNormal);
    }
  }

  // Check all entries with index >= from are not avaliable. We set
  // another paramter "to" to specify the max check raft index
  void CheckInvalidLogEntry(raft_index_t from, raft_index_t to) {
    for (auto check_index = from; check_index <= to; ++check_index) {
      auto ent = Get(check_index);
      ASSERT_EQ(ent, nullptr);  // Invalid entry
    }
  }

  void OverwriteEntry(raft_index_t from, raft_index_t to, raft_term_t term) {
    for (raft_index_t raft_index = from; raft_index <= to; ++raft_index) {
      LogEntry ent;
      ent.SetIndex(raft_index);
      ent.SetTerm(term);
      ent.SetType(kNormal);
      lm_->OverWriteLogEntry(ent, raft_index);
    }
  }

  void CheckGetBatchEntry(int check_run, raft_term_t term) {
    std::vector<LogEntry> vec;
    for (int run = 1; run < check_run; ++run) {
      vec.clear();
      auto check_idx = validRandomIndex();
      Get(check_idx, &vec);
      CheckEntryVector(vec, check_idx, LastLogEntryIndex());
    }
  }

  void AppendLogEntryUpTo(raft_index_t raft_index, raft_term_t term) {
    for (int put_index = 1; put_index <= raft_index; ++put_index) {
      Append(put_index, term);
    }
  }

  void DeleteEntryFrom(raft_index_t raft_index) { lm_->DeleteLogEntriesFrom(raft_index); }

  raft_index_t validRandomIndex() const { return rand() % LastLogEntryIndex() + 1; }

 private:
  LogManager *lm_;
};

TEST_F(LogManagerTest, TestSimpleAppendAndGet) {
  const raft_term_t kPutTerm = 1;

  AppendLogEntryUpTo(kPutIndex, kPutTerm);

  ASSERT_EQ(LastLogEntryIndex(), kPutIndex);
  ASSERT_EQ(LastLogEntryTerm(), 1);

  auto last_index = LastLogEntryIndex();
  CheckValidLogEntry(1, last_index, kPutTerm);
}

TEST_F(LogManagerTest, TestAppendAndBatchGet) {
  const raft_term_t kPutTerm = 2;
  const int kCheckIter = 100;

  AppendLogEntryUpTo(kPutIndex, kPutTerm);

  ASSERT_EQ(LastLogEntryIndex(), kPutIndex);
  ASSERT_EQ(LastLogEntryTerm(), kPutTerm);

  CheckGetBatchEntry(kCheckIter, kPutTerm);
}

TEST_F(LogManagerTest, TestAppendAndDelete) {
  const raft_term_t kPutTerm = 3;
  const int kCheckIter = 100;

  AppendLogEntryUpTo(kPutIndex, kPutTerm);

  ASSERT_EQ(LastLogEntryIndex(), kPutIndex);
  ASSERT_EQ(LastLogEntryTerm(), kPutTerm);

  const int kDeleteRun = 10;
  for (int run = 1; run <= kDeleteRun; ++run) {
    raft_index_t delete_index = validRandomIndex();
    DeleteEntryFrom(delete_index);
    ASSERT_EQ(delete_index - 1, LastLogEntryIndex());
    ASSERT_EQ(kPutTerm, LastLogEntryTerm());
  }

  // Check the rest raft index are valid to access
  auto last_index = LastLogEntryIndex();
  CheckValidLogEntry(1, last_index, kPutTerm);
  CheckInvalidLogEntry(last_index + 1, kPutIndex);
  CheckGetBatchEntry(kCheckIter, kPutTerm);
}

TEST_F(LogManagerTest, TestOverWrite) {
  const raft_term_t kPutTerm = 3;

  AppendLogEntryUpTo(kPutIndex, kPutTerm);

  ASSERT_EQ(LastLogEntryIndex(), kPutIndex);
  ASSERT_EQ(LastLogEntryTerm(), kPutTerm);

  auto from = LastLogEntryIndex() / 2;
  auto to = LastLogEntryIndex();

  OverwriteEntry(from, to, kPutTerm + 1);

  auto last_index = LastLogEntryIndex();
  CheckValidLogEntry(1, from - 1, kPutTerm);
  CheckValidLogEntry(from, last_index, kPutTerm + 1);
}
}  // namespace raft
