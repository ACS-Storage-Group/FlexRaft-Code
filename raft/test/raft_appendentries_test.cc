#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "log_entry.h"
#include "raft.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "rpc.h"
#include "storage.h"

namespace raft {

// A storage implementation that simulates a persister, we need this class to construct
// pre-exists log entries during test
class StorageMock : public MemStorage {
 public:
  void Append(raft_term_t raft_term, raft_index_t raft_index) {
    LogEntry ent;
    ent.SetTerm(raft_term);
    ent.SetIndex(raft_index);
    persisted_entries_.push_back(ent);
  }

  struct Entry {
    raft_term_t raft_term;
    raft_index_t raft_index;
    bool operator==(const Entry& rhs) const {
      return raft_term == rhs.raft_term && raft_index == rhs.raft_index;
    }
  };

  using Logs = std::vector<Entry>;

  // Construct a storage mock from a bunch of (term, index) pair. We expect that a raft
  // peer that build based on this storage would contain log entries with the same index
  // and term as input std::vector<Entry>
  static StorageMock* BuildStorage(std::vector<Entry> entries) {
    auto ret = new StorageMock();
    for (const auto& ent : entries) {
      ret->Append(ent.raft_term, ent.raft_index);
    }
    return ret;
  }
};

// A TestRaftState is a subset of raft state, used to track the status change before
// and after one RPC
struct TestRaftState {
  raft_term_t raft_term;
  RaftRole role;
  raft_index_t commit_index;
  StorageMock::Logs entries;

  bool operator==(const TestRaftState& rhs) const {
    return raft_term == rhs.raft_term && role == rhs.role &&
           commit_index == rhs.commit_index && entries == rhs.entries;
  }

  static TestRaftState CollectRaftState(const RaftState* raft) {
    auto ret = TestRaftState{raft->CurrentTerm(), raft->Role(), raft->CommitIndex()};
    auto last_raft_index = raft->LastLogIndex();
    for (raft_index_t i = 1; i <= last_raft_index; ++i) {
      ret.entries.push_back({raft->TermAt(i), i});
    }
    return ret;
  }
};

// This test tests the state changes when a server receives an AppendEntries RPC call
// Since the AppendEntries call only changes the raft state based on the index and term
// of log entries, we omit any other fields of an log entry
class RaftAppendEntriesTest : public ::testing::Test {
 public:
  // A TestCase is the configuration of one test condition. When running a test, first
  // construct a RaftState instance based on this init_state and storage, then let the
  // state process specified AppendEntriesArgs and finally check if the state is exactly
  // the same as "expect_state" and if the reply is exactly the same as "expect_reply"
  struct TestCase {
    TestRaftState init_state;
    AppendEntriesArgs args;
    TestRaftState expect_state;
    AppendEntriesReply expect_reply;
  };

  static bool AppendEntriesReplyEqual(const AppendEntriesReply& lhs,
                                      const AppendEntriesReply& rhs) {
    return std::memcmp(&lhs, &rhs, sizeof(AppendEntriesReply)) == 0;
  }

 public:
  static void RunSingleTest(const TestCase& test);
  static void RunAllTests(const std::vector<TestCase>& tests) {
    std::for_each(tests.begin(), tests.end(), RunSingleTest);
  }
};

static std::vector<LogEntry> ConstructLogEntry(const StorageMock::Logs& entry) {
  std::vector<LogEntry> ret;
  auto construct = [&ret](const StorageMock::Entry& ent) {
    LogEntry log_ent;
    log_ent.SetTerm(ent.raft_term);
    log_ent.SetIndex(ent.raft_index);
    ret.push_back(log_ent);
  };
  std::for_each(entry.begin(), entry.end(), construct);
  return ret;
}

void RaftAppendEntriesTest::RunSingleTest(const TestCase& test) {
  auto storage = StorageMock::BuildStorage(test.init_state.entries);
  // This test does not require rpc client since it's passive
  auto config = RaftConfig{1, {}, storage};

  auto raft_state = RaftState::NewRaftState(config);
  raft_state->SetCurrentTerm(test.init_state.raft_term);
  raft_state->SetRole(test.init_state.role);
  raft_state->SetCommitIndex(test.init_state.commit_index);

  AppendEntriesReply reply;
  raft_state->Process(const_cast<AppendEntriesArgs*>(&test.args), &reply);

  ASSERT_TRUE(AppendEntriesReplyEqual(test.expect_reply, reply));
  ASSERT_EQ(test.expect_state, TestRaftState::CollectRaftState(raft_state));

  delete raft_state;
  delete storage;
}

// A Test class that simulates the state changes when receiving a response of an
// AppendEntries RPC call
class RaftHandleAppendEntriesReplyTest : public ::testing::Test {
 public:
  // A TestCase is the configuration of one test condition. When running a test, first
  // construct a RaftState instance based on this init_state and storage, then let the
  // state process specified AppendEntriesArgs and finally check if the state is exactly
  // the same as "expect_state" and if the reply is exactly the same as "expect_reply"
  struct TestCase {
    TestRaftState init_state;
    AppendEntriesReply input_reply;
    TestRaftState expect_state;
  };

 public:
  static void RunSingleTest(const TestCase& test);
  static void RunAllTests(const std::vector<TestCase>& tests) {
    std::for_each(tests.begin(), tests.end(), RunSingleTest);
  }
};

void RaftHandleAppendEntriesReplyTest::RunSingleTest(const TestCase& test) {
  auto storage = StorageMock::BuildStorage(test.init_state.entries);
  // This test does not require rpc client since it's passive
  auto config = RaftConfig{1, {{1, nullptr}, {2, nullptr}, {3, nullptr}}, storage};

  auto raft_state = RaftState::NewRaftState(config);
  raft_state->SetCurrentTerm(test.init_state.raft_term);
  raft_state->SetRole(test.init_state.role);
  raft_state->SetCommitIndex(test.init_state.commit_index);

  raft_state->Process(const_cast<AppendEntriesReply*>(&test.input_reply));

  ASSERT_EQ(test.expect_state, TestRaftState::CollectRaftState(raft_state));

  delete raft_state;
  delete storage;
}

TEST_F(RaftAppendEntriesTest, TestAppendEntriesTerm) {
  std::vector<TestCase> tests = {
      // Case: Receive server is at Term 2, with no existed log entries
      // Input: Input AppendEntries Args has term 1
      // Expect Result: Refuse to accepect this AppendEntries call and remain old state
      TestCase{
          TestRaftState{2, kFollower, 0, {}},
          AppendEntriesArgs{1, 2, 0, 0, 0, 1, 0, ConstructLogEntry({{1, 1}})},
          TestRaftState{2, kFollower, 0, {}},
          AppendEntriesReply{2, false, 0, 1},
      },
      // Case: Receive server is at Term 0, with no existed log entries
      // Input: Input AppendEntries Args has term2, carries no log entry, basically a
      //        heartbeat message
      // Expect Result: Accept this Heartbeat message and converts to at Term 2
      TestCase{
          TestRaftState{0, kFollower, 0, {}},
          AppendEntriesArgs{1, 2, 0, 0, 0, 0, 0, {}},
          TestRaftState{1, kFollower, 0, {}},
          AppendEntriesReply{1, true, 1, 1},
      },
      // Case: Receive server is at Term 1, with no existed log entries
      // Input: Input carries the same term and a single log entry
      // Expect Result: Accpect this AppendEntriesArgs and accept this log entry
      TestCase{
          TestRaftState{1, kFollower, 0, {}},
          AppendEntriesArgs{1, 2, 0, 0, 0, 1, 0, ConstructLogEntry({{1, 1}})},
          TestRaftState{1, kFollower, 0, {{1, 1}}},
          AppendEntriesReply{1, true, 2, 1},
      },
  };
  RunAllTests(tests);
}

TEST_F(RaftAppendEntriesTest, TestConvertToFollowerOnReceivingAppendEntries) {
  std::vector<TestCase> tests = {
      TestCase{
          TestRaftState{1, kCandidate, 0, {{1, 1}, {1, 2}}},
          AppendEntriesArgs{1, 2, 2, 1, 0, 0, 0, ConstructLogEntry({})},
          TestRaftState{1, kFollower, 0, {{1, 1}, {1, 2}}},
          AppendEntriesReply{1, true, 3, 1},
      },
      TestCase{
          TestRaftState{1, kCandidate, 0, {{1, 1}, {1, 2}}},
          AppendEntriesArgs{1, 2, 2, 1, 0, 3, 0,
                            ConstructLogEntry({{1, 3}, {2, 4}, {3, 5}})},
          TestRaftState{1, kFollower, 0, {{1, 1}, {1, 2}, {1, 3}, {2, 4}, {3, 5}}},
          AppendEntriesReply{1, true, 6, 1},
      },
      TestCase{
          TestRaftState{1, kLeader, 0, {{1, 1}, {2, 2}, {3, 3}, {4, 4}}},
          AppendEntriesArgs{2, 2, 1, 1, 0, 2, 0, ConstructLogEntry({{3, 2}, {4, 3}})},
          TestRaftState{2, kFollower, 0, {{1, 1}, {3, 2}, {4, 3}}},
          AppendEntriesReply{2, true, 4, 1},
      },
  };
  RunAllTests(tests);
}

TEST_F(RaftAppendEntriesTest, TestSimplyAppendEntries) {
  // Test simply append log entries
  std::vector<TestCase> tests = {
      // case: receive server is at Term 1, with existed log entry (1, 1), (1, 2);
      // Input: Carries Log entry of (1, 3), (2, 4), (3, 5)
      // Expect Result: Accpect all log entries and has totally 5 log entries
      TestCase{
          TestRaftState{1, kFollower, 0, {{1, 1}, {1, 2}}},
          AppendEntriesArgs{1, 2, 2, 1, 0, 3, 0,
                            ConstructLogEntry({{1, 3}, {2, 4}, {3, 5}})},
          TestRaftState{1, kFollower, 0, {{1, 1}, {1, 2}, {1, 3}, {2, 4}, {3, 5}}},
          AppendEntriesReply{1, true, 6, 1},
      },
      // case: receive server is at Term 1, with existed log entry (1, 1), (1, 2);
      // Input: Carries Log entry of (1, 2), (2, 3), (3, 4)
      // Expect Result: Accpect all log entries and has totally 5 log entries
      TestCase{
          TestRaftState{1, kFollower, 0, {{1, 1}, {1, 2}}},
          AppendEntriesArgs{1, 2, 1, 1, 0, 3, 0,
                            ConstructLogEntry({{1, 2}, {2, 3}, {3, 4}})},
          TestRaftState{1, kFollower, 0, {{1, 1}, {1, 2}, {2, 3}, {3, 4}}},
          AppendEntriesReply{1, true, 5, 1},
      },
  };

  RunAllTests(tests);
}

TEST_F(RaftAppendEntriesTest, TestDetectMismatchPrevLogEntry) {
  // Test detect prev log entry mismatches
  std::vector<TestCase> tests = {
      // case: receive server is at Term1, with existed entries (1, 1), (1, 2);
      // Input: Carries three log entries, but request prev log entry is (2, 3);
      // Expect Result: refuse since there is no entry at (2, 3)
      TestCase{
          TestRaftState{1, kFollower, 0, {{1, 1}, {1, 2}}},
          AppendEntriesArgs{1, 2, 3, 2, 0, 3, 0,
                            ConstructLogEntry({{2, 4}, {2, 5}, {3, 6}})},
          TestRaftState{1, kFollower, 0, {{1, 1}, {1, 2}}},
          AppendEntriesReply{1, false, 1, 1},
      },
      // case: receive server is at Term1, with existed entries (1, 1), (1, 2);
      // Input: Carries three log entries, but request prev log entry is (2, 2);
      // Expect Result: refuse since prev log entry mismatches, index 2 has term 1
      TestCase{
          TestRaftState{1, kFollower, 0, {{1, 1}, {1, 2}}},
          AppendEntriesArgs{1, 2, 2, 2, 0, 3, 0,
                            ConstructLogEntry({{2, 3}, {2, 4}, {3, 5}})},
          TestRaftState{1, kFollower, 0, {{1, 1}, {1, 2}}},
          AppendEntriesReply{1, false, 1, 1},
      },
  };
  RunAllTests(tests);
}

TEST_F(RaftAppendEntriesTest, TestDetectConflictLogEntries) {
  // Test delete conflicting log entries
  std::vector<TestCase> tests = {
      // Case: receive server is at Term1, with existed entries (1, 1), (1, 2), (2, 3)
      // Input: Carries three log entries with varied term
      // Expect Result: (1, 2) and input (2, 2) conflicts, delete (1, 2) and (2, 3),
      // append (2, 2) to receive server's log
      TestCase{
          TestRaftState{1, kFollower, 0, {{1, 1}, {1, 2}, {2, 3}}},
          AppendEntriesArgs{1, 2, 1, 1, 0, 3, 0,
                            ConstructLogEntry({{2, 2}, {3, 3}, {4, 4}})},
          TestRaftState{1, kFollower, 0, {{1, 1}, {2, 2}, {3, 3}, {4, 4}}},
          AppendEntriesReply{1, true, 5, 1},
      },

      // Case: Receive server is at Term1, with existed log entries 1->5
      // Input: Carries log entries even less than receive server
      // Expect Result: (3, 3) and (2, 3) conlicts, receive server has to delete all
      // following logs even its log is newer than the leader
      TestCase{
          TestRaftState{1, kFollower, 0, {{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}}},
          AppendEntriesArgs{1, 2, 2, 2, 0, 2, 0, ConstructLogEntry({{2, 3}, {3, 4}})},
          TestRaftState{1, kFollower, 0, {{1, 1}, {2, 2}, {2, 3}, {3, 4}}},
          AppendEntriesReply{1, true, 5, 1},
      },
  };
  RunAllTests(tests);
}

TEST_F(RaftAppendEntriesTest, TestDropMessagesIfTermIsLower) {
  // Case: Omit input AppendEntriesArgs since its term is less than current
  std::vector<TestCase> tests = {
      TestCase{
          TestRaftState{4, kFollower, 0, {{1, 1}, {2, 2}, {3, 3}, {4, 4}}},
          AppendEntriesArgs{3, 2, 1, 1, 0, 3, 0, ConstructLogEntry({{3, 2}, {3, 3}})},
          TestRaftState{4, kFollower, 0, {{1, 1}, {2, 2}, {3, 3}, {4, 4}}},
          AppendEntriesReply{4, false, 0, 1},
      },
  };
  RunAllTests(tests);
}

TEST_F(RaftAppendEntriesTest, TestFollowerUpdateCommitIndex) {
  std::vector<TestCase> tests = {
      // Case: Receive server is at Term1, with commit index 0
      // Input: Carries no entries but with leader commit index = 2
      // Expect Result: commit index should be 2
      TestCase{
          TestRaftState{1, kFollower, 0, {{1, 1}, {2, 2}}},
          AppendEntriesArgs{1, 2, 2, 2, 2, 0, 0, ConstructLogEntry({})},
          TestRaftState{1, kFollower, 2, {{1, 1}, {2, 2}}},
          AppendEntriesReply{1, true, 3, 1},
      },
      // Case: Receive server is at Term1, with existed log entries (1, 1), (2, 2)
      // Input: Carries no entries but with leader commit index = 3
      // Expect Result: commit index should be 1, for the following reason:
      //  This AE args doesn't carry any entry, so the last match entry is (1, 1), the
      //  follower can only set commit index to be 1
      TestCase{
          TestRaftState{1, kFollower, 0, {{1, 1}, {2, 2}}},
          AppendEntriesArgs{1, 2, 1, 1, 3, 0, 0, ConstructLogEntry({})},
          TestRaftState{1, kFollower, 1, {{1, 1}, {2, 2}}},
          AppendEntriesReply{1, true, 2, 1},
      },

      // Case: Receive server is at Term1, with existed log entries (1, 1), (2, 2)
      // Input: Carries no entries but with leader commit index = 3
      // Expect Result: commit index should be 2, for the following reason:
      //  This AE args carries one entry, the receive server can update its logs to (1,
      //  1), (3, 2), so the commit index should be min(leader_commit, 2) = 2
      TestCase{
          TestRaftState{1, kFollower, 0, {{1, 1}, {2, 2}}},
          AppendEntriesArgs{1, 2, 1, 1, 3, 1, 0, ConstructLogEntry({{3, 2}})},
          TestRaftState{1, kFollower, 2, {{1, 1}, {3, 2}}},
          AppendEntriesReply{1, true, 3, 1},
      },
  };
  RunAllTests(tests);
}

TEST_F(RaftHandleAppendEntriesReplyTest, TestConvertToFollowerOnReceivingHigherTerm) {
  std::vector<TestCase> tests = {
      TestCase{
          TestRaftState{1, kLeader, 0, {}},
          AppendEntriesReply{2, false, 1, 2},
          TestRaftState{2, kFollower, 0, {}},
      },
  };
  RunAllTests(tests);
}

TEST_F(RaftHandleAppendEntriesReplyTest, TestUpdateCommitIndex) {
  std::vector<TestCase> tests = {
      TestCase{
          TestRaftState{1, kLeader, 0, {{1, 1}}},
          AppendEntriesReply{1, true, 2, 2},
          TestRaftState{1, kLeader, 1, {{1, 1}}},
      },
  };
  RunAllTests(tests);
}

}  // namespace raft
