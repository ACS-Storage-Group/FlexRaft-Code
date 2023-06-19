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

static bool inline RequestVoteReplyEqual(const RequestVoteReply &l,
                                         const RequestVoteReply &r) {
  return std::memcmp(&l, &r, sizeof(RequestVoteReply)) == 0;
}

static bool inline RequestVoteArgsEqual(const RequestVoteArgs &l,
                                        const RequestVoteArgs &r) {
  return std::memcmp(&l, &r, sizeof(RequestVoteArgs)) == 0;
}

static bool inline AppendEntriesArgsEqual(const AppendEntriesArgs &l,
                                          const AppendEntriesArgs &r) {
  // bool hdr_equal = std::memcmp(&l, &r, kAppendEntriesArgsHdrSize) == 0;
  bool hdr_equal = l.term == r.term & l.prev_log_index == r.prev_log_index &
                   l.prev_log_term == r.prev_log_term & l.leader_id == r.leader_id &
                   l.leader_commit == r.leader_commit & l.entry_cnt == r.entry_cnt;
  if (!hdr_equal || l.entries.size() != r.entries.size()) {
    return false;
  }

  for (decltype(l.entries.size()) i = 0; i < l.entries.size(); ++i) {
    if (!(l.entries[i] == r.entries[i])) {
      return false;
    }
  }
  return true;
}

struct CollectRaftState {
  RaftRole role;
  raft_term_t term;
  raft_node_id_t voteFor;
};

class RaftRequestVoteTest : public ::testing::Test {
 public:
  // A TestCase is a simulated test condition where you give some input to the
  // raft state instance and get a return value and change to the raft state
  // itself
  struct TestCase {
    // Input of RequestVoteArgs RPC
    RequestVoteArgs args;
    // Output of RequestVoteArgs RPC
    RequestVoteReply expect_reply;
    // The expect raft status after it processes the input
    CollectRaftState expect_stat;
  };

 public:
  // Running and check a single test case
  void CheckTestsRunning(RaftState *raft_state, const TestCase &test_case) {
    RequestVoteReply reply;
    raft_state->Process(const_cast<RequestVoteArgs *>(&test_case.args), &reply);
    ASSERT_TRUE(RequestVoteReplyEqual(test_case.expect_reply, reply));
    ASSERT_EQ(test_case.expect_stat.role, raft_state->Role());
    ASSERT_EQ(test_case.expect_stat.term, raft_state->CurrentTerm());
    ASSERT_EQ(test_case.expect_stat.voteFor, raft_state->VoteFor());
  }

  void RunAllTestCase(RaftState *raft_state, const std::vector<TestCase> &tests) {
    for (const auto &test : tests) {
      CheckTestsRunning(raft_state, test);
    }
  }
};

class RaftHandleRequestVoteReplyTest : public ::testing::Test {
 public:
  // A TestCase is a simulated test condition where you give some input to the
  // raft state instance and get a return value and change to the raft state
  // itself
  struct TestCase {
    RequestVoteReply reply;
    CollectRaftState expect_stat;
  };

 public:
  void CheckTestsRunning(RaftState *raft_state, const TestCase &test_case) {
    raft_state->Process(const_cast<RequestVoteReply *>(&test_case.reply));
    ASSERT_EQ(test_case.expect_stat.role, raft_state->Role());
    ASSERT_EQ(test_case.expect_stat.term, raft_state->CurrentTerm());
    ASSERT_EQ(test_case.expect_stat.voteFor, raft_state->VoteFor());
  }

  void RunAllTestCase(RaftState *raft_state, const std::vector<TestCase> &tests) {
    for (const auto &test : tests) {
      CheckTestsRunning(raft_state, test);
    }
  }
};

class RaftElectionTest : public ::testing::Test {
 public:
  // A RpcClient mock is used to simulate a raft peer sending rpc request. In this
  // test condition, we use a memory storage to record the sent request so that we
  // can check if the messages are ok
  class RpcClientMock : public rpc::RpcClient {
   public:
    using RequestVoteMsgChannel = std::vector<RequestVoteArgs>;
    using AppendEntriesMsgChannel = std::vector<AppendEntriesArgs>;
    RpcClientMock(RequestVoteMsgChannel *requestvote_channel,
                  AppendEntriesMsgChannel *appendentries_channel)
        : requestvote_channel_(requestvote_channel),
          appendentries_channel_(appendentries_channel) {}
    void Init() override {}
    void sendMessage(const RequestVoteArgs &args) override {
      // Record the message by simply push it into channel
      requestvote_channel_->push_back(args);
    }
    void sendMessage(const AppendEntriesArgs &args) override {
      appendentries_channel_->push_back(args);
    }

    void sendMessage(const RequestFragmentsArgs &args) override {}
    void setState(void *) override {}
    void stop() override {}
    void recover() override {}

   private:
    RequestVoteMsgChannel *requestvote_channel_;
    AppendEntriesMsgChannel *appendentries_channel_;
  };
};

// A mock class that inherits from MemPersister for providing more conveinient
// public interface to modify stored entries
class StorageMock : public MemStorage {
 public:
  void Append(raft_term_t raft_term, raft_index_t raft_index) {
    LogEntry ent;
    ent.SetTerm(raft_term);
    ent.SetIndex(raft_index);
    persisted_entries_.push_back(ent);
  }
};

struct Entry {
  raft_term_t raft_term;
  raft_index_t raft_index;
};

// Return a storage mock from a bunch of (term, index) pair
static StorageMock *BuildStorage(std::vector<Entry> entries) {
  auto ret = new StorageMock();
  for (const auto &ent : entries) {
    ret->Append(ent.raft_term, ent.raft_index);
  }
  return ret;
}

TEST_F(RaftRequestVoteTest, TestRequestVoteGrantWithNoLogEntry) {
  RaftConfig config = {1, {{1, nullptr}, {2, nullptr}, {3, nullptr}}, nullptr};
  auto raft = RaftState::NewRaftState(config);

  raft->SetCurrentTerm(1);
  raft->SetRole(kFollower);
  raft->SetVoteFor(RaftState::kNotVoted);

  std::vector<TestCase> tests = {
      TestCase{{1, 2, 0, 0}, {1, true, 1}, {kFollower, 1, 2}},
      TestCase{{1, 2, 1, 1}, {1, true, 1}, {kFollower, 1, 2}},
      TestCase{{2, 2, 0, 0}, {2, true, 1}, {kFollower, 2, 2}},
  };

  RunAllTestCase(raft, tests);

  delete raft;
}

TEST_F(RaftRequestVoteTest, TestRequestVoteWhenAlreadyVote) {
  const raft_node_id_t vote_for = 1;
  const raft_node_id_t req_node = 2;

  RaftConfig config = {1, {{1, nullptr}, {2, nullptr}, {3, nullptr}}, nullptr};
  auto raft = RaftState::NewRaftState(config);

  raft->SetCurrentTerm(1);
  raft->SetRole(kFollower);
  raft->SetVoteFor(vote_for);

  // case1 & case2: the raft peer directly refuses even though request
  // server has newer log, as the server already voted and the term is not
  // change
  //
  // case3: the raft peer must vote for the target server since the term
  // is changed, and the voteFor is reset to be NotVoted
  std::vector<TestCase> tests = {
      TestCase{{1, req_node, 0, 0}, {1, false, 1}, {kFollower, 1, vote_for}},
      TestCase{{1, req_node, 1, 1}, {1, false, 1}, {kFollower, 1, vote_for}},
      TestCase{{2, req_node, 1, 1}, {2, true, 1}, {kFollower, 2, req_node}},
  };

  RunAllTestCase(raft, tests);

  delete raft;
}

TEST_F(RaftRequestVoteTest, TestRequestVoteAcceptIfLogIsNewer) {
  const raft_node_id_t req_node = 2;

  auto storage = BuildStorage({{1, 1}, {1, 2}, {2, 3}});
  RaftConfig config = {1, {{1, nullptr}, {2, nullptr}, {3, nullptr}}, storage};
  auto raft = RaftState::NewRaftState(config);

  raft->SetCurrentTerm(2);
  raft->SetRole(kFollower);
  raft->SetVoteFor(RaftState::kNotVoted);

  // case1: last log term and last log index is the same as the raft peer
  // case2: last log term is higher
  // case3: both last log term and index is higher
  // case4: term is higher, but index is lower
  std::vector<TestCase> tests = {
      TestCase{{2, req_node, 3, 2}, {2, true, 1}, {kFollower, 2, req_node}},
      TestCase{{3, req_node, 3, 3}, {3, true, 1}, {kFollower, 3, req_node}},
      TestCase{{4, req_node, 5, 4}, {4, true, 1}, {kFollower, 4, req_node}},
      TestCase{{4, req_node, 1, 4}, {4, true, 1}, {kFollower, 4, req_node}},
  };

  RunAllTestCase(raft, tests);

  delete raft;
  delete storage;
}

TEST_F(RaftRequestVoteTest, TestRequestVoteRefuseIfLogIsOld) {
  const raft_node_id_t req_node = 2;

  auto storage = BuildStorage({{1, 1}, {1, 2}, {2, 3}});
  RaftConfig config = {1, {{1, nullptr}, {2, nullptr}, {3, nullptr}}, storage};
  auto raft = RaftState::NewRaftState(config);

  raft->SetCurrentTerm(2);
  raft->SetRole(kFollower);
  raft->SetVoteFor(RaftState::kNotVoted);

  // case1: last log term is lower than peer's term
  // case2: log term is the same, but last log index is lower
  std::vector<TestCase> tests = {
      TestCase{{2, req_node, 1, 1}, {2, false, 1}, {kFollower, 2, RaftState::kNotVoted}},
      TestCase{{2, req_node, 2, 2}, {2, false, 1}, {kFollower, 2, RaftState::kNotVoted}},
  };

  RunAllTestCase(raft, tests);

  delete raft;
  delete storage;
}

TEST_F(RaftHandleRequestVoteReplyTest, TestUpdateTermIfReplyTermIsHigher) {
  const raft_node_id_t req_node = 1;

  RaftConfig config = {1, {{1, nullptr}, {2, nullptr}, {3, nullptr}}, nullptr};
  auto raft = RaftState::NewRaftState(config);

  raft->SetCurrentTerm(1);
  raft->SetRole(kCandidate);
  raft->SetVoteFor(req_node);

  // Case1: Reply term is exactly the same as the server's, remaining the state
  // Case2: Reply term is higher, the server falls back to be follower and reset voteFor
  std::vector<TestCase> tests = {
      TestCase{{1, false}, {kCandidate, 1, req_node}},
      TestCase{{2, false}, {kFollower, 2, RaftState::kNotVoted}},
  };

  RunAllTestCase(raft, tests);

  delete raft;
}

// This test has been disabled because we doesn't specify rpc client pointer during
// construction, which results in segment fault when candidate becomes leader and tend
// to send heartbeat messages
TEST_F(RaftHandleRequestVoteReplyTest, DISABLED_TestConvertToBeLeaderIfWinMajorityVote) {
  const raft_node_id_t req_node = 1;

  RaftConfig config = {1, {{1, nullptr}, {2, nullptr}, {3, nullptr}}, nullptr};
  auto raft = RaftState::NewRaftState(config);

  raft->SetCurrentTerm(1);
  raft->SetRole(kCandidate);
  raft->SetVoteFor(req_node);
  raft->SetVoteCnt(1);

  // Case1: Receive a refuse vote, still remains candidate
  // Case2: Receive an accept vote, convert to be a leader
  // Case3: Receive an accept vote with a higer term, falls back to be follower
  std::vector<TestCase> tests = {
      // TestCase{{1, false}, {kCandidate, 1, req_node}},
      TestCase{{1, true}, {kLeader, 1, req_node}},
  };

  RunAllTestCase(raft, tests);

  delete raft;
}

TEST_F(RaftElectionTest, TestStartElectionIfTimeOut) {
  RpcClientMock::RequestVoteMsgChannel channel1;
  RpcClientMock::AppendEntriesMsgChannel channel2;
  RpcClientMock rpc2(&channel1, &channel2);
  RpcClientMock rpc3(&channel1, &channel2);
  RaftConfig config = {1, {{1, nullptr}, {2, &rpc2}, {3, &rpc3}}, nullptr, 150, 200};
  auto raft = RaftState::NewRaftState(config);
  raft->SetRole(kFollower);

  // Check initial status
  ASSERT_EQ(raft->CurrentTerm(), 0);

  // Wait for election timeout
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  raft->Tick();

  ASSERT_EQ(raft->Role(), kCandidate);
  ASSERT_EQ(raft->VoteFor(), 1);
  ASSERT_EQ(raft->CurrentTerm(), 1);

  // Check messages that have been sent
  ASSERT_EQ(channel1.size(), 2);
  auto args = RequestVoteArgs{1, 1, 0, 0};
  for (const auto &arg : channel1) {
    ASSERT_TRUE(RequestVoteArgsEqual(arg, args));
  }
}

TEST_F(RaftElectionTest, TestRestartElectionIfNotReceiveReply) {
  RpcClientMock::RequestVoteMsgChannel channel1;
  RpcClientMock::AppendEntriesMsgChannel channel2;
  RpcClientMock rpc2(&channel1, &channel2);
  RpcClientMock rpc3(&channel1, &channel2);
  RaftConfig config = {1, {{1, nullptr}, {2, &rpc2}, {3, &rpc3}}, nullptr, 150, 200};
  auto raft = RaftState::NewRaftState(config);
  raft->SetRole(kFollower);

  // Check initial status
  ASSERT_EQ(raft->CurrentTerm(), 0);

  // Wait for election timeout
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  raft->Tick();

  // The second round
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  raft->Tick();

  ASSERT_EQ(raft->Role(), kCandidate);
  ASSERT_EQ(raft->VoteFor(), 1);
  ASSERT_EQ(raft->CurrentTerm(), 2);

  ASSERT_EQ(channel1.size(), 4);
  auto args1 = RequestVoteArgs{1, 1, 0, 0};
  auto args2 = RequestVoteArgs{2, 1, 0, 0};

  for (decltype(channel1.size()) i = 0; i < 2; ++i) {
    ASSERT_TRUE(RequestVoteArgsEqual(channel1[i], args1));
  }
  for (decltype(channel1.size()) i = 2; i < 4; ++i) {
    ASSERT_TRUE(RequestVoteArgsEqual(channel1[i], args2));
  }
}

TEST_F(RaftElectionTest, TestBecomeLeaderIfWinMajorityReply) {
  RpcClientMock::RequestVoteMsgChannel channel1;
  RpcClientMock::AppendEntriesMsgChannel channel2;
  RpcClientMock rpc2(&channel1, &channel2);
  RpcClientMock rpc3(&channel1, &channel2);
  RaftConfig config = {1, {{1, nullptr}, {2, &rpc2}, {3, &rpc3}}, nullptr, 150, 200};
  auto raft = RaftState::NewRaftState(config);
  raft->SetRole(kFollower);

  // Check initial status
  ASSERT_EQ(raft->CurrentTerm(), 0);

  // Wait for election timeout
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  raft->Tick();

  ASSERT_EQ(raft->Role(), kCandidate);
  ASSERT_EQ(raft->VoteFor(), 1);
  ASSERT_EQ(raft->CurrentTerm(), 1);

  auto reply = RequestVoteReply{1, true};
  raft->Process(&reply);

  ASSERT_EQ(raft->Role(), kLeader);
  ASSERT_EQ(raft->CurrentTerm(), 1);

  // Check if heartbeat messages are sent correctly
  auto ref_appendentries_args =
      AppendEntriesArgs{1, 1, 0, 0, 0, 0, 0, std::vector<LogEntry>()};

  for (const auto &entry : channel2) {
    ASSERT_TRUE(AppendEntriesArgsEqual(ref_appendentries_args, entry));
  }
}

TEST_F(RaftElectionTest, TestConvertToFollowerIfFindHigherTerm) {
  RpcClientMock::RequestVoteMsgChannel channel1;
  RpcClientMock::AppendEntriesMsgChannel channel2;
  RpcClientMock rpc2(&channel1, &channel2);
  RpcClientMock rpc3(&channel1, &channel2);
  RaftConfig config = {1, {{1, nullptr}, {2, &rpc2}, {3, &rpc3}}, nullptr, 150, 200};
  auto raft = RaftState::NewRaftState(config);
  raft->SetRole(kFollower);

  // Check initial status
  ASSERT_EQ(raft->CurrentTerm(), 0);

  // Wait for election timeout
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  raft->Tick();

  ASSERT_EQ(raft->Role(), kCandidate);
  ASSERT_EQ(raft->VoteFor(), 1);
  ASSERT_EQ(raft->CurrentTerm(), 1);

  auto reply = RequestVoteReply{2, false};
  raft->Process(&reply);

  ASSERT_EQ(raft->Role(), kFollower);
  ASSERT_EQ(raft->CurrentTerm(), 2);
}

}  // namespace raft
