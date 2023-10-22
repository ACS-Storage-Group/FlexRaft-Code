#include "raft_node_test.h"

#include <string>

#include "util.h"

namespace raft {
class RaftNodeBasicTest : public RaftNodeTest {
 public:
  static NetConfig ConstructNetConfig(int server_num) {
    std::string default_ip = "127.0.0.1";
    uint16_t init_port = 50001;
    NetConfig ret;
    for (uint16_t i = 0; i < server_num; ++i) {
      ret.insert({i, {default_ip, static_cast<uint16_t>(init_port + i)}});
    }
    return ret;
  }
};

TEST_F(RaftNodeBasicTest, DISABLED_TestSimplyProposeEntry) {
  auto config = ConstructNodesConfig(5, false);
  LaunchAllServers(config);
  sleepMs(10);

  // Test propose a few entries
  EXPECT_TRUE(ProposeOneEntry(1, true));
  EXPECT_TRUE(ProposeOneEntry(2, true));
  EXPECT_TRUE(ProposeOneEntry(3, true));

  ClearTestContext(config);
}

// Test one follower fail
TEST_F(RaftNodeBasicTest, DISABLED_TestOneFollowerCrash) {
  auto config = ConstructNodesConfig(7, false);
  LaunchAllServers(config);
  sleepMs(10);

  // Disconnect a follower
  auto leader = GetLeaderId();
  Disconnect((leader + 1) % node_num_);

  // Propose and check data
  EXPECT_TRUE(ProposeOneEntry(1, true));
  EXPECT_TRUE(ProposeOneEntry(2, true));
  EXPECT_TRUE(ProposeOneEntry(3, true));

  // Randomly disable another two follows:
  Disconnect((leader + 2) % node_num_);
  Disconnect((leader + 3) % node_num_);
  EXPECT_TRUE(checkCommittedCodeConversion({1, 1, true}, 1));
  EXPECT_TRUE(checkCommittedCodeConversion({2, 1, true}, 2));
  EXPECT_TRUE(checkCommittedCodeConversion({3, 1, true}, 3));

  ClearTestContext(config);
}

TEST_F(RaftNodeBasicTest, DISABLED_TestTwoFollowersCrash) {
  auto config = ConstructNodesConfig(7, false);
  LaunchAllServers(config);
  sleepMs(10);

  // Disconnect a follower
  auto leader = GetLeaderId();
  Disconnect((leader + 1) % node_num_);
  Disconnect((leader + 2) % node_num_);

  // Propose and check data
  EXPECT_TRUE(ProposeOneEntry(1, true));
  EXPECT_TRUE(ProposeOneEntry(2, true));
  EXPECT_TRUE(ProposeOneEntry(3, true));

  // Randomly disable another two follows:
  Disconnect((leader + 3) % node_num_);
  EXPECT_TRUE(checkCommittedCodeConversion({1, 1, true}, 1));
  EXPECT_TRUE(checkCommittedCodeConversion({2, 1, true}, 2));
  EXPECT_TRUE(checkCommittedCodeConversion({3, 1, true}, 3));

  ClearTestContext(config);
}

TEST_F(RaftNodeBasicTest, DISABLED_TestLeaderCrash) {
  auto config = ConstructNodesConfig(5, false);
  LaunchAllServers(config);
  sleepMs(10);

  EXPECT_TRUE(ProposeOneEntry(1));
  EXPECT_TRUE(ProposeOneEntry(2));
  EXPECT_TRUE(ProposeOneEntry(3));

  auto leader1 = GetLeaderId();

  // Disconnect leader
  LOG(util::kRaft, "----- S%d Disconnect -----", leader1);
  Disconnect(leader1);

  // The cluster should continue to run despite one server down
  EXPECT_TRUE(ProposeOneEntry(4));
  EXPECT_TRUE(ProposeOneEntry(5));
  EXPECT_TRUE(ProposeOneEntry(6));

  ClearTestContext(config);
}

TEST_F(RaftNodeBasicTest, DISABLED_TestNewLeaderGetFullLogEntry) {
  auto config = ConstructNodesConfig(7, false);
  LaunchAllServers(config);
  sleepMs(10);

  EXPECT_TRUE(ProposeOneEntry(1, true));
  EXPECT_TRUE(ProposeOneEntry(2, true));
  EXPECT_TRUE(ProposeOneEntry(3, true));

  auto leader1 = GetLeaderId();

  auto cmd1 = ConstructCommandFromValue(4);
  auto cmd2 = ConstructCommandFromValue(5);

  auto pr1 = nodes_[leader1]->Propose(cmd1);
  EXPECT_TRUE(pr1.is_leader);

  auto pr2 = nodes_[leader1]->Propose(cmd2);
  EXPECT_TRUE(pr2.is_leader);

  // Disconnect old leader before this entry is committed
  Disconnect(leader1);

  sleepMs(500);

  // Must commit another entry
  EXPECT_TRUE(ProposeOneEntry(5, true));
  EXPECT_TRUE(checkCommittedCodeConversion(pr1, 4));
  EXPECT_TRUE(checkCommittedCodeConversion(pr2, 5));

  ClearTestContext(config);
}

TEST_F(RaftNodeBasicTest, DISABLED_TestNewLeaderGatheringFullLogEntry) {
  auto config = ConstructNodesConfig(7, false);
  LaunchAllServers(config);
  sleepMs(10);

  // Shutdown one server from the original
  Disconnect(0);

  EXPECT_TRUE(ProposeOneEntry(1));
  EXPECT_TRUE(ProposeOneEntry(2));
  EXPECT_TRUE(ProposeOneEntry(3));

  auto leader1 = GetLeaderId();

  auto cmd1 = ConstructCommandFromValue(4);
  auto cmd2 = ConstructCommandFromValue(5);

  auto pr1 = nodes_[leader1]->Propose(cmd1);
  EXPECT_TRUE(pr1.is_leader);

  auto pr2 = nodes_[leader1]->Propose(cmd2);
  EXPECT_TRUE(pr2.is_leader);

  // Disconnect old leader before this entry is committed
  Disconnect(leader1);

  sleepMs(500);

  // Must commit another entry
  EXPECT_TRUE(ProposeOneEntry(5));
  EXPECT_TRUE(checkCommitted(pr1, 4));
  EXPECT_TRUE(checkCommitted(pr2, 5));

  ClearTestContext(config);
}
TEST_F(RaftNodeBasicTest, TestRecoverChunksForNewServers) {
  auto config = ConstructNodesConfig(7, false);
  LaunchAllServers(config);
  sleepMs(1000);

  // Disconnect a follower
  auto leader = GetLeaderId();
  Disconnect((leader + 1) % node_num_);

  // Propose and check data
  EXPECT_TRUE(ProposeOneEntry(1, true));
  EXPECT_TRUE(ProposeOneEntry(2, true));
  EXPECT_TRUE(ProposeOneEntry(3, true));

  // Reconnect the node, check if the leader can recover the entries
  Reconnect((leader + 1) % node_num_);
  sleepMs(1000);  // Wait 1 second to recover the failed server

  EXPECT_TRUE(ProposeOneEntry(4, true));
  EXPECT_TRUE(ProposeOneEntry(5, true));
  EXPECT_TRUE(ProposeOneEntry(6, true));

  ClearTestContext(config);
}

}  // namespace raft
