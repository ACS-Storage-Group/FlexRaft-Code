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
  auto config = ConstructNodesConfig(3, false);
  LaunchAllServers(config);
  sleepMs(10);

  // Test propose a few entries
  EXPECT_TRUE(ProposeOneEntry(1));
  EXPECT_TRUE(ProposeOneEntry(2));
  EXPECT_TRUE(ProposeOneEntry(3));

  ClearTestContext(config);
}

TEST_F(RaftNodeBasicTest, DISABLED_TestFollowersCrash) {
  auto config = ConstructNodesConfig(5, false);
  LaunchAllServers(config);
  sleepMs(10);

  EXPECT_TRUE(ProposeOneEntry(1));
  EXPECT_TRUE(ProposeOneEntry(2));
  EXPECT_TRUE(ProposeOneEntry(3));

  auto leader = GetLeaderId();

  // Disconnect a follower
  Disconnect((leader + 1) % node_num_);

  EXPECT_TRUE(ProposeOneEntry(4));
  EXPECT_TRUE(ProposeOneEntry(5));
  EXPECT_TRUE(ProposeOneEntry(6));

  Disconnect((leader + 2) % node_num_);

  EXPECT_TRUE(ProposeOneEntry(7));
  EXPECT_TRUE(ProposeOneEntry(8));
  EXPECT_TRUE(ProposeOneEntry(9));

  // Bring failed followers back
  Reconnect((leader + 1) % node_num_);
  EXPECT_TRUE(ProposeOneEntry(10));

  Reconnect((leader + 2) % node_num_);
  EXPECT_TRUE(ProposeOneEntry(11));

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
  auto config = ConstructNodesConfig(3, false);
  LaunchAllServers(config);
  sleepMs(10);

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

TEST_F(RaftNodeBasicTest, TestRecoverFailedFollowerData) {
  auto config = ConstructNodesConfig(7, false);
  LaunchAllServers(config);
  sleepMs(1000);

  auto leader = GetLeaderId();
  Disconnect((leader + 1) % node_num_);

  // Propose and check the data
  EXPECT_TRUE(ProposeOneEntry(1));
  EXPECT_TRUE(ProposeOneEntry(2));
  EXPECT_TRUE(ProposeOneEntry(3));

  // Reconnect the failed follower, check if the data can be recoverred
  Reconnect((leader + 1) % node_num_);

  EXPECT_TRUE(ProposeOneEntry(4));
  EXPECT_TRUE(ProposeOneEntry(5));
  EXPECT_TRUE(ProposeOneEntry(6));
}

}  // namespace raft
