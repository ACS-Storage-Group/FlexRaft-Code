#include <gtest/gtest.h>

#include <filesystem>
#include <unordered_map>

#include "RCF/InitDeinit.hpp"
#include "raft_node_test.h"
#include "raft_type.h"
namespace raft {
class RaftNodeCrashTest : public RaftNodeTest {
 public:
  void Crash(raft_node_id_t id) {
    if (!nodes_[id]->Exited()) {
      nodes_[id]->Exit();
    }
    delete nodes_[id];
    nodes_[id] = nullptr;
  }

  void Reboot(const NodesConfig& config, raft_node_id_t id) {
    auto net_config = GetNetConfigFromNodesConfig(config);
    auto node_config = config.at(id);
    LaunchRaftNodeInstance({id, net_config, node_config.storage_name, new RsmMock});
  }

  void Restart(const NodesConfig& config, raft_node_id_t id) {
    if (nodes_[id] != nullptr) {
      Crash(id);
    }
    Reboot(config, id);
  }

  void PreRemoveFiles(const NodesConfig& config) {
    for (const auto& [_, config] : config) {
      // std::filesystem::remove(config.storage_name);
      remove(config.storage_name.c_str());
    }
  }
};

TEST_F(RaftNodeCrashTest, TestSimpleRestartLeader) {
  auto config = ConstructNodesConfig(3, true);
  PreRemoveFiles(config);

  LaunchAllServers(config);
  sleepMs(10);

  EXPECT_TRUE(ProposeOneEntry(101));
  auto leader = GetLeaderId();

  Disconnect((leader + 2) % node_num_);

  EXPECT_TRUE(ProposeOneEntry(102));

  Crash((leader + 0) % node_num_);
  Crash((leader + 1) % node_num_);

  Reconnect((leader + 2) % node_num_);
  // Can not commit one entry since there is no majority
  // EXPECT_FALSE(ProposeOneEntry(103));

  // Able to commit one entry since majority comes back
  Reboot(config, (leader + 0) % node_num_);
  EXPECT_TRUE(ProposeOneEntry(103));

  Reboot(config, (leader + 1) % node_num_);
  EXPECT_TRUE(ProposeOneEntry(104));

  ClearTestContext(config);
}

TEST_F(RaftNodeCrashTest, TestSomeCrashOccurs) {
  auto config = ConstructNodesConfig(3, true);
  PreRemoveFiles(config);

  LaunchAllServers(config);
  sleepMs(10);

  EXPECT_TRUE(ProposeOneEntry(101));

  for (int i = 0; i < node_num_; ++i) {
    Restart(config, i);
  }

  EXPECT_TRUE(ProposeOneEntry(102));

  auto leader1 = GetLeaderId();
  Disconnect(leader1);
  Restart(config, leader1);

  EXPECT_TRUE(ProposeOneEntry(103));

  auto leader2 = GetLeaderId();
  Disconnect(leader2);
  EXPECT_TRUE(ProposeOneEntry(104));
  Restart(config, leader2);

  sleepMs(1000);

  auto i3 = (GetLeaderId() + 1) % node_num_;
  Disconnect(i3);
  EXPECT_TRUE(ProposeOneEntry(105));
  Restart(config, i3);

  EXPECT_TRUE(ProposeOneEntry(106));

  ClearTestContext(config);
}

TEST_F(RaftNodeCrashTest, TestManyCrashOccurs) {
  auto config = ConstructNodesConfig(5, true);
  PreRemoveFiles(config);

  LaunchAllServers(config);
  sleepMs(10);

  const int iter_cnt = 5;
  int init_val = 1;
  for (int iter = 0; iter < iter_cnt; ++iter) {
    EXPECT_TRUE(ProposeOneEntry(init_val));
    ++init_val;

    auto leader1 = GetLeaderId();

    Disconnect((leader1 + 1) % node_num_);
    Disconnect((leader1 + 2) % node_num_);

    EXPECT_TRUE(ProposeOneEntry(init_val));
    ++init_val;

    Disconnect((leader1 + 3) % node_num_);
    Disconnect((leader1 + 4) % node_num_);
    Disconnect((leader1 + 0) % node_num_);

    Restart(config, (leader1 + 1) % node_num_);
    Restart(config, (leader1 + 2) % node_num_);
    Restart(config, (leader1 + 3) % node_num_);

    EXPECT_TRUE(ProposeOneEntry(init_val));
    ++init_val;

    Reconnect((leader1 + 0) % node_num_);
    Reconnect((leader1 + 4) % node_num_);
  }
  ClearTestContext(config);
}
}  // namespace raft
