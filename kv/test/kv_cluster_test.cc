#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "client.h"
#include "config.h"
#include "gtest/gtest.h"
#include "kv_node.h"
#include "raft_type.h"
#include "type.h"
#include "util.h"
namespace kv {
// This test should be running on a multi-core machine, so that each thread can run
// smoothly without interpretation
class KvClusterTest : public ::testing::Test {
  static constexpr int kMaxNodeNum = 10;

 public:
  using KvClientPtr = std::shared_ptr<KvServiceClient>;

 public:
  void LaunchKvServiceNodes(const KvClusterConfig& config) {
    node_num_ = config.size();
    for (const auto& [id, conf] : config) {
      auto node = KvServiceNode::NewKvServiceNode(config, id);
      node->InitServiceNodeState();
      nodes_[id] = node;
    }

    for (int i = 0; i < node_num_; ++i) {
      nodes_[i]->StartServiceNode();
    }
  }

  void ClearTestContext(const KvClusterConfig& config) {
    for (int i = 0; i < node_num_; ++i) {
      nodes_[i]->StopServiceNode();
    }

    // Sleep enough time to make sure the ticker threads successfully exit
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    for (int i = 0; i < node_num_; ++i) {
      delete nodes_[i];
    }

    for (const auto& [id, conf] : config) {
      if (conf.raft_log_filename != "") {
        std::filesystem::remove(conf.raft_log_filename);
      }
      if (conf.kv_dbname != "") {
        std::filesystem::remove_all(conf.kv_dbname);
      }
    }
  }

  void sleepMs(int cnt) { std::this_thread::sleep_for(std::chrono::milliseconds(cnt)); }

  raft::raft_node_id_t GetLeaderId() {
    for (int i = 0; i < node_num_; ++i) {
      if (!nodes_[i]->IsDisconnected() && nodes_[i]->IsLeader()) {
        return i;
      }
    }
    return kNoDetectLeader;
  }

  void Disconnect(int i) {
    if (!nodes_[i]->IsDisconnected()) {
      nodes_[i]->Disconnect();
    }
  }

  void Reconnect(int i) {
    if (nodes_[i]->IsDisconnected()) {
      nodes_[i]->Reconnect();
    }
  }

  void CheckBatchPut(KvClientPtr client, const std::string& key_prefix,
                     const std::string& value_prefix, int key_lo, int key_hi) {
    for (int i = key_lo; i <= key_hi; ++i) {
      auto key = key_prefix + std::to_string(i);
      auto value = value_prefix + std::to_string(i);
      EXPECT_EQ(client->Put(key, value).err, kOk);
    }
  }

  void CheckBatchGet(KvClientPtr client, const std::string& key_prefix,
                     const std::string expect_val_prefix, int key_lo, int key_hi) {
    std::string get_val;
    for (int i = key_lo; i <= key_hi; ++i) {
      auto key = key_prefix + std::to_string(i);
      auto expect_value = expect_val_prefix + std::to_string(i);
      EXPECT_EQ(client->Get(key, &get_val).err, kOk);
      ASSERT_EQ(get_val, expect_value);
    }
  }

 public:
  KvServiceNode* nodes_[kMaxNodeNum];
  static const raft::raft_node_id_t kNoDetectLeader = -1;
  int node_num_;
};

TEST_F(KvClusterTest, DISABLED_TestSimplePutGetOperation) {
  auto cluster_config = KvClusterConfig{
      {0, {0, {"127.0.0.1", 50000}, {"127.0.0.1", 50003}, "", "./testdb0"}},
      {1, {1, {"127.0.0.1", 50001}, {"127.0.0.1", 50004}, "", "./testdb1"}},
      {2, {2, {"127.0.0.1", 50002}, {"127.0.0.1", 50005}, "", "./testdb2"}},
  };
  LaunchKvServiceNodes(cluster_config);
  sleepMs(1000);

  auto client = std::make_shared<KvServiceClient>(cluster_config, 0);
  int put_cnt = 10;
  CheckBatchPut(client, "key", "value-abcdefg-", 1, put_cnt);
  CheckBatchGet(client, "key", "value-abcdefg-", 1, put_cnt);
  ClearTestContext(cluster_config);
}

TEST_F(KvClusterTest, TestGetAfterOldLeaderFail) {
  auto cluster_config = KvClusterConfig{
      {0, {0, {"127.0.0.1", 50000}, {"127.0.0.1", 50005}, "", "./testdb0"}},
      {1, {1, {"127.0.0.1", 50001}, {"127.0.0.1", 50006}, "", "./testdb1"}},
      {2, {2, {"127.0.0.1", 50002}, {"127.0.0.1", 50007}, "", "./testdb2"}},
      {3, {3, {"127.0.0.1", 50003}, {"127.0.0.1", 50008}, "", "./testdb3"}},
      {4, {4, {"127.0.0.1", 50004}, {"127.0.0.1", 50009}, "", "./testdb4"}},
  };
  LaunchKvServiceNodes(cluster_config);
  sleepMs(1000);

  auto client = std::make_shared<KvServiceClient>(cluster_config, 0);
  int put_cnt = 10;

  CheckBatchPut(client, "key", "value-abcdefg-", 1, put_cnt);
  sleepMs(1000);
  auto leader = GetLeaderId();
  Disconnect(leader);

  CheckBatchGet(client, "key", "value-abcdefg-", 1, put_cnt);

  ClearTestContext(cluster_config);
}

TEST_F(KvClusterTest, DISABLED_TestGetAfterOldLeaderFailRepeatedly) {
  auto cluster_config = KvClusterConfig{
      {0, {0, {"127.0.0.1", 50000}, {"127.0.0.1", 50005}, "", "./testdb0"}},
      {1, {1, {"127.0.0.1", 50001}, {"127.0.0.1", 50006}, "", "./testdb1"}},
      {2, {2, {"127.0.0.1", 50002}, {"127.0.0.1", 50007}, "", "./testdb2"}},
      {3, {3, {"127.0.0.1", 50003}, {"127.0.0.1", 50008}, "", "./testdb3"}},
      {4, {4, {"127.0.0.1", 50004}, {"127.0.0.1", 50009}, "", "./testdb4"}},
  };
  LaunchKvServiceNodes(cluster_config);
  sleepMs(1000);

  auto client = std::make_shared<KvServiceClient>(cluster_config, 0);
  int put_cnt = 1000;

  CheckBatchPut(client, "key", "value-abcdefg-", 1, put_cnt);
  sleepMs(1000);
  auto leader = GetLeaderId();
  Disconnect(leader);

  // Repeatedly check these keys, the read needs no gathering and decoding
  // for the read operations of the second round
  CheckBatchGet(client, "key", "value-abcdefg-", 1, put_cnt);
  CheckBatchGet(client, "key", "value-abcdefg-", 1, put_cnt);
  CheckBatchGet(client, "key", "value-abcdefg-", 1, put_cnt);

  ClearTestContext(cluster_config);
}

TEST_F(KvClusterTest, DISABLED_TestFollowerRejoiningAfterLeaderCommitingSomeNewEntries) {
  auto cluster_config = KvClusterConfig{
      {0, {0, {"127.0.0.1", 50000}, {"127.0.0.1", 50005}, "", "./testdb0"}},
      {1, {1, {"127.0.0.1", 50001}, {"127.0.0.1", 50006}, "", "./testdb1"}},
      {2, {2, {"127.0.0.1", 50002}, {"127.0.0.1", 50007}, "", "./testdb2"}},
      {3, {3, {"127.0.0.1", 50003}, {"127.0.0.1", 50008}, "", "./testdb3"}},
      {4, {4, {"127.0.0.1", 50004}, {"127.0.0.1", 50009}, "", "./testdb4"}},
  };
  LaunchKvServiceNodes(cluster_config);
  sleepMs(1000);

  const std::string key_prefix = "key";
  const std::string value_prefix = "value-abcdefg-";

  int put_cnt = 10;
  auto client = std::make_shared<KvServiceClient>(cluster_config, 0);
  CheckBatchPut(client, key_prefix, value_prefix, 1, put_cnt);

  auto leader1 = GetLeaderId();
  Disconnect((leader1 + 1) % node_num_);  // randomly disable a follower
  LOG(raft::util::kRaft, "S%d disconnect", (leader1 + 1) % node_num_);

  CheckBatchPut(client, key_prefix, value_prefix, put_cnt + 1, put_cnt * 2);
  CheckBatchGet(client, key_prefix, value_prefix, 1, 2 * put_cnt);

  Reconnect((leader1 + 1) % node_num_);
  LOG(raft::util::kRaft, "S%d reconnect", (leader1 + 1) % node_num_);

  CheckBatchPut(client, key_prefix, value_prefix, 2 * put_cnt + 1, 3 * put_cnt);

  // Let the leader down and check put value
  sleepMs(1000);
  leader1 = GetLeaderId();
  Disconnect(leader1);
  LOG(raft::util::kRaft, "S%d disconnect", (leader1 + 1) % node_num_);

  CheckBatchGet(client, key_prefix, value_prefix, 1, 3 * put_cnt);

  ClearTestContext(cluster_config);
}
}  // namespace kv
