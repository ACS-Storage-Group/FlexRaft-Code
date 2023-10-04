#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "chunk.h"
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

  // The size should be divided by the chunk_cnt
  std::string ConstructValue(int chunk_cnt) {
    int sz = chunk_cnt * 2 - sizeof(int);
    std::string ret = "value-abcdefg-";
    ret.append(sz - ret.size(), '1');
    return ret;
  }

  void DoBatchWrite(KvClientPtr client, int l, int r, const std::string& key_prefix,
                    const std::string& value) {
    for (int i = l; i <= r; ++i) {
      auto key = key_prefix + std::to_string(i);
      ASSERT_EQ(client->Put(key, value).err, kOk);
    }
  }

  void DoBatchGetAndCheck(KvClientPtr client, int l, int r, const std::string& key_prefix,
                          const std::string& check_val) {
    std::string get_value;
    for (int i = l; i <= r; ++i) {
      ASSERT_EQ(client->Get(key_prefix + std::to_string(i), &get_value).err, kOk);
      ASSERT_EQ(check_val, get_value);
    }
  }

  auto ConstructKVClusterConfigs(uint32_t node_num) {
    KvClusterConfig ret;
    const std::string kLocalIP = "127.0.0.1";
    uint16_t port_num = 50000;
    for (uint32_t i = 0; i < node_num; ++i) {
      ret.insert_or_assign(i, KvServiceNodeConfig{i,
                                                  {kLocalIP, port_num++},
                                                  {kLocalIP, port_num++},
                                                  "",
                                                  "./testdb" + std::to_string(i)});
    }
    return ret;
  }

 public:
  KvServiceNode* nodes_[kMaxNodeNum];
  static const raft::raft_node_id_t kNoDetectLeader = -1;
  int node_num_;
};

TEST_F(KvClusterTest, DISABLED_TestSimplePutGetOperation) {
  auto cluster_config = ConstructKVClusterConfigs(7);
  LaunchKvServiceNodes(cluster_config);
  sleepMs(1000);

  const int K = 4;
  int chunk_cnt = raft::code_conversion::get_chunk_count(K);
  auto value = ConstructValue(chunk_cnt);
  const int kTestCnt = 1000;

  auto client = std::make_shared<KvServiceClient>(cluster_config, 0);
  DoBatchWrite(client, 1, kTestCnt, "key-", value);
  sleepMs(1000);
  DoBatchGetAndCheck(client, 1, kTestCnt, "key-", value);

  // Test one follower down
  auto leader = GetLeaderId();
  Disconnect((leader + 1) % node_num_);

  DoBatchWrite(client, kTestCnt, 2 * kTestCnt, "key-", value);
  sleepMs(1000);
  DoBatchGetAndCheck(client, kTestCnt, 2 * kTestCnt, "key-", value);

  ClearTestContext(cluster_config);
}

TEST_F(KvClusterTest, TestGetAfterLeaderDown) {
  auto cluster_config = ConstructKVClusterConfigs(7);
  LaunchKvServiceNodes(cluster_config);
  sleepMs(1000);

  const int K = 4;
  int chunk_cnt = raft::code_conversion::get_chunk_count(K);
  auto value = ConstructValue(chunk_cnt);
  const int kTestCnt = 1000;

  auto client = std::make_shared<KvServiceClient>(cluster_config, 0);
  DoBatchWrite(client, 1, kTestCnt, "key-", value);
  sleepMs(1000);

  // Make the leader down and propose a new leader
  auto leader = GetLeaderId();
  Disconnect(leader);
  sleepMs(1000);

  DoBatchGetAndCheck(client, 1, kTestCnt, "key-", value);

  ClearTestContext(cluster_config);
}

// TEST_F(KvClusterTest, TestGetAfterOldLeaderFail) {
//   auto cluster_config = KvClusterConfig{
//       {0, {0, {"127.0.0.1", 50000}, {"127.0.0.1", 50005}, "", "./testdb0"}},
//       {1, {1, {"127.0.0.1", 50001}, {"127.0.0.1", 50006}, "", "./testdb1"}},
//       {2, {2, {"127.0.0.1", 50002}, {"127.0.0.1", 50007}, "", "./testdb2"}},
//       {3, {3, {"127.0.0.1", 50003}, {"127.0.0.1", 50008}, "", "./testdb3"}},
//       {4, {4, {"127.0.0.1", 50004}, {"127.0.0.1", 50009}, "", "./testdb4"}},
//   };
//   LaunchKvServiceNodes(cluster_config);
//   sleepMs(1000);

//   auto client = std::make_shared<KvServiceClient>(cluster_config, 0);
//   int put_cnt = 10;

//   CheckBatchPut(client, "key", "value-abcdefg-", 1, put_cnt);
//   sleepMs(1000);
//   auto leader = GetLeaderId();
//   Disconnect(leader);

//   CheckBatchGet(client, "key", "value-abcdefg-", 1, put_cnt);

//   ClearTestContext(cluster_config);
// }

// TEST_F(KvClusterTest, DISABLED_TestGetAfterOldLeaderFailRepeatedly) {
//   auto cluster_config = KvClusterConfig{
//       {0, {0, {"127.0.0.1", 50000}, {"127.0.0.1", 50005}, "", "./testdb0"}},
//       {1, {1, {"127.0.0.1", 50001}, {"127.0.0.1", 50006}, "", "./testdb1"}},
//       {2, {2, {"127.0.0.1", 50002}, {"127.0.0.1", 50007}, "", "./testdb2"}},
//       {3, {3, {"127.0.0.1", 50003}, {"127.0.0.1", 50008}, "", "./testdb3"}},
//       {4, {4, {"127.0.0.1", 50004}, {"127.0.0.1", 50009}, "", "./testdb4"}},
//   };
//   LaunchKvServiceNodes(cluster_config);
//   sleepMs(1000);

//   auto client = std::make_shared<KvServiceClient>(cluster_config, 0);
//   int put_cnt = 1000;

//   CheckBatchPut(client, "key", "value-abcdefg-", 1, put_cnt);
//   sleepMs(1000);
//   auto leader = GetLeaderId();
//   Disconnect(leader);

//   // Repeatedly check these keys, the read needs no gathering and decoding
//   // for the read operations of the second round
//   CheckBatchGet(client, "key", "value-abcdefg-", 1, put_cnt);
//   CheckBatchGet(client, "key", "value-abcdefg-", 1, put_cnt);
//   CheckBatchGet(client, "key", "value-abcdefg-", 1, put_cnt);

//   ClearTestContext(cluster_config);
// }

// TEST_F(KvClusterTest, DISABLED_TestFollowerRejoiningAfterLeaderCommitingSomeNewEntries) {
//   auto cluster_config = KvClusterConfig{
//       {0, {0, {"127.0.0.1", 50000}, {"127.0.0.1", 50005}, "", "./testdb0"}},
//       {1, {1, {"127.0.0.1", 50001}, {"127.0.0.1", 50006}, "", "./testdb1"}},
//       {2, {2, {"127.0.0.1", 50002}, {"127.0.0.1", 50007}, "", "./testdb2"}},
//       {3, {3, {"127.0.0.1", 50003}, {"127.0.0.1", 50008}, "", "./testdb3"}},
//       {4, {4, {"127.0.0.1", 50004}, {"127.0.0.1", 50009}, "", "./testdb4"}},
//   };
//   LaunchKvServiceNodes(cluster_config);
//   sleepMs(1000);

//   const std::string key_prefix = "key";
//   const std::string value_prefix = "value-abcdefg-";

//   int put_cnt = 10;
//   auto client = std::make_shared<KvServiceClient>(cluster_config, 0);
//   CheckBatchPut(client, key_prefix, value_prefix, 1, put_cnt);

//   auto leader1 = GetLeaderId();
//   Disconnect((leader1 + 1) % node_num_);  // randomly disable a follower
//   LOG(raft::util::kRaft, "S%d disconnect", (leader1 + 1) % node_num_);

//   CheckBatchPut(client, key_prefix, value_prefix, put_cnt + 1, put_cnt * 2);
//   CheckBatchGet(client, key_prefix, value_prefix, 1, 2 * put_cnt);

//   Reconnect((leader1 + 1) % node_num_);
//   LOG(raft::util::kRaft, "S%d reconnect", (leader1 + 1) % node_num_);

//   CheckBatchPut(client, key_prefix, value_prefix, 2 * put_cnt + 1, 3 * put_cnt);

//   // Let the leader down and check put value
//   sleepMs(1000);
//   leader1 = GetLeaderId();
//   Disconnect(leader1);
//   LOG(raft::util::kRaft, "S%d disconnect", (leader1 + 1) % node_num_);

//   CheckBatchGet(client, key_prefix, value_prefix, 1, 3 * put_cnt);

//   ClearTestContext(cluster_config);
// }
}  // namespace kv
