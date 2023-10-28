#pragma once

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "encoder.h"
#include "gtest/gtest.h"
#include "log_entry.h"
#include "raft.h"
#include "raft_node.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "rpc.h"
#include "rsm.h"
#include "storage.h"
#include "util.h"

#define CONFLICT_TERM -2

namespace raft {
// A bunch of basic test framework
class RaftNodeTest : public ::testing::Test {
 public:
  static constexpr int kMaxNodeNum = 9;
  static constexpr raft_node_id_t kNoLeader = -1;
  static constexpr int kCommandDataLength = 1024;

  // Some necessary Test structs
  using NetConfig = std::unordered_map<raft_node_id_t, rpc::NetAddress>;
  struct TestNodeConfig {
    rpc::NetAddress ip;
    std::string storage_name;
  };
  using NodesConfig = std::unordered_map<raft_node_id_t, TestNodeConfig>;

  // This is a simple simulated state machine, it assumes that each command is
  // simply an integer and the applier simply records it with associated log
  // index
  class RsmMock : public Rsm {
    // (index, term) uniquely identify an entry
    using CommitResult = std::pair<raft_term_t, int>;

   public:
    void ApplyLogEntry(LogEntry ent) override { applied_entries_.insert({ent.Index(), ent}); }

    // Return the applied entry at particular index, return true if there is
    // such an index, otherwise returns false
    bool getEntry(raft_index_t raft_index, LogEntry *ent) {
      if (applied_entries_.count(raft_index) == 0) {
        return false;
      }
      *ent = applied_entries_[raft_index];
      return true;
    }

   private:
    std::unordered_map<raft_index_t, LogEntry> applied_entries_;
  };

  static NodesConfig ConstructNodesConfig(int server_num, bool with_storage) {
    std::string default_ip = "127.0.0.1";
    uint16_t init_port = 50001;
    NodesConfig ret;
    for (uint16_t i = 0; i < server_num; ++i) {
      TestNodeConfig node_config;
      node_config.ip = {default_ip, static_cast<uint16_t>(init_port + i)};
      node_config.storage_name =
          with_storage ? std::string("test_storage") + std::to_string(i) : "";
      ret.insert({i, node_config});
    }
    return ret;
  }

  void LaunchAllServers(const NetConfig &net_config) {
    node_num_ = net_config.size();
    for (const auto &[id, _] : net_config) {
      LaunchRaftNodeInstance({id, net_config, "", new RsmMock});
    }
  }

  NetConfig GetNetConfigFromNodesConfig(const NodesConfig &nodes_config) {
    NetConfig net_config;
    for (const auto &[id, config] : nodes_config) {
      net_config.insert({id, config.ip});
    }
    return net_config;
  }

  void LaunchAllServers(const NodesConfig &nodes_config) {
    node_num_ = nodes_config.size();
    NetConfig net_config = GetNetConfigFromNodesConfig(nodes_config);
    for (const auto &[id, config] : nodes_config) {
      LaunchRaftNodeInstance({id, net_config, config.storage_name, new RsmMock});
    }
  }

  // Create a thread that holds this raft node, and start running this node
  // immediately returns the pointer to that raft node
  void LaunchRaftNodeInstance(const RaftNode::NodeConfig &config) {
    auto raft_node = new RaftNode(config);
    this->nodes_[config.node_id_me] = raft_node;
    raft_node->Init();
    auto node_thread = std::thread([=]() { raft_node->Start(); });
    node_thread.detach();
  };

  bool CheckNoLeader() {
    bool has_leader = false;
    std::for_each(nodes_, nodes_ + node_num_,
                  [&](RaftNode *node) { has_leader |= (node->getRaftState()->Role() == kLeader); });
    return has_leader == false;
  }

  CommandData ConstructCommandFromValue(int val) {
    auto data = new char[kCommandDataLength + 10];
    *reinterpret_cast<int *>(data) = val;
    // For more strict test on encoding/decoding
    *reinterpret_cast<int *>(data + kCommandDataLength - 4) = val;
    return CommandData{sizeof(int), Slice(data, kCommandDataLength)};
  }

  // Check that at every fixed term, there is and there is only one leader alive
  // NOTE: CheckOneLeader may fail under such condition:
  //   Say there are 3 servers, the old leader and one is alive, but another one
  //   is separate from network and becoming candidate with much higher term.
  bool CheckOneLeader() {
    const int retry_cnt = 10;
    std::unordered_map<raft_term_t, int> leader_cnt;
    auto record = [&](RaftNode *node) {
      if (!node->IsDisconnected()) {
        auto raft_state = node->getRaftState();
        leader_cnt[raft_state->CurrentTerm()] += (raft_state->Role() == kLeader);
      }
    };
    for (int i = 0; i < retry_cnt; ++i) {
      leader_cnt.clear();
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      std::for_each(nodes_, nodes_ + node_num_, record);

      raft_term_t lastTerm = 0;
      for (const auto &[term, cnt] : leader_cnt) {
        if (cnt > 2) {
          std::printf("Term %d has more than one leader\n", term);
          return false;
        }
        lastTerm = std::max(lastTerm, term);
      }
      if (lastTerm > 0 && leader_cnt[lastTerm] == 1) {
        return true;
      }
    }
    return false;
  }

  // Where is a majority servers alive in cluster, it's feasible to propose one
  // entry and reach an agreement among these servers
  bool ProposeOneEntry(int value) {
    const int retry_cnt = 20;

    for (int run = 0; run < retry_cnt; ++run) {
      raft_node_id_t leader_id = kNoLeader;
      ProposeResult propose_result;
      auto cmd = ConstructCommandFromValue(value);
      for (int i = 0; i < node_num_; ++i) {  // Search for a leader
        if (!Alive(i)) {
          continue;
        }
        propose_result = nodes_[i]->getRaftState()->Propose(cmd);
        if (propose_result.is_leader) {
          leader_id = i;
          break;
        }
      }

      // This value has been proposed by a leader, however, we can not gurantee
      // it will be committed
      if (leader_id != kNoLeader) {
        // Wait this entry to be committed
        assert(propose_result.propose_index > 0);

        // Retry 10 times
        for (int run2 = 0; run2 < 10; ++run2) {
          bool succ = checkCommitted(propose_result, value);
          if (succ) {
            LOG(util::kRaft, "[SUCC] Check Value Done: %d", value);
            return true;
          } else {
            sleepMs(20);
          }
          // if (val == CONFLICT_TERM) {
          //   break;
          // } else if (val == -1) {
          // } else {
          //   LOG(util::kRaft, "Check Propose value: Expect %d Get %d", value,
          //   val); return val == value;
          // }
        }
      }
      LOG(util::kRaft, "[FAILED] Wait a propose entry to be committed");

      // Sleep for 50ms so that the entry will be committed
      sleepMs(500);
    }
    return false;
  }

  void sleepMs(int num) { std::this_thread::sleep_for(std::chrono::milliseconds(num)); }

  // Check if propose_val has been committed and applied, return true if
  // committed and applied, i.e. we can read the applied fragments from
  // corresponding state machine
  bool checkCommitted(const ProposeResult &propose_result, int propose_val) {
    Stripe stripe;
    int alive_number = 0;
    raft_encoding_param_t decode_k = 1000;
    Encoder::EncodingResults collected_res;
    for (int i = 0; i < node_num_; ++i) {
      if (Alive(i)) {
        alive_number += 1;
        auto rsm = reinterpret_cast<RsmMock *>(nodes_[i]->getRsm());
        auto raft = nodes_[i]->getRaftState();
        if (raft->CommitIndex() >= propose_result.propose_index) {
          LogEntry ent;
          auto stat = rsm->getEntry(propose_result.propose_index, &ent);
          if (ent.Term() != propose_result.propose_term) {
            continue;
          }
          if (ent.Type() == kFragments) {
            // Note that i is identical to the fragment id
            collected_res.insert_or_assign(static_cast<raft_frag_id_t>(i), ent.FragmentSlice());
            decode_k = std::min(decode_k, ent.GetChunkInfo().GetK());
            LOG(util::kRaft, "Update DecodeK=%d", decode_k);
          }
        }
      }
    }

    // Correct encoding parameter k and m
    int k = decode_k;
    int m = node_num_ - k;
    if (collected_res.size() == 0) {
      return false;
    }

    // Dump all contents of fragments
    for (const auto &[id, frag] : collected_res) {
      LOG(util::kRaft, "Fragment Id%d Len=%d LastData=%d", id, frag.size(),
          *(int *)(frag.data() + (frag.size() - 4)));
    }

    Encoder encoder;
    Slice res;
    LOG(util::kRaft, "Check Committed K=%d M=%d", k, m);
    // Failed decoding: may due to insufficient fragments
    auto decode_stat = encoder.DecodeSlice(collected_res, k, m, &res);
    if (!decode_stat) {
      LOG(util::kRaft, "[FAILED] Decode Slice Fail");
      return false;
    }
    LOG(util::kRaft, "Decode Results: size=%d Tail Value=%d", res.size(),
        *(int *)(res.data() + res.size() - 4));
    auto val_tail = *reinterpret_cast<int *>(res.data() + kCommandDataLength - 8);
    EXPECT_EQ(propose_val, val_tail);
    if (propose_val != val_tail) {
      return false;
    }
    return true;
  }

  int LivenessLevel() const { return node_num_ / 2; }

  bool Alive(int i) {
    return nodes_[i] != nullptr && !nodes_[i]->Exited() && !nodes_[i]->IsDisconnected();
  }

  // Find current leader and returns its associated node id, if there is
  // multiple leader, for example, due to network partition, returns the
  // smallest one
  raft_node_id_t GetLeaderId() {
    for (int i = 0; i < node_num_; ++i) {
      if (Alive(i) && nodes_[i]->getRaftState()->Role() == kLeader) {
        return i;
      }
    }
    return kNoLeader;
  }

  // TODO: Use pause to replace shut down. A paused node should not respond to
  // any RPC call and not makes progress, as if the server is paused at some
  // point void ShutDown(raft_node_id_t id) {
  //   if (!nodes_[id]->Exited()) {
  //     nodes_[id]->Exit();
  //   }
  // }

  void Disconnect(raft_node_id_t id) {
    // std::cout << "Disconnect " << id << std::endl;
    if (!nodes_[id]->IsDisconnected()) {
      nodes_[id]->Disconnect();
    }
    LOG(util::kRaft, "------ Disconnect S%d ------", id);
  }

  void Reconnect(raft_node_id_t id) {
    LOG(util::kRaft, "------ Reconnect S%d ------", id);
    // std::cout << "Reconnect " << id << std::endl;
    if (nodes_[id]->IsDisconnected()) {
      nodes_[id]->Reconnect();
    }
  }

  // Calling end will exits all existed raft node thread, and clear all
  // allocated resources. This should be only called when a test is done
  void TestEnd() {
    std::for_each(nodes_, nodes_ + node_num_, [](RaftNode *node) {
      if (!node->Exited()) {
        node->Exit();
      }
    });
    std::for_each(nodes_, nodes_ + node_num_, [](RaftNode *node) { delete node; });
  }

  void ClearTestContext(const NodesConfig &nodes_config) {
    auto cmp = [](RaftNode *node) {
      if (!node->Exited()) {
        // Mark the node has been exited
        node->Exit();
      }
    };
    std::for_each(nodes_, nodes_ + node_num_, cmp);
    // Make sure all ticker threads have exited
    std::this_thread::sleep_for(std::chrono::milliseconds(config::kRaftTickBaseInterval * 2));
    for (int i = 0; i < node_num_; ++i) {
      delete nodes_[i];
    }

    // Clear created log files
    for (const auto &[_, config] : nodes_config) {
      if (config.storage_name != "") {
        // std::filesystem::remove(config.storage_name);
        remove(config.storage_name.c_str());
      }
    }
  }

 public:
  // Record each nodes and all nodes number
  RaftNode *nodes_[kMaxNodeNum];
  int node_num_;
};
}  // namespace raft
