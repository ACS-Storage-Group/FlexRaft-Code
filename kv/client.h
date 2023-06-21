#pragma once
#include <cstdint>
#include <thread>
#include <unordered_map>

#include "RCF/Future.hpp"
#include "config.h"
#include "kv_node.h"
#include "raft_type.h"
#include "rpc.h"
#include "type.h"
namespace kv {

struct OperationResults {
  ErrorType err;
  uint64_t apply_elapse_time;
  uint64_t commit_elapse_time;
};
class KvServiceClient {
  // If a KV Request is not done within 10 seconds
  static const int kKVRequestTimesoutCnt = 10;

 public:
  KvServiceClient(const KvClusterConfig &config, uint32_t client_id);
  ~KvServiceClient();

  struct GatherValueTask {
    std::string key;
    raft::raft_index_t read_index;
    raft::raft_node_id_t replied_id;
    raft::Encoder::EncodingResults *decode_input;
    int k, m;
  };

  struct GatherValueTaskResults {
    std::string *value;
    ErrorType err;
  };

 public:
  OperationResults Put(const std::string &key, const std::string &value);
  OperationResults Get(const std::string &, std::string *value);
  OperationResults Delete(const std::string &key);
  OperationResults Abort();

  void DoGatherValueTask(const GatherValueTask *task, GatherValueTaskResults *res);

  static void OnGetValueRpcComplete(RCF::Future<GetValueResponse> ret, KvServiceClient *client);

  raft::raft_node_id_t LeaderId() const { return curr_leader_; }

  struct DecodedString {
    int k, m;
    raft::raft_frag_id_t frag_id;
    raft::Slice frag;

    std::string ToString() const {
      char buf[256];
      sprintf(buf, "DecodedString{k=%d, m=%d, frag_id=%d}", k, m, frag_id);
      return std::string(buf);
    }
  };

  static DecodedString DecodeString(std::string *str) {
    auto bytes = str->c_str();

    int k = *reinterpret_cast<const int *>(bytes);
    bytes += sizeof(int);

    int m = *reinterpret_cast<const int *>(bytes);
    bytes += sizeof(int);

    auto frag_id = *reinterpret_cast<const raft::raft_frag_id_t *>(bytes);
    bytes += sizeof(raft::raft_frag_id_t);

    auto remaining_size = str->size() - sizeof(int) * 2 - sizeof(raft::raft_frag_id_t);
    return DecodedString{k, m, frag_id, raft::Slice(const_cast<char *>(bytes), remaining_size)};
  }

  uint32_t ClientId() const { return client_id_; }

 private:
  raft::raft_node_id_t DetectCurrentLeader();
  Response WaitUntilRequestDone(const Request &request);
  void sleepMs(int cnt) { std::this_thread::sleep_for(std::chrono::milliseconds(cnt)); }

 private:
  rpc::KvServerRPCClient *GetRPCStub(raft::raft_node_id_t id) { return servers_[id]; }

 private:
  std::unordered_map<raft::raft_node_id_t, rpc::KvServerRPCClient *> servers_;
  raft::raft_node_id_t curr_leader_;
  static const raft::raft_node_id_t kNoDetectLeader = -1;

  raft::raft_term_t curr_leader_term_;

  uint32_t client_id_;
};
}  // namespace kv
