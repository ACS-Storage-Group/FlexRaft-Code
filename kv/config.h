#pragma once
#include <unordered_map>

#include "raft_node.h"
#include "raft_type.h"
#include "type.h"
namespace kv {
struct KvServiceNodeConfig {
  raft::raft_node_id_t id;
  rpc::NetAddress kv_rpc_addr;
  raft::rpc::NetAddress raft_rpc_addr;
  std::string raft_log_filename;
  std::string kv_dbname;
};

using KvClusterConfig = std::unordered_map<raft::raft_node_id_t, KvServiceNodeConfig>;

std::unordered_map<raft::raft_node_id_t, raft::rpc::NetAddress>
ConstructRaftClusterConfig(const KvClusterConfig& config);

}  // namespace kv
