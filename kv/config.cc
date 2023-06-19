#include "config.h"

#include <unordered_map>
namespace kv {
std::unordered_map<raft::raft_node_id_t, raft::rpc::NetAddress>
ConstructRaftClusterConfig(const KvClusterConfig& config) {
  std::unordered_map<raft::raft_node_id_t, raft::rpc::NetAddress> ret;
  for (const auto& [id, ent] : config) {
    ret.insert({id, ent.raft_rpc_addr});
  }
  return ret;
}
};  // namespace kv
