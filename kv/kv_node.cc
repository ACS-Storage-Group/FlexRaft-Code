#include "kv_node.h"
#include "kv_server.h"
namespace kv {
KvServiceNode* KvServiceNode::NewKvServiceNode(const KvClusterConfig& config,
                                               raft::raft_node_id_t id) {
  auto ret = new KvServiceNode(config, id);
  return ret;
}

KvServiceNode::KvServiceNode(const KvClusterConfig& config, raft::raft_node_id_t id)
    : config_(config), id_(id) {
  auto raft_cluster_config = ConstructRaftClusterConfig(config);
  auto kv_node_config = config.at(id);
  auto raft_config = raft::RaftNode::NodeConfig{
      id, raft_cluster_config, kv_node_config.raft_log_filename, nullptr};
  // kv_server_ = KvServer::NewKvServer({raft_config, kv_node_config.kv_dbname});
  kv_server_ = KvServer::NewKvServer(config, id);
  rpc_server_ = new rpc::KvServerRPCServer(kv_node_config.kv_rpc_addr, id,
                                           rpc::KvServerRPCService(kv_server_));
}

KvServiceNode::~KvServiceNode() {
  if (!kv_server_->Exited()) {
    kv_server_->Exit();
  }
  rpc_server_->Stop();
  delete kv_server_;
  delete rpc_server_;
}

void KvServiceNode::InitServiceNodeState() { kv_server_->Init(); }

void KvServiceNode::StartServiceNode() {
  kv_server_->Start();
  rpc_server_->Start();
  printf("[KVNode Start Running]:\n[Storage Engine]: %s\n",
         kv_server_->DB()->EngineName().c_str());
}

void KvServiceNode::StopServiceNode() {
  if (!kv_server_->Exited()) {
    kv_server_->Exit();
  }
  rpc_server_->Stop();
}

};  // namespace kv
