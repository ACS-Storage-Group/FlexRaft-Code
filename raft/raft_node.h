#pragma once
#include <atomic>
#include <cstdio>
#include <memory>
#include <unordered_map>

#include "raft.h"
#include "raft_type.h"
#include "rcf_rpc.h"
#include "rpc.h"
#include "rsm.h"
#include "storage.h"
namespace raft {

// A raft node is the collection of raft state runtime, i.e. the raft node is
// responsible for maintaining the RaftState instance, creating RPC calls,
// persisting log entries, creating timer thread and so on.

namespace config {
static const int kRaftTickBaseInterval = 20;
}
class RaftNode {
public:
  struct NodeConfig {
    raft_node_id_t node_id_me;
    std::unordered_map<raft_node_id_t, rpc::NetAddress> servers;
    // The storage file_name, not used for now
    std::string storage_filename;
    // TODO: Add state machine into this config
    Rsm *rsm;
  };

  // Constructor
  RaftNode(const NodeConfig &node_config);
  ~RaftNode();

  // Start running this raft node
  void Start();

  // Do all necessary initialization work before starting running this node
  // server
  void Init();

  // Calling exit to stop running this raft node, and release all resources
  void Exit();

  // Check if current node has exited
  bool Exited() { return exit_.load(); }

  // To stop and continue current node. This is typically an interface for
  // testing Raft cluster robustness under unstable network condition. We call
  // Pause() on this node to make an illusion that this node is separate from
  // the cluster void Pause(); void Continue();

  // Disconnect this node from cluster, i.e. This node can not receive inbound
  // RPC and is not able to send outbound RPC, however, the ticker thread may
  // also works. This function is basically for testing
  void Disconnect();
  void Reconnect();
  bool IsDisconnected() const { return disconnected_.load(); }

  int ClusterServerNum() const { return raft_state_->GetClusterServerNumber(); }

  // NOTE: This method should only be used in test or debug mod
  RaftState *getRaftState() { return raft_state_; }
  Rsm *getRsm() { return rsm_; }

  ProposeResult Propose(const CommandData &cmd) {
    return raft_state_->Propose(cmd);
  }

  uint64_t CommitLatency(raft_index_t raft_index) {
    return raft_state_->CommitLatency(raft_index);
  }

  // Return the last raft index of this raft node
  raft::raft_index_t LastIndex() const { return raft_state_->LastIndex(); }

  bool IsLeader() { return raft_state_->Role() == kLeader; }

private:
  void startTickerThread();
  void startApplierThread();

private:
  raft_node_id_t node_id_me_;
  std::unordered_map<raft_node_id_t, rpc::NetAddress> servers_;
  RaftState *raft_state_;

  // RPC related struct
  rpc::RpcServer *rcf_server_;
  std::unordered_map<raft_node_id_t, rpc::RpcClient *> rcf_clients_;

  // Indicating if this server has exited, i.e. Stop running, this is important
  // so that the ticker thread and applier thread can also exit normally
  std::atomic<bool> exit_;
  std::atomic<bool> disconnected_;
  Rsm *rsm_;
  Storage *storage_;
};
} // namespace raft
