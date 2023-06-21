#include "raft_node.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "raft.h"
#include "rcf_rpc.h"
#include "rsm.h"
#include "storage.h"
#include "util.h"

namespace raft {

RaftNode::RaftNode(const NodeConfig &node_config)
    : node_id_me_(node_config.node_id_me),
      servers_(node_config.servers),
      raft_state_(nullptr),
      rsm_(node_config.rsm) {
  if (node_config.storage_filename != "") {
    storage_ = FileStorage::Open(node_config.storage_filename);
  } else {
    storage_ = nullptr;
  }
}

void RaftNode::Init() {
  // Create an RPC server that receives request from remote servers
  rcf_server_ = new rpc::RCFRpcServer(servers_[node_id_me_]);
  rcf_server_->Start();

  // Create an RPC client that is able to send RPC request to remote server
  for (const auto &[id, addr] : servers_) {
    if (id != node_id_me_) {
      rcf_clients_.insert({id, new rpc::RCFRpcClient(addr, id)});
    }
  }

  // Create Raft State instance
  RaftConfig config = RaftConfig{
      node_id_me_, rcf_clients_, storage_, config::kElectionTimeoutMin, config::kElectionTimeoutMax,
      rsm_};
  raft_state_ = RaftState::NewRaftState(config);

  // Set related state for all RPC related struct
  rcf_server_->setState(raft_state_);
  for (auto &[_, client] : rcf_clients_) {
    client->setState(raft_state_);
  }

  // Setup failure state
  exit_.store(false);
  disconnected_.store(false);
}

void RaftNode::Start() {
  LOG(util::kRaft, "S%d Starts", node_id_me_);
  printf("[FlexibleK Raft Starts Running]\n");
  exit_.store(false);
  raft_state_->Init();
  startTickerThread();
  startApplierThread();
}

void RaftNode::Exit() {
  // First ensures ticker thread and applier thread exits, in case they access
  // raft_state field after we release it
  this->exit_.store(true);
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // TODO: Release storage or state machine if it's necessary
  rcf_server_->Stop();  // Stop running rcf server
  delete rcf_server_;
  delete storage_;
}

RaftNode::~RaftNode() {
  delete raft_state_;
  for (auto &[_, client] : rcf_clients_) {
    delete client;
  }
  // delete rcf_server_;
}

void RaftNode::startTickerThread() {
  auto ticker = [=]() {
    while (!this->Exited()) {
      // Tick the raft state for every 10ms so that the raft can make progress
      // std::this_thread::sleep_for(std::chrono::milliseconds(10));
      this->raft_state_->Tick();
      std::this_thread::sleep_for(std::chrono::milliseconds(config::kRaftTickBaseInterval));
    }
  };
  std::thread ticker_thread(ticker);
  ticker_thread.detach();
}

void RaftNode::Disconnect() {
  LOG(util::kRaft, "S%d stop running raft node", node_id_me_);
  rcf_server_->Stop();
  for (auto [_, client_ptr] : rcf_clients_) {
    client_ptr->stop();
  }
  disconnected_.store(true);
}

void RaftNode::Reconnect() {
  rcf_server_->Start();
  for (auto [_, client_ptr] : rcf_clients_) {
    client_ptr->recover();
  }
  disconnected_.store(false);
}

void RaftNode::startApplierThread() {
  // TODO
}

}  // namespace raft
