#pragma once

#include <string>

#include "raft_struct.h"
#include "raft_type.h"

namespace raft {
namespace rpc {

struct NetAddress {
  std::string ip;
  uint16_t port;
  bool operator==(const NetAddress& rhs) const {
    return this->ip == rhs.ip && this->port == rhs.port;
  }
};

// An interface for sending rpc requests to target remote peer
class RpcClient {
 public:
  virtual ~RpcClient() = default;
  // Do all initialization work, for example, bind to remote target server
  virtual void Init() = 0;
  virtual void sendMessage(const RequestVoteArgs& args) = 0;
  virtual void sendMessage(const AppendEntriesArgs& args) = 0;
  virtual void sendMessage(const RequestFragmentsArgs& args) = 0;
  virtual void setState(void* state) = 0;
  // Temporarily shut down this client stub. After calling this method, any sendMessage()
  // call would not work unless a recover() is called
  virtual void stop() = 0;
  virtual void recover() = 0;
};

// An interface for receiving rpc request and deals with it
class RpcServer {
 public:
  virtual ~RpcServer() = default;
  // Start running the server
  virtual void Start() = 0;
  // Stop running the rpc server, i.e Refuse process with inbound RPC
  virtual void Stop() = 0;
  virtual void dealWithMessage(const RequestVoteArgs& reply) = 0;
  virtual void setState(void* state) = 0;
};

}  // namespace rpc
}  // namespace raft
