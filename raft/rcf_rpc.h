#pragma once

#include <chrono>
#include <memory>

#include "RCF/ByteBuffer.hpp"
#include "RCF/ClientStub.hpp"
#include "RCF/Future.hpp"
#include "RCF/InitDeinit.hpp"
#include "RCF/RcfServer.hpp"
#include "raft.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "rpc.h"
#include "serializer.h"

namespace raft {
namespace rpc {

namespace config {
// Each RPC call size must not exceed 512MB
static constexpr size_t kMaxMessageLength = 512 * 1024 * 1024;
static constexpr size_t kRPCTimeout = 500;
};  // namespace config

RCF_BEGIN(I_RaftRPCService, "I_RaftRPCService")
RCF_METHOD_R1(RCF::ByteBuffer, RequestVote, const RCF::ByteBuffer &)
RCF_METHOD_R1(RCF::ByteBuffer, AppendEntries, const RCF::ByteBuffer &)
RCF_METHOD_R1(RCF::ByteBuffer, RequestFragments, const RCF::ByteBuffer &)
RCF_END(I_RaftService)

// Some statistics about one rpc call arguments
struct RPCArgStats {
  size_t arg_size;
  util::TimePoint start_time;
};

// Some statistics about one rpc call
struct RPCStats {
  size_t arg_size;
  size_t resp_size;
  int64_t total_time;
  int64_t transfer_time;
  int64_t process_time;

  std::string ToString() const {
    char buf[512];
    sprintf(buf,
            "[Total Time = %ldus][Process Time = %ldus][Transfer Time = "
            "%ldus][Args "
            "size=%luB][Reply size=%luB]",
            total_time, process_time, total_time - process_time, arg_size, resp_size);
    return std::string(buf);
  }
};

struct RPCStatsRecorder {
 public:
  RPCStatsRecorder() : history_() {
    history_.reserve(10000);
    // Add a default history result
    history_.push_back({0, 0, 0, 0, 0});
  }

  // Write the results to a specified file
  void Dump(const std::string &dst);
  void Dump(std::ofstream &of);

  void Add(const RPCStats &stat) { history_.push_back(stat); }

  std::vector<RPCStats> history_;
};

class RaftRPCService {
 public:
  RaftRPCService() = default;
  void SetRaftState(RaftState *raft) { raft_ = raft; }
  RCF::ByteBuffer RequestVote(const RCF::ByteBuffer &arg_buf);
  RCF::ByteBuffer AppendEntries(const RCF::ByteBuffer &arg_buf);
  RCF::ByteBuffer RequestFragments(const RCF::ByteBuffer &arg_buf);

 private:
  RaftState *raft_;
};

// An implementation of RpcClient interface using RCF (Remote Call Framework)
class RCFRpcClient final : public RpcClient {
  using ClientPtr = std::shared_ptr<RcfClient<I_RaftRPCService>>;

 public:
  // Construction
  RCFRpcClient(const NetAddress &target_address, raft_node_id_t id);

  RCFRpcClient &operator=(const RCFRpcClient &) = delete;
  RCFRpcClient(const RCFRpcClient &) = delete;

 public:
  void SetRaftState(RaftState *raft) { raft_ = raft; }
  void Dump(const std::string &filename) { recorder_.Dump(filename); }
  void Dump(std::ofstream &of) { recorder_.Dump(of); }

 public:
  void Init() override;
  void sendMessage(const RequestVoteArgs &args) override;
  void sendMessage(const AppendEntriesArgs &args) override;
  void sendMessage(const RequestFragmentsArgs &args) override;
  void setState(void *state) override { raft_ = reinterpret_cast<RaftState *>(state); }

  void setMaxTransportLength(ClientPtr ptr) {
    ptr->getClientStub().getTransport().setMaxOutgoingMessageLength(config::kMaxMessageLength);
    ptr->getClientStub().getTransport().setMaxIncomingMessageLength(config::kMaxMessageLength);
  }

  void stop() override { stopped_ = true; };
  void recover() override { stopped_ = false; };

 private:
  // Callback function
  static void onRequestVoteComplete(RCF::Future<RCF::ByteBuffer> buf, ClientPtr client_ptr,
                                    RaftState *raft, raft_node_id_t peer);

  static void onAppendEntriesComplete(RCF::Future<RCF::ByteBuffer> buf, ClientPtr client_ptr,
                                      RaftState *raft, raft_node_id_t peer, RPCArgStats arg_stats,
                                      RPCStatsRecorder *recorder);

  static void onAppendEntriesCompleteRecordTimer(RCF::Future<RCF::ByteBuffer> buf,
                                                 ClientPtr client_ptr, RaftState *raft,
                                                 raft_node_id_t peer,
                                                 util::AppendEntriesRPCPerfCounter counter);

  static void onRequestFragmentsComplete(RCF::Future<RCF::ByteBuffer> buf, ClientPtr client_ptr,
                                         RaftState *raft, raft_node_id_t peer);

 private:
  RaftState *raft_ = nullptr;
  RCF::RcfInit rcf_init_;
  NetAddress target_address_;
  bool stopped_;
  raft_node_id_t id_;

  RPCStatsRecorder recorder_;
};

class RCFRpcServer final : public RpcServer {
 public:
  RCFRpcServer(const NetAddress &my_address);

 public:
  void Start() override;
  void Stop() override;
  void dealWithMessage(const RequestVoteArgs &reply) override;
  void setState(void *state) override {
    service_.SetRaftState(reinterpret_cast<RaftState *>(state));
  }

 private:
  RCF::RcfInit rcf_init_;
  RCF::RcfServer server_;
  RaftRPCService service_;
};
}  // namespace rpc
}  // namespace raft
