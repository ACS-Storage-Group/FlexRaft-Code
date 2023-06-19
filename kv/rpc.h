#pragma once
#include <functional>

#include "RCF/ClientStub.hpp"
#include "RCF/Future.hpp"
#include "RCF/InitDeinit.hpp"
#include "RCF/RCF.hpp"
#include "RCF/RcfFwd.hpp"
#include "RCF/RcfMethodGen.hpp"
#include "RCF/RcfServer.hpp"
#include "RCF/TcpEndpoint.hpp"
#include "kv_server.h"
#include "raft_type.h"
#include "type.h"
#include "util.h"
namespace kv {
class KvServer;
namespace rpc {

// Define the RPC return value and parameter
RCF_BEGIN(I_KvServerRPCService, "I_KvServerRPCService")
RCF_METHOD_R1(Response, DealWithRequest, const Request &)
RCF_METHOD_R1(GetValueResponse, GetValue, const GetValueRequest &)
RCF_END(I_KvServerRPCService)

class KvServerRPCService {
public:
  KvServerRPCService() = default;
  KvServerRPCService(KvServer *server) : server_(server) {}
  Response DealWithRequest(const Request &req) {
    Response resp;
    server_->DealWithRequest(&req, &resp);
    return resp;
  }

  GetValueResponse GetValue(const GetValueRequest &request) {
    LOG(raft::util::kRaft, "S%d recv GetValue Request: readIndex=%d",
        server_->Id(), request.read_index);
    raft::util::Timer timer;
    timer.Reset();
    // Spin until the entries before read index have been applied into the DB
    while (server_->LastApplyIndex() < request.read_index) {
      ;
    }
    std::string value;
    auto found = server_->DB()->Get(request.key, &value);
    LOG(raft::util::kRaft, "S%d make GetValue Response", server_->Id());
    if (found) {
      return GetValueResponse{std::move(value), kOk, server_->Id()};
    } else {
      return GetValueResponse{std::string(""), kKeyNotExist, server_->Id()};
    }
  }

  void SetKvServer(KvServer *server) { server_ = server; }

private:
  KvServer *server_;
};

// RPC client issues a DealWithRequest RPC to specified KvNode by
// simply call "DealWithRequest()". The call is synchronized and might be
// blocked. We need a timeout to solve this problem.
//
// Each KvServerRPCClient object responds to a KvNode
class KvServerRPCClient {
public:
  using ClientPtr = std::shared_ptr<RcfClient<I_KvServerRPCService>>;
  KvServerRPCClient(const NetAddress &net_addr, raft::raft_node_id_t id)
      : address_(net_addr), id_(id),
        client_stub_(RCF::TcpEndpoint(net_addr.ip, net_addr.port)),
        rcf_init_() {
    client_stub_.getClientStub().getTransport().setMaxIncomingMessageLength(
        raft::rpc::config::kMaxMessageLength);
    client_stub_.getClientStub().getTransport().setMaxOutgoingMessageLength(
        raft::rpc::config::kMaxMessageLength);
  }

  Response DealWithRequest(const Request &request);

  void GetValue(const GetValueRequest &request,
                std::function<void(const GetValueResponse &)> cb);

  void onGetValueComplete(RCF::Future<GetValueResponse> ret,
                          std::function<void(const GetValueResponse &)> cb);

  GetValueResponse GetValue(const GetValueRequest &request);

  // Set timeout for this RPC call, a typical value might be 300ms?
  void SetRPCTimeOutMs(int cnt) {
    client_stub_.getClientStub().setRemoteCallTimeoutMs(cnt);
  }

private:
  RCF::RcfInit rcf_init_;
  NetAddress address_;
  raft::raft_node_id_t id_;
  RcfClient<I_KvServerRPCService> client_stub_;
};

// Server side of a KvNode, the server calls Start() to continue receive
// RPC request from client and deal with it.
class KvServerRPCServer {
public:
  KvServerRPCServer(const NetAddress &net_addr, raft::raft_node_id_t id,
                    KvServerRPCService service)
      : address_(net_addr), id_(id),
        server_(RCF::TcpEndpoint(net_addr.ip, net_addr.port)),
        service_(service) {
    LOG(raft::util::kRaft, "S%d RPC init with (ip=%s port=%d)", id_,
        net_addr.ip.c_str(), net_addr.port);
  }
  KvServerRPCServer() = default;

  void Start() {
    server_.getServerTransport().setMaxIncomingMessageLength(
        raft::rpc::config::kMaxMessageLength);
    server_.bind<I_KvServerRPCService>(service_);
    server_.start();
  }

  void Stop() {
    LOG(raft::util::kRaft, "S%d stop RPC server", id_);
    server_.stop();
  }

  void SetServiceContext(KvServer *server) { service_.SetKvServer(server); }

private:
  RCF::RcfInit rcf_init_;
  NetAddress address_;
  raft::raft_node_id_t id_;
  RCF::RcfServer server_;
  KvServerRPCService service_;
};

} // namespace rpc
} // namespace kv
