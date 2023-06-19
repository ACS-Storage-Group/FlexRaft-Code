#pragma once
#include <cstdint>
#include <string>

#include "raft_type.h"

namespace kv {
enum RequestType {
  kPut = 1,
  kDelete = 2,
  kGet = 3,
  kDetectLeader = 4,
  // kUndetermined = 5,
};

enum ErrorType {
  kNotALeader = 1,
  kKeyNotExist = 2,
  kEntryDeleted = 3,
  kRequestExecTimeout = 4,
  kOk = 5,
  // This error is used in test, in which a request can not be done within
  // specified seconds, probably due to no enough servers
  kKVRequestTimesout = 6,
  kRPCCallFailed = 7,
  kKVDecodeFail = 8,
};

struct Request {
  RequestType type;
  uint32_t client_id;
  uint32_t sequence;
  std::string key;
  std::string value;  // Ignore it if this request is not Put
  void serialize(SF::Archive& ar) { ar& type& client_id& sequence& key& value; }
};

struct Response {
  RequestType type;
  uint32_t client_id;
  uint32_t sequence;
  ErrorType err;
  raft::raft_term_t raft_term;
  std::string value;              // Valid if type is Get
  int k, m;                       // The parameter needed to construct an original value
  raft::raft_index_t read_index;  // This is valid only when type is Get
  raft::raft_node_id_t reply_server_id;  // The id of server that makes this response
  uint64_t apply_elapse_time;   // Time elapsed to apply this entry to state machine
  uint64_t commit_elapse_time;  // Time elapsed to commit a raft entry: From proposal to
                                // append it into channel
  void serialize(SF::Archive& ar) {
    ar& type& client_id& sequence& err& raft_term& value& k& m& read_index&
        reply_server_id& apply_elapse_time& commit_elapse_time;
  }
};

// The request & response struct uniquely marks an RPC process named GetValue
struct GetValueRequest {
  std::string key;
  raft::raft_index_t read_index;
  void serialize(SF::Archive& ar) { ar& key& read_index; }
};

struct GetValueResponse {
  std::string value;
  ErrorType err;
  raft::raft_node_id_t reply_server_id;
  void serialize(SF::Archive& ar) { ar& value& err& reply_server_id; }
};

struct RequestWithFragment {
  RequestType type;
  uint32_t client_id;
  uint32_t sequence;
  std::string key;
  std::string fragmentvalue;
  int k, m;  // The parameter needed to construct an original value
};

inline constexpr size_t RequestHdrSize() {
  return sizeof(RequestType) + sizeof(uint32_t) * 2;
}

inline constexpr size_t ResponseHdrSize() {
  return sizeof(RequestType) + sizeof(uint32_t) * 2 + sizeof(ErrorType) +
         sizeof(raft::raft_term_t);
}

const std::string ToString(RequestType type);
const std::string ToString(ErrorType type);
const std::string ToString(const Request& req);

namespace rpc {
struct NetAddress {
  std::string ip;
  uint16_t port;
  bool operator==(const NetAddress& rhs) const {
    return this->ip == rhs.ip && this->port == rhs.port;
  }
};
};

}  // namespace kv
