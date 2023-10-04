#include "client.h"

#include "chunk.h"
#include "code_conversion.h"
#include "config.h"
#include "encoder.h"
#include "kv_format.h"
#include "raft_type.h"
#include "type.h"
#include "util.h"
namespace kv {
KvServiceClient::KvServiceClient(const KvClusterConfig &config, uint32_t client_id)
    : client_id_(client_id) {
  for (const auto &[id, conf] : config) {
    servers_.insert({id, new rpc::KvServerRPCClient(conf.kv_rpc_addr, id)});
  }
  curr_leader_ = kNoDetectLeader;
  curr_leader_term_ = 0;
}

KvServiceClient::~KvServiceClient() {
  for (auto [id, ptr] : servers_) {
    delete ptr;
  }
}

Response KvServiceClient::WaitUntilRequestDone(const Request &request) {
  raft::util::Timer timer;
  timer.Reset();
  LOG(raft::util::kRaft, "[C%d] Dealing With Req (%s)", ClientId(), ToString(request).c_str());
  while (timer.ElapseMilliseconds() < kKVRequestTimesoutCnt * 1000) {
    if (curr_leader_ == kNoDetectLeader && DetectCurrentLeader() == kNoDetectLeader) {
      LOG(raft::util::kRaft, "Detect No Leader");
      sleepMs(300);
      continue;
    }
    LOG(raft::util::kRaft, "[C%d] Send Req (%s) to S%d", ClientId(), ToString(request).c_str(),
        curr_leader_);
    auto resp = GetRPCStub(curr_leader_)->DealWithRequest(request);
    switch (resp.err) {
      case kOk:
      case kKeyNotExist:
        return resp;

      case kEntryDeleted:
      // The leader might be separated from the cluster
      case kRequestExecTimeout:
      case kNotALeader:
      case kRPCCallFailed:
        LOG(raft::util::kRaft, "[C%d] Recv Resp(err=%s), Fallback to Nonleader", ClientId(),
            ToString(resp.err).c_str());
        curr_leader_ = kNoDetectLeader;
        curr_leader_term_ = 0;
        break;
        // default:
        // assert(false);
    }

    // The abort command only executes once
    if (request.type == kAbort) {
      return resp;
    }
  }
  // Timeout
  Response resp;
  resp.err = kRequestExecTimeout;
  return resp;
}

OperationResults KvServiceClient::Put(const std::string &key, const std::string &value) {
  Request request = {kPut, ClientId(), 0, key, value};
  auto resp = WaitUntilRequestDone(request);
  return OperationResults{resp.err, resp.apply_elapse_time, resp.commit_elapse_time};
}

OperationResults KvServiceClient::AbortLeader() {
  Request request = {kAbort, ClientId(), 0, "", ""};
  auto resp = WaitUntilRequestDone(request);
  return OperationResults{resp.err, 0, 0};
}

OperationResults KvServiceClient::Get(const std::string &key, std::string *value) {
  Request request = {kGet, ClientId(), 0, key, std::string("")};
  auto resp = WaitUntilRequestDone(request);

  LOG(raft::util::kRaft, "[C%d] Recv Resp from S%d", ClientId(), resp.reply_server_id);

  if (resp.err != kOk) {
    return OperationResults{resp.err, resp.apply_elapse_time};
  }

  // Decoding the response byte array for further information: we may need to
  // collect other fragments
  auto format = DecodeString(&resp.value);

  // This is the value
  if (format.k == 1) {
    GetKeyFromPrefixLengthFormat(format.frag.data(), value);
    return OperationResults{kOk, 0};
  }

  int k = format.k, m = format.m;
  LOG(raft::util::kRaft, "[Get Partial Value: k=%d m=%d readindex=%d], start collecting", format.k,
      format.m, resp.read_index);

  // Initialize a GatherValue task
  // raft::Encoder::EncodingResults input;
  // input.insert({format.frag_id, raft::Slice::Copy(format.frag)});
  // LOG(raft::util::kRaft, "[C%d] Add Fragment of Frag%d from S%d", ClientId(), format.frag_id,
  //     resp.reply_server_id);

  // GatherValueTask task{key, resp.read_index, resp.reply_server_id, &input, k, m};
  // GatherValueTaskResults res{value, kOk};

  // DoGatherValueTask(&task, &res);

  // Initialize a GatherValue task (ver. Code Conversion)
  std::map<raft::raft_node_id_t, raft::code_conversion::ChunkVector> decode_input;
  raft::code_conversion::ChunkVector cv;
  cv.Deserialize(format.frag);
  decode_input.insert_or_assign(resp.reply_server_id, cv);

  GatherValueTaskCodeConversion cc_task{
      key, resp.read_index, resp.reply_server_id, &decode_input, k, m};
  GatherValueTaskResults cc_res{value, kOk};

  LOG(raft::util::kRaft, "[CC] [KVClient] Add ChunkVector of Frag%d from S%d", format.frag_id,
      resp.reply_server_id);

  DoGatherValueTaskCodeConversion(&cc_task, &cc_res);

  return OperationResults{cc_res.err, 0};
}

OperationResults KvServiceClient::Delete(const std::string &key) {
  Request request = {kDelete, ClientId(), 0, key, ""};
  auto resp = WaitUntilRequestDone(request);
  return OperationResults{resp.err, resp.apply_elapse_time};
}

raft::raft_node_id_t KvServiceClient::DetectCurrentLeader() {
  for (auto &[id, stub] : servers_) {
    if (stub == nullptr) {
      continue;
    }
    Request detect_request = {kDetectLeader, ClientId(), 0, "", ""};
    auto resp = GetRPCStub(id)->DealWithRequest(detect_request);
    if (resp.err == kOk) {
      if (resp.raft_term > curr_leader_term_) {
        curr_leader_ = id;
        curr_leader_term_ = resp.raft_term;
      }
    }
  }
  return curr_leader_;
}

void KvServiceClient::DoGatherValueTask(const GatherValueTask *task, GatherValueTaskResults *res) {
  LOG(raft::util::kRaft, "[C%d] Start running Gather Value Task, k=%d, m=%d", ClientId(), task->k,
      task->m);
  std::atomic<bool> gather_value_done = false;

  // Use lock to prevent concurrent callback function running
  std::mutex mtx;

  auto call_back = [=, &gather_value_done, &mtx](const GetValueResponse &resp) {
    LOG(raft::util::kRaft, "[C%d] Recv GetValue Response from S%d", ClientId(),
        resp.reply_server_id);
    if (resp.err != kOk) {
      return;
    }
    std::scoped_lock<std::mutex> lck(mtx);

    auto fmt = DecodeString(const_cast<std::string *>(&resp.value));
    LOG(raft::util::kRaft, "[C%d] Decode Value: k=%d, m=%d, fragid=%d", ClientId(), fmt.k, fmt.m,
        fmt.frag_id)

    // This is an full entry
    if (fmt.k == 1 && fmt.m == 0) {
      GetKeyFromPrefixLengthFormat(fmt.frag.data(), res->value);
      res->err = kOk;
      gather_value_done.store(true);
      LOG(raft::util::kRaft, "[C%d] Get Full Entry, value=%s", ClientId(), res->value->c_str());
      return;
    } else {
      // Collect a fragment
      if (fmt.k == task->k && fmt.m == task->m) {
        task->decode_input->insert({fmt.frag_id, raft::Slice::Copy(fmt.frag)});
        LOG(raft::util::kRaft, "[C%d] Add Fragment%d", ClientId(), fmt.frag_id);
      }

      // The gather value task is not done, and there is enough fragments to
      // decode the entry
      if (!gather_value_done.load() && task->decode_input->size() >= task->k) {
        raft::Encoder encoder;
        raft::Slice results;
        auto stat = encoder.DecodeSlice(*(task->decode_input), task->k, task->m, &results);
        if (stat) {
          GetKeyFromPrefixLengthFormat(results.data(), res->value);
          res->err = kOk;
          gather_value_done.store(true);
          LOG(raft::util::kRaft, "[C%d] Decode Value Succ", ClientId());
        } else {
          res->err = kKVDecodeFail;
          LOG(raft::util::kRaft, "[C%d] Decode Value Fail", ClientId());
        }
      }
    }
  };

  auto clear_gather_ctx = [=]() {
    for (auto &[_, frag] : *(task->decode_input)) {
      delete[] frag.data();
    }
  };

  // issues parallel GetValue RPC to all nodes and decode the value when
  // receiving at least F response
  auto get_req = GetValueRequest{task->key, task->read_index};
  for (auto &[id, server] : servers_) {
    if (id != task->replied_id) {
      // GetRPCStub(id)->GetValue(get_req, call_back);
      GetRPCStub(id)->SetRPCTimeOutMs(1000);
      // Sync call for simplicity
      auto resp = GetRPCStub(id)->GetValue(get_req);
      if (resp.err == kOk) {
        call_back(resp);
      }
    }
  }

  raft::util::Timer timer;
  timer.Reset();
  while (timer.ElapseMilliseconds() <= 1000) {
    if (gather_value_done.load() == true) {
      clear_gather_ctx();
      return;
    } else {
      sleepMs(100);
    }
  }
  //  Set the error code
  if (res->err == kOk) {
    res->err = kRequestExecTimeout;
  }
  clear_gather_ctx();
}

void KvServiceClient::DoGatherValueTaskCodeConversion(GatherValueTaskCodeConversion *task,
                                                      GatherValueTaskResults *res) {
  LOG(raft::util::kRaft, "[C%d] Start running Gather Value Task, k=%d, m=%d", ClientId(), task->k,
      task->m);
  std::atomic<bool> gather_value_done = false;

  int recover_F = GetServerNum() / 2, recover_k = GetServerNum() - recover_F;
  int r = raft::code_conversion::get_chunk_count(recover_k) / recover_k;
  raft::code_conversion::CodeConversionManagement ccm(recover_k, recover_F, r);
  std::vector<raft::Slice> copied_slice;

  // Use lock to prevent concurrent callback function running
  std::mutex mtx;

  auto call_back = [=, &ccm, &gather_value_done, &mtx,
                    &copied_slice](const GetValueResponse &resp) {
    LOG(raft::util::kRaft, "[CC] [KvClient] Recv GetValue Response from S%d", resp.reply_server_id);
    if (resp.err != kOk) {
      return;
    }
    std::scoped_lock<std::mutex> lck(mtx);

    auto fmt = DecodeString(const_cast<std::string *>(&resp.value));
    LOG(raft::util::kRaft, "[CC] [KvClient] Decode Value: k=%d, m=%d, fragid=%d", fmt.k, fmt.m,
        fmt.frag_id)

    // This is an full entry
    if (fmt.k == 1 && fmt.m == 0) {
      GetKeyFromPrefixLengthFormat(fmt.frag.data(), res->value);
      res->err = kOk;
      gather_value_done.store(true);
      LOG(raft::util::kRaft, "[CC] [KvClient] Get Full Entry, value=%s", res->value->c_str());
      return;
    } else {
      // Collect a fragment
      if (fmt.k == task->k && fmt.m == task->m) {
        auto slice = raft::Slice::Copy(fmt.frag);
        copied_slice.emplace_back(slice);
        raft::code_conversion::ChunkVector cv;
        cv.Deserialize(slice);
        task->decode_input->insert_or_assign(fmt.frag_id, cv);
        LOG(raft::util::kRaft, "[KvClient] Add CV(size=%d) From S%d", cv.size(), fmt.frag_id);
      }

      // The gather value task is not done, and there is enough fragments to
      // decode the entry
      if (!gather_value_done.load() && task->decode_input->size() >= task->k) {
        raft::Slice results;
        auto stat = ccm.DecodeCollectedChunkVec(*(task->decode_input), &results);
        if (stat) {
          GetKeyFromPrefixLengthFormat(results.data(), res->value);
          res->err = kOk;
          gather_value_done.store(true);
          LOG(raft::util::kRaft, "[CC] [KvClient] Decode Value Succ");
        } else {
          res->err = kKVDecodeFail;
          LOG(raft::util::kRaft, "[CC] [KvClient] Decode Value Fail");
        }
      }
    }
  };

  auto clear_gather_ctx = [=]() {
    for (auto &slice : copied_slice) {
      delete[] slice.data();
    }
  };

  // issues parallel GetValue RPC to all nodes and decode the value when
  // receiving at least F response
  auto get_req = GetValueRequest{task->key, task->read_index};
  for (auto &[id, server] : servers_) {
    if (id != task->replied_id) {
      // GetRPCStub(id)->GetValue(get_req, call_back);
      GetRPCStub(id)->SetRPCTimeOutMs(1000);
      // Sync call for simplicity
      auto resp = GetRPCStub(id)->GetValue(get_req);
      if (resp.err == kOk) {
        call_back(resp);
      }
    }
  }

  raft::util::Timer timer;
  timer.Reset();
  while (timer.ElapseMilliseconds() <= 1000) {
    if (gather_value_done.load() == true) {
      clear_gather_ctx();
      return;
    } else {
      sleepMs(100);
    }
  }
  //  Set the error code
  if (res->err == kOk) {
    res->err = kRequestExecTimeout;
  }
  clear_gather_ctx();
}

}  // namespace kv
