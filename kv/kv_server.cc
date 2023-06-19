#include "kv_server.h"

#include <chrono>
#include <mutex>
#include <thread>

#include "RCF/RecursionLimiter.hpp"
#include "RCF/ThreadLibrary.hpp"
#include "client.h"
#include "kv_format.h"
#include "log_entry.h"
#include "raft_node.h"
#include "raft_struct.h"
#include "rpc.h"
#include "storage_engine.h"
#include "type.h"
#include "util.h"

namespace kv {
KvServer *KvServer::NewKvServer(const KvServerConfig &config) {
  auto kv_server = new KvServer();
  kv_server->channel_ = Channel::NewChannel(100000);
  kv_server->db_ = StorageEngine::Default(config.storage_engine_name);
  kv_server->id_ = config.raft_node_config.node_id_me;

  // Pass channel as a Rsm into raft
  auto raft_config = config.raft_node_config;
  raft_config.rsm = kv_server->channel_;
  kv_server->raft_ = new raft::RaftNode(raft_config);

  kv_server->exit_ = false;
  return kv_server;
}

KvServer *KvServer::NewKvServer(const KvClusterConfig &config,
                                raft::raft_node_id_t node_id) {
  auto raft_cluster_config = ConstructRaftClusterConfig(config);
  assert(config.count(node_id) > 0);
  auto kv_node_config = config.at(node_id);
  auto raft_config = raft::RaftNode::NodeConfig{
      node_id, raft_cluster_config, kv_node_config.raft_log_filename, nullptr};
  auto kv_server =
      KvServer::NewKvServer({raft_config, kv_node_config.kv_dbname});
  // Register the RPC clients to it
  for (const auto &[id, conf] : config) {
    if (id == node_id) {
      continue;
    }
    kv_server->kv_peers_.insert(
        {id, new rpc::KvServerRPCClient(conf.kv_rpc_addr, id)});
  }
  return kv_server;
}

void KvServer::Start() {
  raft_->Start();
  startApplyKvRequestCommandsThread();
}

// A server receives a request from outside world(e.g. A client or a mock
// client) and it should deal with this request properly
void KvServer::DealWithRequest(const Request *request, Response *resp) {
  if (IsDisconnected()) {
    resp->err = kRequestExecTimeout;
    return;
  }
  LOG(raft::util::kRaft, "S%d Deals with Req(From C%d) %s", id_,
      request->client_id, ToString(*request).c_str());

  resp->type = request->type;
  resp->client_id = request->client_id;
  resp->sequence = request->sequence;
  resp->raft_term = raft_->getRaftState()->CurrentTerm();
  resp->reply_server_id = id_;

  switch (request->type) {
  case kDetectLeader:
    resp->err = raft_->IsLeader() ? kOk : kNotALeader;
    LOG(raft::util::kRaft, "S%d reply DetectLeader term:%d err:%s", Id(),
        resp->raft_term, ToString(resp->err).c_str());
    return;
  case kPut:
  case kDelete: {
    auto size = GetRawBytesSizeForRequest(*request);
    auto data = new char[size + 12];
    RequestToRawBytes(*request, data);

    // find the start offset, it must contain the request Header and the key
    // content
    int start_offset = RequestHdrSize() + sizeof(int) + request->key.size();

    LOG(raft::util::kRaft, "S%d propose request startoffset(%d)", id_,
        start_offset);

    // Construct a raft command
    raft::util::Timer commit_timer;
    commit_timer.Reset();

    auto cmd = raft::CommandData{start_offset, raft::Slice(data, size)};
    auto pr = raft_->Propose(cmd);

    // Loop until the propose entry to be applied
    raft::util::Timer timer;
    timer.Reset();
    KvRequestApplyResult ar;
    while (timer.ElapseMilliseconds() <= 300) {
      // Check if applied
      if (CheckEntryCommitted(pr, &ar)) {
        resp->err = ar.err;
        resp->value = ar.value;
        resp->apply_elapse_time = ar.elapse_time;
        // Calculate the time elapsed for commit
        resp->commit_elapse_time =
            commit_timer.ElapseMicroseconds() - resp->apply_elapse_time;
        // resp->commit_elapse_time = raft_->CommitLatency(pr.propose_index);
        LOG(raft::util::kRaft, "S%d ApplyResult value=%s", id_,
            resp->value.c_str());
        return;
      }
    }
    // Otherwise timesout
    resp->err = kRequestExecTimeout;
    return;
  }

  case kGet: {
    ExecuteGetOperation(request, resp);
    return;
  }
  }
}

// Check if a particular propose has been committed and set the ApplyResult
// struct if it has been committed
bool KvServer::CheckEntryCommitted(const raft::ProposeResult &pr,
                                   KvRequestApplyResult *apply) {
  // Not committed entry
  std::scoped_lock<std::mutex> lck(map_mutex_);
  if (applied_cmds_.count(pr.propose_index) == 0) {
    return false;
  }

  auto ar = applied_cmds_[pr.propose_index];
  apply->raft_term = ar.raft_term;
  if (ar.raft_term != pr.propose_term) {
    apply->err = kEntryDeleted;
    apply->value = "";
    apply->elapse_time = ar.elapse_time;
  } else {
    apply->err = ar.err;
    apply->value = ar.value;
    apply->elapse_time = ar.elapse_time;
  }
  return true;
}

void KvServer::ApplyRequestCommandThread(KvServer *server) {
  raft::util::Timer elapse_timer;
  while (!server->exit_.load()) {
    raft::LogEntry ent;
    if (!server->channel_->TryPop(ent)) {
      continue;
    }
    LOG(raft::util::kRaft, "S%d Pop Ent From Raft I%d T%d", server->Id(),
        ent.Index(), ent.Term());

    elapse_timer.Reset();

    // Apply this entry to state machine(i.e. Storage Engine)
    Request req;
    // RawBytesToRequest(ent.CommandData().data(), &req);
    RaftEntryToRequest(ent, &req, server->Id(), server->ClusterServerNum());

    LOG(raft::util::kRaft, "S%d Apply request(%s) to db", server->Id(),
        ToString(req).c_str());

    std::string get_value;
    KvRequestApplyResult ar = {ent.Term(), kOk, std::string("")};
    switch (req.type) {
    case kPut: {
      server->db_->Put(req.key, req.value);
      ar.elapse_time = elapse_timer.ElapseMicroseconds();
      break;
    }
    case kDelete: {
      server->db_->Delete(req.key);
      ar.elapse_time = elapse_timer.ElapseMicroseconds();
      break;
    }
    // NOTE: Get will not go through this path since no raft entry will be
    // generated for Get operation
    case kGet:
    default:
      assert(0);
    }

    server->applied_index_ = ent.Index();

    LOG(raft::util::kRaft, "S%d Apply request(%s) to db Done, APPLY I%d",
        server->Id(), ToString(req).c_str(), server->LastApplyIndex());
    // Add the apply result into map
    std::scoped_lock<std::mutex> lck(server->map_mutex_);
    server->applied_cmds_.insert({ent.Index(), ar});
  }
}

void KvServer::ExecuteGetOperation(const Request *request, Response *resp) {
  auto read_index = this->raft_->LastIndex();
  LOG(raft::util::kRaft, "S%d Execute Get Operation, ReadIndex=%d", id_,
      read_index);

  resp->read_index = read_index;

  // spin until the entry has been applied
  raft::util::Timer timer;
  timer.Reset();
  while (LastApplyIndex() < read_index) {
    if (timer.ElapseMilliseconds() >= 500) {
      LOG(raft::util::kRaft, "S%d Execute Get Operation Timeout, ReadIndex=%d",
          id_, read_index);
      resp->err = kRequestExecTimeout;
      return;
    }
    LOG(raft::util::kRaft,
        "S%d Execute Get Operation(ApplyIndex:%d) ReadIndex%d", id_,
        LastApplyIndex(), read_index);

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  auto succ = db_->Get(request->key, &(resp->value));
  if (!succ) {
    resp->err = kKeyNotExist;
    return;
  }

  // The key is successfully found, however, the get value might be a fragment.
  // In such case, the leader issues GetValue RPC to all servers to retrive the
  // fragments back.
  auto format = KvServiceClient::DecodeString(&(resp->value));

  // k = 1 means this this the full entry value, simply returns Ok
  if (format.k == 1) {
    resp->err = kOk;
    return;
  }

  // Otherwise, a value gathering task should be established and executed to get
  // the full entry value.
  int k = format.k, m = format.m;
  raft::Encoder::EncodingResults input;
  input.insert({format.frag_id, raft::Slice::Copy(format.frag)});
  LOG(raft::util::kRaft, "[S%d] Add Fragment of Frag%d", Id(), format.frag_id);

  ValueGatheringTask task{
      request->key, resp->read_index, resp->reply_server_id, &input, k, m};
  ValueGatheringTaskResults res{&(resp->value), kOk};

  DoValueGatheringTask(&task, &res);

  // Add this new decoded entry into database
  char tmp_data[12]; 
  *reinterpret_cast<int*>(tmp_data) = 1;
  *reinterpret_cast<int*>(tmp_data + 4) = 0;
  *reinterpret_cast<int*>(tmp_data + 8) = 0;

  std::string insert_full_entry = "";
  for (int i = 0; i < 12; ++i) {
    insert_full_entry.push_back(tmp_data[i]);
  }

  auto prefix_key_size = res.value->size() + sizeof(int);
  char* tmp = new char[prefix_key_size];

  MakePrefixLengthKey(*res.value, tmp);
  insert_full_entry.append(tmp, prefix_key_size);

  resp->value = insert_full_entry;
  resp->err = kOk;
  
  // Add this entry into database
  db_->Put(request->key, insert_full_entry);

  delete[] tmp;
  return;
}

void KvServer::DoValueGatheringTask(ValueGatheringTask *task,
                                    ValueGatheringTaskResults *res) {
  LOG(raft::util::kRaft, "[S%d] Start running ValueGatheringTask, k=%d, m=%d",
      Id(), task->k, task->m);
  std::atomic<bool> gather_value_done = false;

  // Use lock to prevent concurrent callback function running
  std::mutex mtx;
  auto call_back = [=, &gather_value_done, &mtx](const GetValueResponse &resp) {
    LOG(raft::util::kRaft, "[S%d] Recv GetValue Response from S%d", Id(),
        resp.reply_server_id);
    if (resp.err != kOk) {
      return;
    }

    std::scoped_lock<std::mutex> lck(mtx);
    auto fmt =
        KvServiceClient::DecodeString(const_cast<std::string *>(&resp.value));
    LOG(raft::util::kRaft, "[S%d] DecodeString: k=%d, m=%d, fragid=%d", Id(),
        fmt.k, fmt.m, fmt.frag_id);

    // Get a full entry of value
    if (fmt.k == 1 && fmt.m == 0) {
      GetKeyFromPrefixLengthFormat(fmt.frag.data(), res->value);
      res->err = kOk;
      gather_value_done.store(true);
      LOG(raft::util::kRaft, "[S%d] Get Full Entry, value=%s", Id(),
          res->value->c_str());
      return;
    } else {
      // Get a fragment of value
      if (fmt.k == task->k && fmt.m == task->m) {
        task->decode_input->insert({fmt.frag_id, raft::Slice::Copy(fmt.frag)});
        LOG(raft::util::kRaft, "[S%d] Add Fragment%d in ValueGatheringTask",
            Id(), fmt.frag_id);
      }

      // The gather value task is not done, and there is enough fragments to
      // decode the entry
      if (!gather_value_done.load() && task->decode_input->size() >= task->k) {
        raft::Encoder encoder;
        raft::Slice results;
        auto stat = encoder.DecodeSlice(*(task->decode_input), task->k, task->m,
                                        &results);
        if (stat) {
          GetKeyFromPrefixLengthFormat(results.data(), res->value);
          res->err = kOk;
          gather_value_done.store(true);
          LOG(raft::util::kRaft, "[S%d] Decode Value Succ", Id());
        } else {
          res->err = kKVDecodeFail;
          LOG(raft::util::kRaft, "[S%d] Decode Value Fail", Id());
        }
      }
    }
  };

  auto clear_gather_ctx = [=]() {
    for (auto &[_, frag] : *(task->decode_input)) {
      delete[] frag.data();
    }
  };

  auto get_req = GetValueRequest{task->key, task->read_index};
  for (auto &[id, server] : kv_peers_) {
    if (id == task->replied_id) {
      continue;
    }
    auto stub = reinterpret_cast<rpc::KvServerRPCClient *>(server);
    stub->SetRPCTimeOutMs(1000);
    auto resp = stub->GetValue(get_req);
    if (resp.err == kOk) {
      call_back(resp);
    }
    if (gather_value_done.load()) {
      break;
    }
    // Send in an async way
    // stub->GetValue(get_req, call_back);
  }

  raft::util::Timer timer;
  timer.Reset();
  while (timer.ElapseMilliseconds() <= 1000) {
    if (gather_value_done.load() == true) {
      clear_gather_ctx();
      return;
    } else {
      // sleepMs(100);
    }
  }
  //  Set the error code
  if (res->err == kOk) {
    res->err = kRequestExecTimeout;
  }
  clear_gather_ctx();
}
} // namespace kv
