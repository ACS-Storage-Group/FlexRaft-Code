#include <filesystem>
#include <memory>
#include <string>

#include "raft.h"
#include "raft_node.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "rcf_rpc.h"
#include "rpc.h"
#include "util.h"

namespace raft {

static const int kRPCBenchTestPort = 50001;

using NodesConfig = std::unordered_map<raft_node_id_t, rpc::NetAddress>;

static const NodesConfig kLocalConfigs = {
    {1, {"127.0.0.1", 50001}},
    {2, {"127.0.0.1", 50002}},
    {3, {"127.0.0.1", 50003}},
    {4, {"127.0.0.1", 50004}},
};

static const NodesConfig kCloudConfigs = {
    {1, {"172.20.83.195", 50001}}, 
    {2, {"172.20.83.196", 50001}},
    {3, {"172.20.83.191", 50001}}, 
    {4, {"172.20.83.193", 50001}},
    {5, {"172.20.83.194", 50001}}, 
    {6, {"172.20.83.190", 50001}},
};

std::string GetRaftLogName(raft_node_id_t id) {
  return "raftlog" + std::to_string(id);
}

struct CommitLatencyRecorder {
public:
  CommitLatencyRecorder() = default;
  ~CommitLatencyRecorder() = default;
  void Add(uint64_t time) { hist_.push_back(time); }
  void Dump(const std::string &dst) {
    std::ofstream of;
    of.open(dst);
    for (const auto &data : hist_) {
      of << data << "\n";
    }
    of.close();
  }
  std::vector<uint64_t> hist_;
};

auto GenerateRandomSlice(int min_len, int max_len) -> Slice {
  int rand_size =
      (min_len == max_len) ? min_len : rand() % (max_len - min_len) + min_len;
  auto rand_data = new char[rand_size];
  for (decltype(rand_size) i = 0; i < rand_size; ++i) {
    rand_data[i] = rand();
  }
  return Slice(rand_data, rand_size);
}

void DestructSlice(const Slice &slice) { delete[] slice.data(); }

void RunRaftFollower(raft_node_id_t id, const NodesConfig &config) {
  // Create a raft follower
  auto rpc_server = std::make_shared<rpc::RCFRpcServer>(config.at(id));
  auto filename = GetRaftLogName(id);
  auto storage = FileStorage::Open(filename);

  // Construct and initialize a raft state
  auto raft_config = RaftConfig{id, {}, storage, nullptr, 10000, 10000, nullptr};
  auto raft_stat = RaftState::NewRaftState(raft_config);
  raft_stat->SetRole(kFollower);
  raft_stat->SetCurrentTerm(1);
  raft_stat->SetCommitIndex(0);
  rpc_server->setState(raft_stat);

  rpc_server->Start();

  std::cout << "[Input to exit]:";
  char c;
  std::cin >> c;

  std::filesystem::remove(filename);
}

std::shared_ptr<RaftState> ConstructRaftLeader(raft_node_id_t id,
                                               const NodesConfig &configs) {
  std::unordered_map<raft_node_id_t, rpc::RpcClient *> rpc_clients;
  for (const auto &[id, net] : configs) {
    rpc_clients.insert({id, new rpc::RCFRpcClient(net, id)});
  }

  auto filename = GetRaftLogName(id);
  auto storage = FileStorage::Open(filename);

  std::shared_ptr<RaftState> ret;
  auto raft_config =
      RaftConfig{id, rpc_clients, storage, nullptr, 10000, 10000, nullptr};
  ret.reset(RaftState::NewRaftState(raft_config));
  ret->convertToLeader();
  ret->SetCurrentTerm(1);
  ret->SetCommitIndex(0);

  // Remember to set the raft state for RPC clients so that callback can be
  // called
  for (auto id : ret->peers_) {
    auto rpc = ret->rpc_clients_[id];
    rpc->setState(ret.get());
  }

  return ret;
}

void DumpRPCClients(std::shared_ptr<RaftState> raft_stat, int data_size) {
  std::ofstream of;
  auto filename = "results-" + std::to_string(data_size);
  of.open(filename);
  for (auto id : raft_stat->peers_) {
    auto rpc = raft_stat->rpc_clients_[id];
    reinterpret_cast<rpc::RCFRpcClient *>(rpc)->Dump(of);
  }
  of.close();
}

void RunRaftLeader(raft_node_id_t id, const NodesConfig &configs, int data_size,
                   int propose_cnt) {
  auto leader = ConstructRaftLeader(id, configs);
  RCF::sleepMs(1000);
  CommitLatencyRecorder recorder;
  for (int i = 1; i <= propose_cnt; ++i) {
    auto data = GenerateRandomSlice(data_size, data_size);
    auto start = util::NowTime();
    auto pr = leader->Propose(CommandData{0, data});
    assert(pr.is_leader == true);
    // Loop until this entry is committed
    while (leader->CommitIndex() < pr.propose_index) {
      assert(leader->Role() == kLeader);
      leader->Tick();
    }
    auto end = util::NowTime();
    recorder.Add(util::DurationToMicros(start, end));

    printf("\e[?25l");
    printf("Already Done: %5d / %5d\r", i, propose_cnt);
  }
  printf("\e[?25h");
  puts("");

  recorder.Dump("commit_latency.txt");
  DumpRPCClients(leader, data_size);
  std::filesystem::remove(GetRaftLogName(id));
}
} // namespace raft

int ParseNum(const char *s) {
  auto str = std::string(s);
  if (std::isdigit(str.back())) {
    return std::atoi(s);
  }
  auto num = std::atoi(str.substr(0, str.length() - 1).c_str());
  switch (str.back()) {
  case 'K':
  case 'k':
    return num * 1024;
  case 'M':
  case 'm':
    return num * 1024 * 1024;
  }
  return 0;
}

int main(int argc, char *argv[]) {
  bool is_leader = (std::string(argv[1]) == "leader");
  bool is_local = (std::string(argv[2]) == "local");
  auto data_size = ParseNum(argv[3]);
  auto propose_cnt = ParseNum(argv[4]);
  auto id = std::atoi(argv[5]);
  auto configs = is_local ? raft::kLocalConfigs : raft::kCloudConfigs;
  if (is_leader) {
    raft::RunRaftLeader(id, configs, data_size, propose_cnt);
  } else {
    raft::RunRaftFollower(id, configs);
  }
  return 0;
}
