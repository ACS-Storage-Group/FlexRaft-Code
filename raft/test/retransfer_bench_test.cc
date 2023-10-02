#include <filesystem>
#include <memory>
#include <string>

#include "chunk.h"
#include "gflags/gflags.h"
#include "raft.h"
#include "raft_node.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "rcf_rpc.h"
#include "rpc.h"
#include "storage.h"
#include "util.h"

DEFINE_string(conf, "",
              "Path to the cluster configuration file, if this is empty, it means using local "
              "thread as server");
DEFINE_string(size, "", "The size of the values");
DEFINE_int32(id, -1, "The client Id");
DEFINE_int32(write_num, 0, "The number of write operations to execute");
DEFINE_bool(is_leader, false, "If this binary should be started as a leader");

const int K = 4;
const int chunk_cnt = raft::code_conversion::get_chunk_count(K);

namespace raft {

static const int kRPCBenchTestPort = 50001;

using NodesConfig = std::unordered_map<raft_node_id_t, rpc::NetAddress>;

static const NodesConfig kLocalConfigs = {
    {0, {"127.0.0.1", 50000}}, {1, {"127.0.0.1", 50001}}, {2, {"127.0.0.1", 50002}},
    {3, {"127.0.0.1", 50003}}, {4, {"127.0.0.1", 50004}}, {5, {"127.0.0.1", 50005}},
};

static const NodesConfig kCloudConfigs = {
    {0, {"172.20.126.134", 50001}}, {1, {"172.20.126.135", 50001}}, {2, {"172.20.126.136", 50001}},
    {3, {"172.20.126.137", 50001}}, {4, {"172.20.126.138", 50001}}, {5, {"172.20.126.139", 50001}},
};

inline std::string GetRaftLogName(raft_node_id_t id) { return "raftlog" + std::to_string(id); }

inline std::string GetRaftReserveLogName(raft_node_id_t id) {
  return "raftlog" + std::to_string(id) + ".reserve";
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
  int rand_size = (min_len == max_len) ? min_len : rand() % (max_len - min_len) + min_len;
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
  Storage *storage = FileStorage::Open(GetRaftLogName(id));
  Storage *reserve_storage = FileStorage::Open(GetRaftReserveLogName(id));

  // Construct and initialize a raft state
  // No need to pass the RPC related struct to this struct, it only receives the RPC in this test
  // Set the election timeout to be an extremly large value so that it preserves to be the follower
  auto raft_config = RaftConfig{id, {}, storage, reserve_storage, 10000, 10000, nullptr};
  auto raft_stat = RaftState::NewRaftState(raft_config);
  raft_stat->SetRole(kFollower);
  raft_stat->SetCurrentTerm(1);
  raft_stat->SetCommitIndex(0);
  rpc_server->setState(raft_stat);

  rpc_server->Start();

  std::cout << "[Input to exit]:";
  char c;
  std::cin >> c;

  std::filesystem::remove(GetRaftLogName(id));
  std::filesystem::remove(GetRaftReserveLogName(id));
}

std::shared_ptr<RaftState> ConstructRaftLeader(raft_node_id_t id, const NodesConfig &configs) {
  std::unordered_map<raft_node_id_t, rpc::RpcClient *> rpc_clients;
  for (const auto &[id, net] : configs) {
    rpc_clients.insert({id, new rpc::RCFRpcClient(net, id)});
  }

  Storage *storage = FileStorage::Open(GetRaftLogName(id));
  Storage *reserve_storage = FileStorage::Open(GetRaftReserveLogName(id));

  std::shared_ptr<RaftState> ret;
  auto raft_config = RaftConfig{id, rpc_clients, storage, reserve_storage, 10000, 10000, nullptr};
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

void RunRaftLeader(raft_node_id_t id, const NodesConfig &configs, int data_size, int propose_cnt) {
  auto leader = ConstructRaftLeader(id, configs);
  RCF::sleepMs(1000);
  CommitLatencyRecorder recorder;
  for (int i = 1; i <= propose_cnt; ++i) {
    auto data = GenerateRandomSlice(data_size, data_size);

    // First set the liveness number to be N
    leader->SetLivenessNumber(7);

    auto start = util::NowTime();
    auto pr = leader->Propose(CommandData{0, data});
    assert(pr.is_leader == true);

    // Set the Liveness number to be N - 1 so that the next round can send the reserved chunks
    leader->SetLivenessNumber(6);

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
  std::filesystem::remove(GetRaftReserveLogName(id));
}
}  // namespace raft

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
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  bool is_local = FLAGS_conf == "";
  auto data_size = ParseNum(FLAGS_size.c_str());
  data_size = ((data_size - 1) / chunk_cnt - 1) * chunk_cnt;
  auto configs = is_local ? raft::kLocalConfigs : raft::kCloudConfigs;
  assert(FLAGS_id != -1);
  if (FLAGS_is_leader) {
    raft::RunRaftLeader(FLAGS_id, configs, data_size, 1);
  } else {
    raft::RunRaftFollower(FLAGS_id, configs);
  }
  return 0;
}
