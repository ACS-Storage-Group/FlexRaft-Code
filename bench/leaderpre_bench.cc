#include <gflags/gflags.h>

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include "chunk.h"
#include "raft.h"
#include "raft_node.h"
#include "util.h"

DEFINE_string(conf, "", "The position of cluster configuration file");
DEFINE_int32(id, -1, "The node id in the cluster");
DEFINE_string(size, "", "The size for each proposal");
DEFINE_int32(write_num, 0, "The count for proposal");

using namespace raft;

auto ParseRaftConfigurationFile(const std::string &filename, int id) -> RaftNode::NodeConfig {
  std::ifstream cfg(filename);
  RaftNode::NodeConfig ret;
  std::string node_id, raft_rpc_addr, kv_rpc_addr, logname, dbname;
  while (cfg >> node_id >> raft_rpc_addr >> kv_rpc_addr >> logname >> dbname) {
    auto nid = std::stoi(node_id);
    ret.servers.insert_or_assign(nid, ParseNetAddress(raft_rpc_addr));
    if (id == nid) {
      ret.node_id_me = id;
      ret.rsm = nullptr;
      ret.storage_filename = logname;
    }
  }
  return ret;
}

CommandData ConstructCommandFromValue(int val, int write_sz) {
  auto data = new char[write_sz];
  *reinterpret_cast<int *>(data) = val;
  // For more strict test on encoding/decoding
  *reinterpret_cast<int *>(data + write_sz - 4) = val;
  return CommandData{0, Slice(data, write_sz)};
}

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // read configuration from existing files
  if (FLAGS_conf.empty() || FLAGS_id == -1) {
    printf("Invalid Argument: conf = %s, id = %d\n", FLAGS_conf.c_str(), FLAGS_id);
    return 1;
  }
  auto node_cfg = ParseRaftConfigurationFile(FLAGS_conf, FLAGS_id);
  auto node_id = FLAGS_id;

  // Run the server
  auto node = new RaftNode(node_cfg);
  node->Init();
  node->Start();

  std::this_thread::sleep_for(std::chrono::seconds(1));

  const int K = 4;
  const int chunk_cnt = CODE_CONVERSION_NAMESPACE::get_chunk_count(K);
  int write_sz = round_up(ParseCommandSize(FLAGS_size), chunk_cnt);

  // Wait for the leader to be proposed:
  if (node->IsLeader()) {
    printf("I'm Leader, start proposing\n");
    for (int i = 0; i < FLAGS_write_num; ++i) {
      auto cmd = ConstructCommandFromValue(i, write_sz);
      auto pr = node->Propose(cmd);
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    printf("Proposing is done\n");
  }

  std::cout << "[Print to exit]:" << std::endl;
  char c;
  std::cin >> c;

  // Disconnect the kv node
  node->Exit();
}