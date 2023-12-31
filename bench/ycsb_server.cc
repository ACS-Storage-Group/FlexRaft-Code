#include <gflags/gflags.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include "config.h"
#include "kv_node.h"
#include "rpc.h"
#include "util.h"

DEFINE_string(conf, "", "The position of cluster configuration file");
DEFINE_int32(id, -1, "The node id in the cluster");

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // read configuration from existing files
  if (FLAGS_conf.empty() || FLAGS_id == -1) {
    printf("Invalid Argument: conf = %s, id = %d\n", FLAGS_conf.c_str(), FLAGS_id);
    return 1;
  }
  auto cluster_cfg = ParseConfigurationFile(FLAGS_conf);
  auto node_id = FLAGS_id;

  // Run the server
  auto node = kv::KvServiceNode::NewKvServiceNode(cluster_cfg, node_id);
  node->InitServiceNodeState();
  RCF::sleepMs(1000);
  node->StartServiceNode();

  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }

  std::cout << "[Print to exit]:" << std::endl;
  char c;
  std::cin >> c;

  // Disconnect the kv node
  node->Disconnect();
}
