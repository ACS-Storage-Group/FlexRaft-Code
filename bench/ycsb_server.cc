#include <cstdlib>
#include <fstream>
#include <gflags/gflags.h>
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
  auto cluster_cfg = ParseConfigurationFile(FLAGS_conf);
  auto node_id = FLAGS_id;

  // Run the server
  auto node = kv::KvServiceNode::NewKvServiceNode(cluster_cfg, node_id);
  node->InitServiceNodeState();
  RCF::sleepMs(1000);
  node->StartServiceNode();

  std::cout << "[Print to exit]:" << std::endl;
  char c;
  std::cin >> c;

  // Disconnect the kv node
  node->Disconnect();
}
