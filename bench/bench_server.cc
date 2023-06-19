#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include "config.h"
#include "kv_node.h"
#include "rpc.h"
#include "util.h"

int main(int argc, char* argv[]) {
  // read configuration from existing files
  if (argc < 3) {
    std::cerr << "[Error] Needs at least three input parameters: get " << argc
              << std::endl;
    return 0;
  }
  auto cluster_cfg = ParseConfigurationFile(std::string(argv[1]));
  auto node_id = std::stoi(std::string(argv[2]));

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
