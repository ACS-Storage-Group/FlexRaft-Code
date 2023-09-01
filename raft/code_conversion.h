#pragma once
#include <numeric>

#include "raft_type.h"
#include "util.h"

namespace raft {

// The concept is that we want to do a uniform indexing for all data chunks:
//   S0         S0          S2     |     S3          S4
// (0, 0)     (1, 0)      (2, 0)   |   (3, 0)      (4, 0)
// (0, 1)     (1, 1)      (2, 1)   |   (3, 1)      (4, 1)

using raft_chunk_id_t = uint32_t;

using raft_chunk_index_t = std::pair<raft_node_id_t, uint32_t>;

// Given the optimal encoding parameter, calculate the number of chunks
// for code conversion
int get_chunk_count(int k, int m);

// N: the maximum number of server in current cluster
// k, m: the optimal encoding parameter
raft_chunk_index_t convert_to_chunk_index(raft_chunk_id_t d, int r);
raft_chunk_id_t convert_to_chunk_id(raft_chunk_index_t d, int r);

// given the liveness status of the cluster, generate a placement plan
std::vector<std::vector<raft_chunk_index_t>> generate_placement(std::vector<bool> is_alive, int N,
                                                                int F, int k, int m, int r);

};  // namespace raft
