#include "code_conversion.h"

#include <memory>
#include <utility>

#include "raft_type.h"

namespace raft {
inline int get_chunk_count(int k) {
  // The result is lcm (1, 2,...,k-1) * k
  std::vector<int> v(k - 1);
  std::iota(v.begin(), v.end(), 1);
  return util::lcm(v) * k;
}

inline raft_chunk_index_t convert_to_chunk_index(raft_chunk_id_t d, int r) {
  return std::make_pair(d / r, d % r);
}

inline raft_chunk_id_t convert_to_chunk_id(raft_chunk_index_t d, int r) {
  return d.first * r + d.second;
}

// given the liveness status of the cluster, generate a placement plan
inline std::vector<std::vector<raft_chunk_index_t>> generate_placement(std::vector<bool> is_alive,
                                                                       int N, int F, int k, int m,
                                                                       int r) {
  int live_count = 0;

  std::vector<std::vector<raft_chunk_index_t>> ret;
  std::vector<raft_node_id_t> fail_servers, replenish_servers;

  for (int i = 0; i < is_alive.size(); ++i) {
    live_count += is_alive[i];
    if (!is_alive[i]) {
      fail_servers.emplace_back(i);
    }
  }

  // There are #replenish_server_cnt servers need to save the data temporarily
  int replenish_server_cnt = live_count - F;

  for (int i = 0; i < is_alive.size(); ++i) {
    ret.emplace_back();
    if (is_alive[i]) {
      for (int j = 0; j < r; ++j) {
        ret[i].emplace_back(i, j);
      }
      if (replenish_servers.size() < replenish_server_cnt) {
        replenish_servers.push_back(i);
      }
    }
  }

  // Redistribute the chunk of fail servers to replenish servers
  for (int i = 0; i < fail_servers.size(); ++i) {
    int distr_each_cnt = r / replenish_server_cnt;
    for (int j = 0; j < replenish_servers.size(); ++j) {
      auto s = replenish_servers[j];
      int l = j * distr_each_cnt, r = (j + 1) * distr_each_cnt;
      for (int k = l; k < r; ++k) {
        ret[s].emplace_back(fail_servers[i], k);
      }
    }
  }

  return ret;
}

}  // namespace raft
