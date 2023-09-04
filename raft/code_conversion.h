#pragma once
#include <algorithm>
#include <numeric>
#include <unordered_map>

#include "log_entry.h"
#include "raft_type.h"
#include "util.h"

#define CODE_CONVERSION_NAMESPACE code_conversion

namespace raft {

// The concept is that we want to do a uniform indexing for all data chunks:
//   S0         S0          S2     |     S3          S4
// (0, 0)     (1, 0)      (2, 0)   |   (3, 0)      (4, 0)
// (0, 1)     (1, 1)      (2, 1)   |   (3, 1)      (4, 1)

using raft_chunk_id_t = uint32_t;

using raft_chunk_index_t = std::pair<raft_node_id_t, uint32_t>;

namespace CODE_CONVERSION_NAMESPACE {
// Given the optimal encoding parameter, calculate the number of chunks
// for code conversion
int get_chunk_count(int k);

// N: the maximum number of server in current cluster
// k, m: the optimal encoding parameter
raft_chunk_index_t convert_to_chunk_index(raft_chunk_id_t d, int r);
raft_chunk_id_t convert_to_chunk_id(raft_chunk_index_t d, int r);

class ChunkDistribution {
 public:
  // A ChunkVector is a vector of chunks which should be stored within a single server
  struct ChunkVector {
    std::vector<std::pair<raft_chunk_index_t, Slice>> chunks_;

    // Serialize this ChunkVector to a slice and return the data
    Slice Serialize();

    // Deserialize from a slice
    bool Deserialize(const Slice& s);

    void AddChunk(raft_chunk_index_t idx, Slice s) { chunks_.emplace_back(idx, s); }

    auto& as_vec() { return chunks_; }
  };

  struct ChunkPlacement {
    std::vector<std::vector<raft_chunk_index_t>> placement_;
    std::vector<raft_node_id_t> replenish_servers_;
    std::vector<raft_node_id_t> parity_servers_;
    int replenish_chunk_cnt_;

    int replenish_server_num() const { return replenish_servers_.size(); }
    int parity_server_num() const { return parity_servers_.size(); }
    int replenish_chunk_cnt() const { return replenish_chunk_cnt_; }

    const auto& get_replenish_servers() const { return replenish_servers_; }
    const auto& get_parity_servers() const { return parity_servers_; }

    auto& as_vec() { return placement_; }

    void clear() {
      placement_.clear();
      replenish_servers_.clear();
      parity_servers_.clear();
    }
  };

  ChunkDistribution() = default;
  ChunkDistribution(int F, int k, int r) : F_(F), k_(k), r_(r) {}

  ChunkDistribution& operator=(const ChunkDistribution&) = delete;
  ChunkDistribution(const ChunkDistribution&) = delete;

  // This function generates the replenish_servers_ and second_phase_parity_servers_ as well
  ChunkPlacement GeneratePlacement(std::vector<bool> liveness, int F, int k, int r);
  // Simple wrapper that maintains some internal states
  ChunkPlacement GeneratePlacement(std::vector<bool> liveness) {
    return GeneratePlacement(liveness, F_, k_, r_);
  }

  // Encode the slice according to the given placement
  void EncodeForPlacement(const Slice& slice);

  void PrepareOriginalChunks(const Slice& slice);

  Slice org_chunk_at(raft_chunk_index_t idx) {
    return original_chunks_[convert_to_chunk_id(idx, r_)];
  }

  void Redistribute(const std::vector<raft_node_id_t>& replenish_servers,
                    const std::vector<raft_node_id_t>& fail_servers, int r);

  std::vector<raft_node_id_t> get_fail_servers(const std::vector<bool>& liveness) {
    std::vector<raft_node_id_t> ret;
    for (int i = 0; i < liveness.size(); ++i) {
      if (!liveness[i]) ret.push_back(i);
    }
    return ret;
  }

 private:
  ChunkPlacement placement_;  // The placement of chunks
  int F_, k_, r_;
  std::vector<Slice> original_chunks_;

  std::unordered_map<raft_node_id_t, ChunkVector> chunks_map_;
};
};  // namespace CODE_CONVERSION_NAMESPACE

};  // namespace raft
