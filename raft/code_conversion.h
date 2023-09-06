#pragma once
#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <iterator>
#include <numeric>
#include <sstream>
#include <unordered_map>

#include "encoder.h"
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

// using raft_chunk_index_t = std::pair<raft_node_id_t, uint32_t>;
typedef struct ChunkIndex {
  static constexpr int16_t kInvalidNum = (int16_t)-1;

  int16_t node_id = kInvalidNum;
  int16_t chunk_id = kInvalidNum;

  ChunkIndex(int16_t node_id, int16_t chunk_id) : node_id(node_id), chunk_id(chunk_id) {}

  ChunkIndex(const ChunkIndex&) = default;
  ChunkIndex& operator=(const ChunkIndex&) = default;

  bool Valid() { return node_id != kInvalidNum && chunk_id != kInvalidNum; }

  static ChunkIndex InvalidChunkIndex() { return ChunkIndex(kInvalidNum, kInvalidNum); }

  std::string ToString() const {
    std::stringstream ss;
    ss << "(" << std::setw(3) << node_id << "," << std::setw(3) << chunk_id << ")";
    return ss.str();
  }

  friend bool operator==(const ChunkIndex& left, const ChunkIndex& right) {
    return left.chunk_id == right.chunk_id && left.node_id == right.node_id;
  }

} raft_chunk_index_t;

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
  // A chunk may belong to different encoding group, for the original data,
  // its second encoding group member is invalid.
  struct Chunk {
    raft_chunk_index_t idx1;
    raft_chunk_index_t idx2;
    Slice data;

    Chunk(raft_chunk_index_t idx1, raft_chunk_index_t idx2, Slice d)
        : idx1(idx1), idx2(idx2), data(d) {}

    Chunk(const Chunk&) = default;
    Chunk& operator=(const Chunk&) = default;

    std::string ToString() const {
      std::stringstream ss;
      ss << "[" << idx1.ToString() << "," << idx2.ToString() << "]";
      return ss.str();
    }
  };

  // A ChunkVector is a vector of chunks which should be stored within a single server
  struct ChunkVector {
    std::vector<Chunk> chunks_;

    // Serialize this ChunkVector to a slice and return the data
    Slice Serialize();

    // Deserialize the ChunkVector from a slice
    bool Deserialize(const Slice& s);

    void AddChunk(raft_chunk_index_t idx1, raft_chunk_index_t idx2, Slice s) {
      chunks_.emplace_back(idx1, idx2, s);
    }

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

    const auto& At(int node, int id) {
      return placement_[node][id];
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

  // Decode input ChunkVectors to get the original data, write it into the resultant slice
  bool Decode(std::unordered_map<raft_node_id_t, ChunkVector>& chunks, Slice* slice);

  auto GetChunkVector(raft_node_id_t id) -> ChunkVector {
    if (chunks_map_.count(id) != 0) {
      return chunks_map_[id];
    }
    return ChunkVector{};
  }

  // Given the data chunks of each alive server, return the replenish fragments
  auto RecoverReplenishFragments(std::unordered_map<raft_node_id_t, ChunkVector>& chunks)
      -> std::map<raft_node_id_t, Slice>;

 private:
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

  // Assign the first r chunks to a specific raft node, according to the placement generated
  void AssignOriginalChunk(raft_node_id_t node, int r);

  void EncodeReplenishedChunk(Encoder* encoder, int row);

 private:
  int F_, k_, r_;

  ChunkPlacement placement_;            // The placement of chunks
  std::vector<Slice> original_chunks_;  // Original chunks sharded from the original data
  // Calculate the ChunkVector for each server
  std::unordered_map<raft_node_id_t, ChunkVector> chunks_map_;
};
};  // namespace CODE_CONVERSION_NAMESPACE

};  // namespace raft
