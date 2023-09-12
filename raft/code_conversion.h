#pragma once
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iterator>
#include <numeric>
#include <sstream>
#include <unordered_map>

#include "chunk.h"
#include "encoder.h"
#include "log_entry.h"
#include "raft_type.h"
#include "util.h"

namespace raft {

namespace CODE_CONVERSION_NAMESPACE {

// A managing class for a raft entry, it conatins functionalities for data management, such as
// encoding/decoding, generating chunk distribtion according to the liveness status of the
// cluster. In addition, it maintains some replication status for each follower
class CodeConversionManagement {
  static constexpr size_t kMaxNodeNum = 11;

 public:
  CodeConversionManagement() = default;
  CodeConversionManagement(int k, int F, int r) : k_(k), F_(F), r_(r) {}

  CodeConversionManagement(const CodeConversionManagement&) = delete;
  CodeConversionManagement& operator=(const CodeConversionManagement&) = delete;

  // Given the liveness vector that indicates the current liveness status of this cluster,
  // Calculate the corresponding encoding data placement for it.
  void EncodeForPlacement(const Slice& slice, const std::vector<bool>& live_vec);

  bool DecodeCollectedChunkVec(const std::map<raft_node_id_t, ChunkVector>& input, Slice* slice);

  void AdjustChunkDistribution(std::vector<bool>& live_vec);

  auto GetOriginalChunkVector(raft_node_id_t node_id) -> ChunkVector {
    if (node_2_org_chunks_.count(node_id)) {
      return node_2_org_chunks_[node_id];
    }
    return ChunkVector();
  }

  auto GetReservedChunkVector(raft_node_id_t node_id) -> ChunkVector {
    if (node_2_reserved_chunks_.count(node_id)) {
      return node_2_reserved_chunks_.at(node_id);
    }
    return ChunkVector();
  }

  // Adjust the chunk placement according to the new ChunkDistribution. 
  // Note that this function would clear all slice that are allocated in last round.
  void AdjustChunkDistribution(const ChunkDistribution& cd);

 private:
  // Prepare the original chunks and write them into the org_chunks_ attribute
  void PrepareOriginalChunks(const Slice& slice);

  // Assign the slices corresponds to the original chunks to each node
  void AssignOriginalChunksToNode(const ChunkDistribution& cd);

  // Assign the slices corresponds to the reserved chunks to each node
  void AssignReservedChunksToNode(const ChunkDistribution& cd);

  // Encode the reserved chunks and assign them to each node
  void EncodeReservedChunksAndAssignToNode(const ChunkDistribution& cd);

  // Recover reserved chunks first and return the results as a map of node_id -> resultant slice
  std::map<raft_node_id_t, Slice> RecoverReservedChunks(
      const std::map<raft_node_id_t, ChunkVector>& input);

  void add_original_chunks(raft_node_id_t node_id, ChunkIndex idx1, ChunkIndex idx2,
                           const Slice& slice) {
    assert(idx1.node_id == node_id);
    node_2_org_chunks_[node_id].AddChunk(idx1, idx2, slice);
  }

  void add_reserved_chunks(raft_node_id_t node_id, ChunkIndex idx1, ChunkIndex idx2,
                           const Slice& slice) {
    node_2_reserved_chunks_[node_id].AddChunk(idx1, idx2, slice);
  }

 private:
  int k_, F_, r_;
  std::unordered_map<raft_node_id_t, ChunkVector> node_2_org_chunks_, node_2_reserved_chunks_;
  std::vector<Slice> org_chunks_[kMaxNodeNum];
  // A response vector indicate whether a node has received the original chunks
  bool org_chunk_response_[kMaxNodeNum];
};
};  // namespace CODE_CONVERSION_NAMESPACE

};  // namespace raft
