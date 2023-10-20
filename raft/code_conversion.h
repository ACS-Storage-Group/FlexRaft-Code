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
#include "raft_type.h"
#include "subchunk.h"
#include "util.h"

namespace raft {

namespace CODE_CONVERSION_NAMESPACE {

using DecodeInput = std::pair<Slice, SubChunkVector>;

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
  void EncodeForPlacement(const Slice& slice, const std::vector<bool>& live_vec,
                          StaticEncoder* static_encoder);

  // Encode based on current liveness vector 
  void Encode(const Slice& slice, const std::vector<bool>& live_vec, StaticEncoder* static_encoder);

  // Decode to recover the original data
  bool Decode(const std::map<raft_node_id_t, DecodeInput>& input, Slice* slice);

  // Adjust to the new liveness vector
  void AdjustNewLivenessVector(const std::vector<bool>& live_vec);

  bool DecodeCollectedChunkVec(const std::map<raft_node_id_t, ChunkVector>& input, Slice* slice);

  void AdjustChunkDistribution(const std::vector<bool>& live_vec);

  const auto& GetLiveVecForCurrDistribution() const { return live_vec_; }
  void SetLiveVecForCurrDistribution(const std::vector<bool>& live_vec) { live_vec_ = live_vec; }

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

  // If an original chunk corresponded to a specific node has been received by this node,
  // set the corresponding vector bit to be true.
  void UpdateOrgChunkResponseInfo(raft_node_id_t node_id, bool t);

  // Return true if the enqueried server has received the original chunk.
  bool HasReceivedOrgChunk(raft_node_id_t node_id) const {
    return org_chunk_response_[node_id] == true;
  }

  Slice GetAssignedChunk(raft_node_id_t node_id) {
    if (original_chunks_.count(node_id) == 0) {
      return Slice();
    }
    return original_chunks_[node_id];
  }

  SubChunkVector GetAssignedSubChunkVector(raft_node_id_t node_id) {
    if (subchunks_.count(node_id) == 0) {
      return SubChunkVector();
    }
    return subchunks_[node_id];
  }

 private:
  // Adjust the chunk placement according to the new ChunkDistribution.
  // Note that this function would clear all slice that are allocated in last round.
  void AdjustChunkDistribution(const ChunkDistribution& cd);

  // Prepare the original chunks and write them into the org_chunks_ attribute
  void PrepareOriginalChunks(const Slice& slice, StaticEncoder* static_encoder);

  // Encode the slice using (k, F) parameter and record the results
  void EncodeOriginalSlice(const Slice& slice, StaticEncoder* static_encoder);

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
  std::map<raft_node_id_t, ChunkVector> node_2_org_chunks_;
  std::map<raft_node_id_t, ChunkVector> node_2_reserved_chunks_;
  std::vector<Slice> org_chunks_[kMaxNodeNum];
  // A response vector indicate whether a node has received the original chunks
  bool org_chunk_response_[kMaxNodeNum] = {false};

  // The liveness vector when generating chunk distribution or adjusting
  std::vector<bool> live_vec_;

  // The original chunks assigned to each node
  std::map<raft_node_id_t, Slice> original_chunks_;
  // Subchunks preserved for failed servers
  std::map<raft_node_id_t, SubChunkVector> subchunks_;
  // The encoded chunks for the first encoding process
  std::vector<Slice> encoded_chunks_;
};
};  // namespace CODE_CONVERSION_NAMESPACE

};  // namespace raft
