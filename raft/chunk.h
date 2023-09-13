#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iterator>
#include <numeric>
#include <sstream>
#include <unordered_map>

#include "raft_type.h"
#include "util.h"

namespace raft {

#define CODE_CONVERSION_NAMESPACE code_conversion

using raft_chunk_id_t = uint32_t;

// using raft_chunk_index_t = std::pair<raft_node_id_t, uint32_t>;
struct ChunkIndex {
  static constexpr int16_t kInvalidNum = (int16_t)-1;

  int16_t node_id = kInvalidNum;
  int16_t chunk_id = kInvalidNum;

  ChunkIndex(int16_t node_id, int16_t chunk_id) : node_id(node_id), chunk_id(chunk_id) {}
  ChunkIndex() = default;

  ChunkIndex(const ChunkIndex&) = default;
  ChunkIndex& operator=(const ChunkIndex&) = default;

  bool Valid() const { return node_id != kInvalidNum && chunk_id != kInvalidNum; }

  static ChunkIndex InvalidChunkIndex() { return ChunkIndex(kInvalidNum, kInvalidNum); }

  std::string ToString() const {
    std::stringstream ss;
    ss << "(" << std::setw(3) << node_id << "," << std::setw(3) << chunk_id << ")";
    return ss.str();
  }

  friend bool operator==(const ChunkIndex& left, const ChunkIndex& right) {
    return left.chunk_id == right.chunk_id && left.node_id == right.node_id;
  }

  char* Serialize(char* d) const {
    *(int16_t*)d = node_id;
    *(int16_t*)(d + sizeof(int16_t)) = chunk_id;
    return d + sizeof(int16_t) * 2;
  }

  const char* Deserialize(const char* s) {
    node_id = *(int16_t*)s;
    chunk_id = *(int16_t*)(s + sizeof(int16_t));
    return s + sizeof(int16_t) * 2;
  }
};

namespace CODE_CONVERSION_NAMESPACE {
// Given the optimal encoding parameter, calculate the number of chunks
// for code conversion
int get_chunk_count(int k);

// N: the maximum number of server in current cluster
// k, m: the optimal encoding parameter
ChunkIndex convert_to_chunk_index(raft_chunk_id_t d, int r);
raft_chunk_id_t convert_to_chunk_id(ChunkIndex d, int r);

// Chunk is the atomic data unit used for data distribution
struct Chunk {
  ChunkIndex idx1_, idx2_;
  Slice data_;

  Chunk(ChunkIndex idx1, ChunkIndex idx2, Slice d) : idx1_(idx1), idx2_(idx2), data_(d) {}

  Chunk() = default;
  Chunk(const Chunk&) = default;
  Chunk& operator=(const Chunk&) = default;
  ~Chunk() = default;

  char* data() const { return data_.data(); }
  size_t size() const { return data_.size(); }
  Slice slice() const { return data_; }

  ChunkIndex Index1() const { return idx1_; }
  ChunkIndex Index2() const { return idx2_; }

  std::string ToString() const {
    std::stringstream ss;
    ss << "[" << idx1_.ToString() << "," << idx2_.ToString() << "]";
    return ss.str();
  }

  bool operator==(const Chunk& r) const {
    return this->idx1_ == r.idx1_ && this->idx2_ == r.idx2_ && this->slice().compare(r.data_) == 0;
  }

  bool operator!=(const Chunk& r) const { return !operator==(r); }
};

// A ChunkVector is a vector of chunks which should be stored within a single server
struct ChunkVector {
  std::vector<Chunk> chunks_;

  ChunkVector() = default;
  ChunkVector(const ChunkVector&) = default;
  ChunkVector& operator=(const ChunkVector&) = default;
  ~ChunkVector() = default;

  // Append a new chunk to current ChunkVector
  void AddChunk(ChunkIndex idx1, ChunkIndex idx2, Slice s) { chunks_.emplace_back(idx1, idx2, s); }

  // Serialize this ChunkVector to a slice and return the data
  Slice Serialize();

  // Serialize the size to a start address and return the last position after serialization
  char* Serialize(char* d);

  // Return the size needed for serializing this ChunkVector
  size_t SizeForSerialization() const;

  // Return the size needed for serializing the header
  size_t HeaderSizeForSerialization() const;

  // Deserialize the ChunkVector from a slice
  bool Deserialize(const Slice& s);

  const char* Deserialize(const char* s);

  ChunkVector SubVec(int l, int r) const {
    ChunkVector ret;
    for (int i = l; i < std::min(r, (int)chunks_.size()); ++i) {
      ret.chunks_.emplace_back(chunks_[i]);
    }
    return ret;
  }

  // Append another ChunkVector to current chunk vector
  void Concatenate(const ChunkVector& cv) {
    for (const auto& v : cv.chunks_) {
      chunks_.emplace_back(v);
    }
  }

  // Return the chunk of located at specific index
  Chunk chunk_at(int idx) const { return chunks_[idx]; }

  // Return the size of this chunk vector
  size_t size() const { return chunks_.size(); }

  // return the whole vector
  const auto& as_vec() const { return chunks_; }

  auto as_slice_vec() -> std::vector<Slice> const {
    std::vector<Slice> ret;
    for (const auto& c : chunks_) {
      ret.emplace_back(c.slice());
    }
    return ret;
  }
};

// ChunkPlacementInfo represents the basic liveness states of a server
struct ChunkPlacementInfo {
  // fail_servers_: servers that have been identified as failed
  // reservation_servers_: servers that are selected for temporarily reserving chunks of failed
  // servers
  // parity_servers_: servers that are selected for store parity chunks of reserved chunks
  std::vector<raft_node_id_t> fail_servers_, reservation_servers_, parity_servers_;
  int k_, F_, r_;
  // int replenish_chunk_cnt_;

  ChunkPlacementInfo(int k, int F) : k_(k), F_(F) {}
  ChunkPlacementInfo() = default;
  ChunkPlacementInfo(const ChunkPlacementInfo&) = default;
  ChunkPlacementInfo& operator=(const ChunkPlacementInfo&) = default;

  // Generate the basic placement information
  void GenerateInfoFromLivenessVector(const std::vector<bool>& liveness);

  const auto& get_fail_servers() const { return fail_servers_; }
  const auto& get_reservation_servers() const { return reservation_servers_; }
  const auto& get_parity_servers() const { return parity_servers_; }

  void AddFailServer(raft_node_id_t id) { fail_servers_.emplace_back(id); }
  void AddReservationServer(raft_node_id_t id) { reservation_servers_.emplace_back(id); }
  void AddParityServer(raft_node_id_t id) { parity_servers_.emplace_back(id); }

  int get_fail_servers_num() const { return fail_servers_.size(); }
  int get_reservation_server_num() const { return reservation_servers_.size(); }
  int get_parity_servers_num() const { return parity_servers_.size(); }

  // int replenish_chunk_cnt() const { return replenish_chunk_cnt_; }

  void clear() {
    fail_servers_.clear();
    reservation_servers_.clear();
    parity_servers_.clear();
  }
};

struct ChunkDistribution {
  static constexpr size_t kMaxNodeNum = 11;

  std::vector<ChunkIndex> original_chunks_[kMaxNodeNum];
  std::vector<ChunkIndex> reservation_chunks_[kMaxNodeNum];
  int k_, F_, r_;
  ChunkPlacementInfo c_info_;
  int reserved_chunk_cnt_each_ = 0;

  ChunkDistribution() = default;
  ChunkDistribution(int k, int F, int r) : k_(k), F_(F), r_(r), c_info_(k, F) {}

  ChunkDistribution(const ChunkDistribution&) = default;
  ChunkDistribution& operator=(const ChunkDistribution&) = default;

  // Generate distribution from liveness status:
  void GenerateChunkDistribution(const std::vector<bool>& liveness);

  int GetReservedChunkCountForEach() const { return reserved_chunk_cnt_each_; }

  void AddOriginalChunkIndex(raft_node_id_t node, ChunkIndex cidx) {
    original_chunks_[node].emplace_back(cidx);
  }

  void AddReservationChunkIndex(raft_node_id_t node, ChunkIndex cidx) {
    reservation_chunks_[node].emplace_back(cidx);
  }

  void clear() {
    for (int i = 0; i < kMaxNodeNum; ++i) {
      original_chunks_[i].clear();
      reservation_chunks_[i].clear();
    }
    c_info_.clear();
  }

  const auto& GetAssignedOrgChunks(raft_node_id_t node_id) const {
    return original_chunks_[node_id];
  }

  const auto& GetAssignedReserveChunks(raft_node_id_t node_id) const {
    return reservation_chunks_[node_id];
  }

  const auto& GetPlacementInfo() const { return c_info_; }

 private:
  void GenerateDistributionForReservedChunks(const ChunkPlacementInfo& p_info) {
    if (p_info.get_reservation_server_num() == 0) {
      return;
    }
    auto reserved_chunk_each = r_ / p_info.get_reservation_server_num();
    auto reserved_servers = p_info.get_reservation_servers();
    for (const auto& f_server : p_info.get_fail_servers()) {
      for (int i = 0; i < reserved_servers.size(); ++i) {
        int l = reserved_chunk_each * i, r = reserved_chunk_each * (i + 1);
        for (int cidx = l; cidx < r; ++cidx) {
          AddReservationChunkIndex(reserved_servers[i], ChunkIndex(f_server, cidx));
        }
      }
    }
    reserved_chunk_cnt_each_ = reserved_chunk_each * p_info.get_fail_servers_num();
  }
};

};  // namespace CODE_CONVERSION_NAMESPACE
};  // namespace raft