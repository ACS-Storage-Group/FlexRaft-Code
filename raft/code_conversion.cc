#include "code_conversion.h"

#include <memory>
#include <utility>

#include "encoder.h"
#include "log_entry.h"
#include "raft_type.h"

namespace raft {

namespace CODE_CONVERSION_NAMESPACE {

int get_chunk_count(int k) {
  // The result is lcm (1, 2,...,k-1) * k
  std::vector<int> v(k - 1);
  std::iota(v.begin(), v.end(), 1);
  return util::lcm(v) * k;
}

raft_chunk_index_t convert_to_chunk_index(raft_chunk_id_t d, int r) {
  return std::make_pair(d / r, d % r);
}

raft_chunk_id_t convert_to_chunk_id(raft_chunk_index_t d, int r) { return d.first * r + d.second; }

Slice ChunkDistribution::ChunkVector::Serialize() { return Slice(); }

void ChunkDistribution::Redistribute(const std::vector<raft_node_id_t>& replenish_servers,
                                     const std::vector<raft_node_id_t>& fail_servers, int r) {
  auto replenish_server_cnt = replenish_servers.size();
  int distr_each_cnt = r / replenish_server_cnt;
  placement_.replenish_chunk_cnt_ = distr_each_cnt;
  // Redistribute the chunk of fail servers to replenish servers
  for (int i = 0; i < fail_servers.size(); ++i) {
    for (int j = 0; j < replenish_servers.size(); ++j) {
      auto s = replenish_servers[j];
      int left = j * distr_each_cnt, right = (j + 1) * distr_each_cnt;
      for (int k = left; k < right; ++k) {
        placement_.as_vec()[s].emplace_back(fail_servers[i], k);
      }
    }
  }
}

ChunkDistribution::ChunkPlacement ChunkDistribution::GeneratePlacement(std::vector<bool> is_alive,
                                                                       int F, int k, int r) {
  int live_count = 0;

  ChunkPlacement& ret = placement_;
  ret.clear();

  auto fail_servers = get_fail_servers(is_alive);
  live_count = is_alive.size() - fail_servers.size();

  // There are #replenish_server_cnt servers need to save the data temporarily
  int replenish_server_cnt = live_count - F;

  for (int i = 0; i < is_alive.size(); ++i) {
    ret.as_vec().emplace_back();
    if (is_alive[i]) {
      for (int j = 0; j < r; ++j) {
        ret.as_vec()[i].emplace_back(i, j);
      }
      if (ret.replenish_servers_.size() < replenish_server_cnt) {
        ret.replenish_servers_.push_back(i);
      } else {
        ret.parity_servers_.push_back(i);
      }
    }
  }

  Redistribute(ret.get_replenish_servers(), fail_servers, r);

  return ret;
}

void ChunkDistribution::PrepareOriginalChunks(const Slice& slice) {
  int total_chunk_num = r_ * k_;

  // Cut the data into a few chunks:
  // NOTE: We have a compulsory requirements that the data size must be aligned with
  // the chunk number.
  assert(slice.size() % total_chunk_num == 0);

  /// Encode the original data:
  Encoder encoder;
  std::vector<Slice> input_slices = slice.Shard(k_), output_slices;
  original_chunks_.clear();

  encoder.EncodeSlice(input_slices, k_, F_, output_slices);

  // Shard the fragments into chunks accordingly:
  for (int i = 0; i < k_ + F_; ++i) {
    auto s = i < k_ ? input_slices[i] : output_slices[i - k_];
    auto shard = s.Shard(r_);
    original_chunks_.insert(original_chunks_.end(), shard.begin(), shard.end());
  }
}

void ChunkDistribution::EncodeForPlacement(const Slice& slice) {
  // First prepare for the original data chunks
  PrepareOriginalChunks(slice);

  // Assign the original chunks according to placement:
  for (int i = 0; i < placement_.as_vec().size(); ++i) {
    auto& idx_vec = placement_.as_vec()[i];
    for (const auto& idx : idx_vec) {
      chunks_map_[i].AddChunk(idx, org_chunk_at(idx));
    }
  }

  /// Do the second-phase encoding accordingly:
  // Second-phase encoding is not needed
  if (placement_.replenish_server_num() == 0) {
    return;
  }

  // Then for each replenish server: generate the parity chunk for the replenished chunk
  Encoder encoder;
  auto redistr_cnt = placement_.replenish_chunk_cnt();
  for (int i = 0; i < redistr_cnt; ++i) {
    std::vector<Slice> input_slices, output_parity;
    for (const auto& node : placement_.get_replenish_servers()) {
      auto chunk_idx = placement_.as_vec()[node][i + k_];
      input_slices.emplace_back(org_chunk_at(chunk_idx));
    }
    // Do the encoding:
    encoder.EncodeSlice(input_slices, placement_.replenish_server_num(), F_, output_parity);

    // Then distribute the generated parity chunk to live servers:
    for (int i = 0; i < placement_.parity_server_num(); ++i) {
      // TODO: Set a proper chunk index for this paraity
      chunks_map_[i].as_vec().emplace_back(raft_chunk_index_t{0, 0}, output_parity[i]);
    }
  }
}

};  // namespace CODE_CONVERSION_NAMESPACE

};  // namespace raft
