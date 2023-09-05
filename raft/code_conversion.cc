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
  return raft_chunk_index_t(d / r, d % r);
}

raft_chunk_id_t convert_to_chunk_id(raft_chunk_index_t d, int r) {
  return d.node_id * r + d.chunk_id;
}

Slice ChunkDistribution::ChunkVector::Serialize() {
  // Calculate the size of the serialized data
  // auto hdr_sz_for_each = sizeof(raft_chunk_index_t) + sizeof(uint32_t) * 2;
  // auto hdr_sz = sizeof(uint32_t) + hdr_sz_for_each * chunks_.size();
  // auto alloc_sz = hdr_sz;
  // // for (const auto& [idx, s] : chunks_) {
  // //   alloc_sz += s.size();
  // // }

  // auto d = new char[alloc_sz];

  // // Serialize the header part and data part for each contained data
  // int h_offset = sizeof(uint32_t), d_offset = hdr_sz;
  // // for (const auto& [idx, s] : chunks_) {
  // //   *(uint32_t*)(d + h_offset) = idx.node_id;
  // //   *(uint32_t*)(d + h_offset + sizeof(uint32_t)) = idx.chunk_id;
  // //   *(uint32_t*)(d + h_offset + sizeof(uint32_t) * 2) = d_offset;
  // //   *(uint32_t*)(d + h_offset + sizeof(uint32_t) * 3) = s.size();

  // //   std::memcpy(d + d_offset, s.data(), s.size());
  // //   d_offset += s.size();
  // //   h_offset += sizeof(uint32_t) * 4;
  // // }

  // *(uint32_t*)d = chunks_.size();

  // return Slice(d, alloc_sz);
  return Slice();
}

bool ChunkDistribution::ChunkVector::Deserialize(const Slice& s) {
  // this->chunks_.clear();

  // // First get the number of chunk within this slice
  // auto d = s.data();
  // uint32_t chunk_count = *(uint32_t*)d;

  // // read each chunk one by one
  // uint32_t h_off = sizeof(uint32_t);
  // for (uint32_t i = 0; i < chunk_count; ++i) {
  //   auto idx1 = *(uint16_t*)d, idx2 = *(uint16_t*)(d + sizeof(uint32_t));
  //   auto d_off = *(uint32_t*)(d + sizeof(uint32_t) * 2);
  //   auto sz = *(uint32_t*)(d + sizeof(uint32_t) * 3);

  //   // Corrupted data
  //   if (d_off >= s.size() || d_off + sz >= s.size()) {
  //     return false;
  //   }

  //   // chunks_.emplace_back(raft_chunk_index_t{idx1, idx2}, Slice(d + d_off, sz));
  //   h_off += sizeof(uint32_t) * 4;
  // }
  return true;
}

void ChunkDistribution::Redistribute(const std::vector<raft_node_id_t>& replenish_servers,
                                     const std::vector<raft_node_id_t>& fail_servers, int r) {
  // Redistribute the chunk of fail servers to replenish servers
  int distr_each_cnt = placement_.replenish_chunk_cnt_;
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
  int replenish_server_cnt = fail_servers.size() > 0 ? live_count - F : 0;

  for (int i = 0; i < is_alive.size(); ++i) {
    ret.as_vec().emplace_back();
    if (is_alive[i]) {
      for (int j = 0; j < r; ++j) {
        ret.as_vec()[i].emplace_back(i, j);
      }

      if (replenish_server_cnt) {
        if (ret.get_replenish_servers().size() < replenish_server_cnt) {
          ret.replenish_servers_.push_back(i);
        } else {
          ret.parity_servers_.push_back(i);
        }
      }
    }
  }

  if (replenish_server_cnt > 0) {
    ret.replenish_chunk_cnt_ = r / replenish_server_cnt;
  } else {
    ret.replenish_chunk_cnt_ = 0;
  }

  if (replenish_server_cnt > 0) {
    Redistribute(ret.get_replenish_servers(), fail_servers, r);
  }

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

  // Assign the original chunks according to the placement:
  for (int i = 0; i < placement_.as_vec().size(); ++i) {
    auto& idx_vec = placement_.as_vec()[i];
    for (int j = 0; j < std::min(r_, (int)idx_vec.size()); ++j) {
      chunks_map_[i].AddChunk(idx_vec[j], raft_chunk_index_t::InvalidChunkIndex(),
                              org_chunk_at(idx_vec[j]));
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
  auto replenish_server_sets = placement_.get_replenish_servers();
  for (int i = 0; i < redistr_cnt; ++i) {  // For each replenish chunk group
    std::vector<Slice> input_slices, output_parity;

    int fail_server_id = -1;

    // Prepare the input
    for (int j = 0; j < replenish_server_sets.size(); ++j) {
      auto node = replenish_server_sets[j];
      auto chunk_idx = placement_.as_vec()[node][i + r_];
      input_slices.emplace_back(org_chunk_at(chunk_idx));
      chunks_map_[node].AddChunk(chunk_idx, raft_chunk_index_t(j, 0), org_chunk_at(chunk_idx));
      fail_server_id = chunk_idx.node_id;
    }


    // Do the encoding:
    encoder.EncodeSlice(input_slices, placement_.replenish_server_num(), F_, output_parity);

    // Then distribute the generated parity chunk to live servers:
    for (int i = 0; i < placement_.parity_server_num(); ++i) {
      // TODO: Set a proper chunk index for this paraity
      auto s = placement_.get_parity_servers()[i];
      chunks_map_[s].AddChunk(raft_chunk_index_t(fail_server_id, -1),
                              raft_chunk_index_t(i + replenish_server_sets.size(), 0),
                              output_parity[i]);
    }
  }
}

// Decode input ChunkVectors to get the original data, write it into the resultant slice
bool ChunkDistribution::Decode(std::unordered_map<raft_node_id_t, ChunkVector>& chunk_vecs,
                               Slice* slice) {
  // First, check if the replenished chunks can be recovered:
  int chunk_cnt_each = 0;
  std::set<raft_node_id_t> fail_server_sets;
  for (auto& [id, chunk_vec] : chunk_vecs) {
    chunk_cnt_each = std::max(chunk_cnt_each, (int)chunk_vec.as_vec().size());
    for (int i = r_; i < chunk_vec.as_vec().size(); ++i) {
      fail_server_sets.emplace(chunk_vec.as_vec().at(i).idx1.node_id);
    }
  }

  assert(chunk_cnt_each > 0);

  // Recover the information related to the failure case:
  // Let x = # of replenish servers
  //     y = # of replenish chunk count saved by each replenish server
  //     z = # of fail servers
  // Then the following equations hold:
  //   y * z = chunk_cnt_each - r_
  //       x = N - z - F
  //   y * x = r_
  //
  // The solutions would be:
  //   x = (N - F) * r / C
  //   y = C / (N - F)
  //   z = (N - F) * (C - r) / C
  int N = 2 * F_ + 1;
  auto replenish_server_cnt = (N - F_) * r_ / chunk_cnt_each;
  auto replenish_chunk_cnt_each = chunk_cnt_each / (N - F_);
  auto fail_server_cnt = (N - F_) * (chunk_cnt_each - r_) / chunk_cnt_each;

  // First try recovering the data that comes from the second encoding phase:
  Encoder decoder;
  std::vector<Slice> snd_phase_decode_output;
  for (int i = r_; i < chunk_cnt_each; ++i) {
    Encoder::EncodingResults decode_input;
    Slice decode_output;
    for (auto [id, chunk_vec] : chunk_vecs) {
      if (chunk_vec.as_vec().size() == chunk_cnt_each) {
        auto chunk = chunk_vec.as_vec().at(i);
        auto idx = chunk.idx2.node_id;
        decode_input.emplace(idx, chunk.data);
      }
    }
    decoder.DecodeSlice(decode_input, replenish_server_cnt, F_, &decode_output);
    snd_phase_decode_output.emplace_back(decode_output);
  }

  // Now construct the replenish data fragments:
  std::vector<Slice> replenish_fragments;
  int off = 0;
  for (int f = 0; f < fail_server_cnt; ++f) {
    std::vector<Slice> replenish_fragments_input;
    for (int i = 0; i < replenish_server_cnt; ++i) {
      for (int j = 0; j < replenish_chunk_cnt_each; ++j) {
        replenish_fragments_input.emplace_back(
            snd_phase_decode_output[j + off].Shard(replenish_chunk_cnt_each + F_).at(i));
      }
    }
    off += replenish_chunk_cnt_each;
    auto replenish_fragment = Slice::Combine(replenish_fragments_input);
    replenish_fragments.emplace_back(replenish_fragment);
  }

  Encoder::EncodingResults final_decode_input;
  Encoder final_decoder;
  auto iter = fail_server_sets.begin();
  for (int i = 0; i < replenish_fragments.size(); ++i) {
    final_decode_input.emplace(*iter, replenish_fragments[i]);
  }

  assert(fail_server_sets.size() == fail_server_cnt);

  // Now construct the original data
  for (auto &[id, chunk_vec] : chunk_vecs) {
    if (chunk_vec.as_vec().size() == 0) {
      continue;
    }

    if (final_decode_input.size() >= k_) {
      break;
    }

    std::vector<Slice> vec;
    for (int i = 0; i < r_; ++i) {
      vec.emplace_back(chunk_vec.as_vec()[i].data);
    }
    final_decode_input.emplace(id, Slice::Combine(vec));
  }

  return final_decoder.DecodeSlice(final_decode_input, k_, F_, slice);
}

};  // namespace CODE_CONVERSION_NAMESPACE

};  // namespace raft
