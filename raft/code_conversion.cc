#include "code_conversion.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>

#include "chunk.h"
#include "encoder.h"
#include "iter.h"
#include "log_entry.h"
#include "raft_type.h"
#include "util.h"

// namespace raft {

// void ChunkDistribution::PrepareOriginalChunks(const Slice& slice) {
//   int total_chunk_num = r_ * k_;

//   // Cut the data into a few chunks:
//   // NOTE: We have a compulsory requirements that the data size must be aligned with
//   // the chunk number.
//   assert(slice.size() % total_chunk_num == 0);

//   /// Encode the original data:
//   Encoder encoder;
//   std::vector<Slice> input_slices = slice.Shard(k_), output_slices;
//   original_chunks_.clear();

//   encoder.EncodeSlice(input_slices, k_, F_, output_slices);

//   // Shard the fragments into chunks accordingly:
//   for (int i = 0; i < k_ + F_; ++i) {
//     // Note that output_slices also contains the original data slice
//     auto s = output_slices[i];
//     auto shard = s.Shard(r_);
//     original_chunks_.insert(original_chunks_.end(), shard.begin(), shard.end());
//   }
// }

// void ChunkDistribution::AssignOriginalChunk(raft_node_id_t node, int r) {
//   auto& idx_vec = placement_.as_vec()[node];
//   int len = std::min(r, (int)idx_vec.size());
//   std::for_each(idx_vec.begin(), idx_vec.begin() + len, [=](const auto& idx) {
//     this->chunks_map_[node].AddChunk(idx, ChunkIndex::InvalidChunkIndex(), org_chunk_at(idx));
//   });
// }

// void ChunkDistribution::EncodeReplenishedChunk(Encoder* encoder, int row) {
//   auto replenish_server_sets = placement_.get_replenish_servers();
//   std::vector<Slice> encode_input, encode_output;
//   // Prepare the input
//   int encode_group_idx = 0, fail_server_id = -1;
//   for (const auto& node_id : replenish_server_sets) {
//     auto chunk_idx = placement_.At(node_id, row);
//     encode_input.emplace_back(org_chunk_at(chunk_idx));
//     chunks_map_[node_id].AddChunk(chunk_idx, ChunkIndex(encode_group_idx, 0),
//                                   org_chunk_at(chunk_idx));
//     ++encode_group_idx;
//     fail_server_id = chunk_idx.node_id;
//   }

//   // Do the encoding:
//   auto encode_k = placement_.replenish_server_num(), encode_m = F_;
//   encoder->EncodeSlice(encode_input, encode_k, encode_m, encode_output);

//   // Assign encode output to the related server:
//   for (int i = 0; i < placement_.parity_server_num(); ++i) {
//     // TODO: Set a proper chunk index for this paraity
//     auto s = placement_.get_parity_servers()[i];
//     // Note that output_parity includes the original data slice
//     chunks_map_[s].AddChunk(ChunkIndex(fail_server_id, -1),
//                             ChunkIndex(i + replenish_server_sets.size(), 0),
//                             encode_output[i + encode_k]);
//   }
// }

// void ChunkDistribution::EncodeForPlacement(const Slice& slice) {
//   // First prepare for the original data chunks
//   PrepareOriginalChunks(slice);

//   // Assign the original chunks according to the placement:
//   for (int i = 0; i < placement_.as_vec().size(); ++i) {
//     AssignOriginalChunk(i, r_);
//   }

//   // Do the second-phase encoding accordingly: return directly if there is no
//   // need for replenish server
//   if (placement_.replenish_server_num() != 0) {
//     Encoder encoder;
//     auto replenish_chunk_total = placement_.fail_server_num() * placement_.replenish_chunk_cnt();
//     for (int row = r_; row < r_ + replenish_chunk_total; ++row) {
//       EncodeReplenishedChunk(&encoder, row);
//     }
//   }
// }

// std::map<raft_node_id_t, Slice> ChunkDistribution::RecoverReplenishFragments(
//     std::unordered_map<raft_node_id_t, ChunkVector>& chunk_vecs) {
//   int chunk_cnt_each = 0;
//   std::set<raft_node_id_t> fail_server_sets;
//   for (auto& [id, chunk_vec] : chunk_vecs) {
//     chunk_cnt_each = std::max(chunk_cnt_each, (int)chunk_vec.as_vec().size());
//     for (int i = r_; i < chunk_vec.as_vec().size(); ++i) {
//       fail_server_sets.emplace(chunk_vec.as_vec().at(i).Index1().node_id);
//     }
//   }

//   assert(chunk_cnt_each > 0);

//   // Recover the information related to the failure case:
//   // Let x = # of replenish servers
//   //     y = # of replenish chunk count saved by each replenish server
//   //     z = # of fail servers
//   // Then the following equations hold:
//   //   y * z = chunk_cnt_each - r_
//   //       x = N - z - F
//   //   y * x = r_
//   //
//   // The solutions would be:
//   //   x = (N - F) * r / C
//   //   y = C / (N - F)
//   //   z = (N - F) * (C - r) / C
//   int N = 2 * F_ + 1;
//   auto replenish_server_cnt = (N - F_) * r_ / chunk_cnt_each;
//   auto replenish_chunk_cnt_each = chunk_cnt_each / (N - F_);
//   auto fail_server_cnt = (N - F_) * (chunk_cnt_each - r_) / chunk_cnt_each;

//   // First try recovering the data that comes from the second encoding phase:
//   Encoder decoder;
//   std::vector<Slice> snd_phase_decode_output;

//   util::Enumerator<int>(r_, chunk_cnt_each, 1).for_each([&](int i) {
//     Encoder::EncodingResults decode_input;
//     Slice decode_output;
//     for (auto [id, chunk_vec] : chunk_vecs) {
//       if (chunk_vec.as_vec().size() == chunk_cnt_each) {
//         auto chunk = chunk_vec.as_vec().at(i);
//         auto idx = chunk.Index2().node_id;
//         decode_input.emplace(idx, chunk.slice());
//       }
//     }
//     decoder.DecodeSlice(decode_input, replenish_server_cnt, F_, &decode_output);
//     snd_phase_decode_output.emplace_back(decode_output);
//   });

//   // Now construct the replenish data fragments:
//   std::map<raft_node_id_t, Slice> replenish_fragments;
//   int off = 0;
//   auto iter = fail_server_sets.begin();
//   // for (int f = 0; f < fail_server_cnt; ++f, ++iter) {
//   //   std::vector<Slice> replenish_fragments_input;
//   //   for (int i = 0; i < replenish_server_cnt; ++i) {
//   //     for (int j = 0; j < replenish_chunk_cnt_each; ++j) {
//   //       // Note that the slice in snd_phase_decode_output is only the replenish fragments
//   //       // So use replenish_server_cnt to shard, instead of replenish_server_cnt + F_ to shard
//   //       replenish_fragments_input.emplace_back(
//   //           snd_phase_decode_output[j + off].Shard(replenish_server_cnt).at(i));
//   //     }
//   //   }
//   //   off += replenish_chunk_cnt_each;
//   //   auto replenish_fragment = Slice::Combine(replenish_fragments_input);
//   //   replenish_fragments.emplace(*iter, replenish_fragment);
//   // }

//   util::Enumerator<int>(0, fail_server_cnt, 1).for_each([&](int f) {
//     std::vector<Slice> replenish_fragments_input;
//     util::NestedIterator(util::Enumerator(0, replenish_server_cnt, 1),
//                          util::Enumerator(0, replenish_chunk_cnt_each, 1))
//         .for_each([&](int i, int j) {
//           replenish_fragments_input.emplace_back(
//               snd_phase_decode_output[j + off].Shard(replenish_server_cnt).at(i));
//         });
//     off += replenish_chunk_cnt_each;
//     auto replenish_fragment = Slice::Combine(replenish_fragments_input);
//     replenish_fragments.emplace(*iter, replenish_fragment);
//     ++iter;
//   });

//   return replenish_fragments;
// }

// // Decode input ChunkVectors to get the original data, write it into the resultant slice
// bool ChunkDistribution::Decode(std::unordered_map<raft_node_id_t, ChunkVector>& chunk_vecs,
//                                Slice* slice) {
//   Encoder::EncodingResults final_decode_input = RecoverReplenishFragments(chunk_vecs);
//   Encoder final_decoder;

//   // util::ContainerIterator(chunk_vecs)
//   //     .filter([](auto elem) { return elem.second.as_vec().size() > 0; })
//   //     .for_each([&](auto elem) {
//   //       if (final_decode_input.size() >= k_) {
//   //         return;
//   //       }
//   //       final_decode_input.emplace(elem.first, Slice::Combine(elem.second.SubVec(0, r_)));
//   //     });

//   auto iter =
//       NewContainerIter(chunk_vecs)
//           ->filter([](auto elem) { return elem.second.as_vec().size() > 0; })
//           ->for_each([&](auto elem) {
//             if (final_decode_input.size() >= k_) {
//               return;
//             }
//             final_decode_input.emplace(elem.first, Slice::Combine(elem.second.SubVec(0, r_)));
//           });
//   delete iter;

//   return final_decoder.DecodeSlice(final_decode_input, k_, F_, slice);
// }

// };  // namespace CODE_CONVERSION_NAMESPACE

// };  // namespace raft

namespace raft {
namespace CODE_CONVERSION_NAMESPACE {

void CodeConversionManagement::PrepareOriginalChunks(const Slice& slice,
                                                     StaticEncoder* static_encoder) {
  int total_chunk_num = r_ * k_;

  // Cut the data into a few chunks:
  // NOTE: We have a compulsory requirements that the data size must be aligned with
  // the chunk number.
  assert(slice.size() % total_chunk_num == 0);

  /// Encode the original data:
  std::vector<Slice> input_slices = slice.Shard(k_), output_slices;

  if (static_encoder && static_encoder->GetEncodeK() == k_ && static_encoder->GetEncodeM() == F_) {
    static_encoder->EncodeSlice(input_slices, output_slices);
    printf("Use static encoder to encode data\n");
  } else {
    Encoder encoder;
    encoder.EncodeSlice(input_slices, k_, F_, output_slices);
  }

  // Shard the fragments into chunks accordingly. Note that the output_slices
  // also contains the original chunk
  for (int node_id = 0; node_id < k_ + F_; ++node_id) {
    org_chunks_[node_id].clear();
    auto s = output_slices[node_id];
    auto shard = s.Shard(r_);
    org_chunks_[node_id].insert(org_chunks_[node_id].end(), shard.begin(), shard.end());
  }
}

void CodeConversionManagement::AssignOriginalChunksToNode(const ChunkDistribution& cd) {
  for (int i = 0; i < k_ + F_; ++i) {
    for (const auto& cidx : cd.GetAssignedOrgChunks(i)) {
      assert(cidx.node_id == i);
      add_original_chunks(i, cidx, ChunkIndex::InvalidChunkIndex(),
                          org_chunks_[cidx.node_id][cidx.chunk_id]);
    }
  }
}

void CodeConversionManagement::EncodeReservedChunksAndAssignToNode(const ChunkDistribution& cd) {
  auto reserved_servers = cd.GetPlacementInfo().get_reservation_servers();
  auto parity_servers = cd.GetPlacementInfo().get_parity_servers();

  auto encode_for_single_row = [&](int row) {
    std::vector<Slice> encode_input, encode_output;
    int encode_group_idx = 0, fail_server_id = -1;

    for (const auto& reserve_node_id : reserved_servers) {
      auto cidx = cd.GetAssignedReserveChunks(reserve_node_id).at(row);
      add_reserved_chunks(reserve_node_id, cidx, ChunkIndex(encode_group_idx, 0),
                          org_chunks_[cidx.node_id][cidx.chunk_id]);
      encode_input.emplace_back(org_chunks_[cidx.node_id][cidx.chunk_id]);
      ++encode_group_idx;
      fail_server_id = cidx.node_id;
    }

    // Do the encoding process
    Encoder encoder;
    int encode_para1 = reserved_servers.size(), encode_para2 = this->F_;
    encoder.EncodeSlice(encode_input, encode_para1, encode_para2, encode_output);

    // Now write the encoding results
    for (const auto& parity_node_id : parity_servers) {
      add_reserved_chunks(parity_node_id, ChunkIndex(fail_server_id, -1),
                          ChunkIndex(encode_group_idx, 0), encode_output[encode_group_idx]);
      ++encode_group_idx;
    }
  };

  for (int row = 0; row < cd.GetReservedChunkCountForEach(); ++row) {
    encode_for_single_row(row);
  }
}

void CodeConversionManagement::EncodeForPlacement(const Slice& slice,
                                                  const std::vector<bool>& live_vec,
                                                  StaticEncoder* static_encoder) {
  util::LatencyGuard guard([](uint64_t us) { printf("Encode cost: %lu us\n", us); });
  PrepareOriginalChunks(slice, static_encoder);

  ChunkDistribution cd(k_, F_, r_);
  cd.GenerateChunkDistribution(live_vec);

  AssignOriginalChunksToNode(cd);
  EncodeReservedChunksAndAssignToNode(cd);
  SetLiveVecForCurrDistribution(live_vec);
}

void CodeConversionManagement::AdjustChunkDistribution(const std::vector<bool>& live_vec) {
  util::LatencyGuard guard([](uint64_t us) { printf("Adjust cost: %lu us\n", us); });
  if (live_vec == GetLiveVecForCurrDistribution()) {
    // No need to adjust the chunk distribution if the liveness vector has not changed
    return;
  }
  ChunkDistribution cd(k_, F_, r_);
  cd.GenerateChunkDistribution(live_vec);
  AdjustChunkDistribution(cd);
  SetLiveVecForCurrDistribution(live_vec);
}

std::map<raft_node_id_t, Slice> CodeConversionManagement::RecoverReservedChunks(
    const std::map<raft_node_id_t, ChunkVector>& input) {
  int chunk_cnt_each = 0;
  std::set<raft_node_id_t> fail_server_sets;
  for (const auto& [id, chunk_vec] : input) {
    chunk_cnt_each = std::max(chunk_cnt_each, (int)chunk_vec.as_vec().size());
    for (int i = r_; i < chunk_vec.as_vec().size(); ++i) {
      fail_server_sets.emplace(chunk_vec.as_vec().at(i).Index1().node_id);
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

  util::Enumerator<int>(r_, chunk_cnt_each, 1).for_each([&](int i) {
    Encoder::EncodingResults decode_input;
    Slice decode_output;
    for (auto [id, chunk_vec] : input) {
      if (chunk_vec.as_vec().size() == chunk_cnt_each) {
        auto chunk = chunk_vec.as_vec().at(i);
        auto idx = chunk.Index2().node_id;
        decode_input.emplace(idx, chunk.slice());
      }
    }
    decoder.DecodeSlice(decode_input, replenish_server_cnt, F_, &decode_output);
    snd_phase_decode_output.emplace_back(decode_output);
  });

  // Now construct the replenish data fragments:
  std::map<raft_node_id_t, Slice> reserved_fragments;
  int off = 0;
  auto iter = fail_server_sets.begin();
  // for (int f = 0; f < fail_server_cnt; ++f, ++iter) {
  //   std::vector<Slice> replenish_fragments_input;
  //   for (int i = 0; i < replenish_server_cnt; ++i) {
  //     for (int j = 0; j < replenish_chunk_cnt_each; ++j) {
  //       // Note that the slice in snd_phase_decode_output is only the replenish fragments
  //       // So use replenish_server_cnt to shard, instead of replenish_server_cnt + F_ to shard
  //       replenish_fragments_input.emplace_back(
  //           snd_phase_decode_output[j + off].Shard(replenish_server_cnt).at(i));
  //     }
  //   }
  //   off += replenish_chunk_cnt_each;
  //   auto replenish_fragment = Slice::Combine(replenish_fragments_input);
  //   replenish_fragments.emplace(*iter, replenish_fragment);
  // }

  util::Enumerator<int>(0, fail_server_cnt, 1).for_each([&](int f) {
    std::vector<Slice> replenish_fragments_input;
    util::NestedIterator(util::Enumerator(0, replenish_server_cnt, 1),
                         util::Enumerator(0, replenish_chunk_cnt_each, 1))
        .for_each([&](int i, int j) {
          replenish_fragments_input.emplace_back(
              snd_phase_decode_output[j + off].Shard(replenish_server_cnt).at(i));
        });
    off += replenish_chunk_cnt_each;
    auto replenish_fragment = Slice::Combine(replenish_fragments_input);
    reserved_fragments.emplace(*iter, replenish_fragment);
    ++iter;
  });

  return reserved_fragments;
}

bool CodeConversionManagement::DecodeCollectedChunkVec(
    const std::map<raft_node_id_t, ChunkVector>& input, Slice* slice) {
  util::LatencyGuard guard([](uint64_t us) { printf("Decode cost: %lu us\n", us); });
  Encoder::EncodingResults final_decode_input = RecoverReservedChunks(input);
  Encoder final_decoder;

  iter::NewContainerIterator(input)
      .filter([](auto elem) { return elem.second.as_vec().size() > 0; })
      .for_each([&](auto elem) {
        if (final_decode_input.size() >= k_) {
          return;
        }
        const auto& [id, v] = elem;
        final_decode_input.emplace(id, Slice::Combine(v.SubVec(0, r_).as_slice_vec()));
      });

  return final_decoder.DecodeSlice(final_decode_input, k_, F_, slice);
}

void CodeConversionManagement::AdjustChunkDistribution(const ChunkDistribution& cd) {
  // First, clear all exists chunks from original chunk:
  node_2_org_chunks_.clear();

  // Re-assign the original ChunkVector to each node, note that there is no need
  // to encode the original data slice again.
  AssignOriginalChunksToNode(cd);

  // Then, clear all reserved chunks and delete all allocated memory
  for (const auto& [node_id, chunk_vec] : node_2_reserved_chunks_) {
    for (const auto& chunk : chunk_vec.as_vec()) {
      // This is a parity chunk
      if (chunk.Index1().chunk_id == -1) {
        // delete[] chunk.data();
      }
    }
  }
  node_2_reserved_chunks_.clear();

  // Re-do the 2nd phase encoding
  EncodeReservedChunksAndAssignToNode(cd);
}

void CodeConversionManagement::UpdateOrgChunkResponseInfo(raft_node_id_t node_id, bool t) {
  org_chunk_response_[node_id] = t;
}

};  // namespace CODE_CONVERSION_NAMESPACE
};  // namespace raft
