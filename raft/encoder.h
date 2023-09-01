#pragma once
#include <algorithm>
#include <map>
#include <unordered_map>
#include <vector>

#include "log_entry.h"
#include "raft_type.h"
#include "util.h"

namespace raft {

// Stripe is a collection of fragments encoded from a complete entry
struct Stripe {
  raft_index_t raft_index;
  raft_term_t raft_term;
  std::map<raft_frag_id_t, LogEntry> fragments;
  // std::vector<LogEntry> collected_fragments;
  std::map<raft_frag_id_t, LogEntry> collected_fragments;

  size_t CollectFragmentsCount() const { return collected_fragments.size(); }
};

// Encoder controls the encoding/decoding process of a raft log entry by
// different parameters. The encoding and decoding of a raft entry is using an
// RS erasure coding scheme
class Encoder {
  static constexpr int kMaxK = 9;
  static constexpr int kMaxM = 9;

  // The RS encoding process may require fragment length to be aligned. When
  // kAlignment = 1, there is no need to padding the alignment.
  static constexpr int kAlignment = 8;

 public:
  using EncodingResults = std::map<raft_frag_id_t, Slice>;

  Encoder() {
    encode_matrix_ = new unsigned char[kMaxK * (kMaxM + kMaxK)];
    decode_matrix_ = new unsigned char[kMaxK * (kMaxM + kMaxK)];
    errors_matrix_ = new unsigned char[kMaxK * kMaxK];
    invert_matrix_ = new unsigned char[kMaxK * kMaxK];
  }

  ~Encoder() {
    delete[] encode_matrix_;
    delete[] decode_matrix_;
    delete[] errors_matrix_;
  }

  Encoder(const Encoder &) = delete;
  Encoder &operator=(const Encoder &) = delete;

 public:
  // Encode a specified slice into fragments by setting input results vector,
  // parameters k and m is used to control this encoding process
  bool EncodeSlice(const Slice &slice, int k, int m, EncodingResults *results);

  // Decode a set of fragments to generate the full content, the corresponding k
  // and m parameters should be set before calling this method
  bool DecodeSlice(const EncodingResults &input, int k, int m, Slice *results);

  // A helper function: Decode entry to a data place specified by data, set the
  // size to be data length after decoding
  bool DecodeSliceHelper(const EncodingResults &input, int k, int m, char *data, int *size);

 private:
  // A built-in encoding matrix space. Since encoding matrix will not be changed
  // during encoding/decoding process, we can allocate this matrix during
  // initialization an EC controller and deallocate it in destructor.
  //
  // However, remember to reset the rs matrix coefficients for encode_matrix
  // every time calling EncodeEntry since k, m might have been changed
  unsigned char *encode_matrix_;

  unsigned char *decode_matrix_;

  unsigned char *errors_matrix_;

  unsigned char *invert_matrix_;

  // When decoding an array consists of k fragments, we need to check out which
  // rows should be picked in encode matrix and which are not
  std::vector<int> missing_rows_, valid_rows_;

  // An array that stores the start pointer of each input fragments to be
  // encoded
  unsigned char *encode_input_[kMaxK];

  // An array that stores the start pointer of each output fragments that have
  // been encoded
  unsigned char *encode_output_[kMaxM];

  unsigned char *decode_input_[kMaxK];

  unsigned char *decode_output_[kMaxK];
};
}  // namespace raft
