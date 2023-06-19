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

// Version is used in FlexibleK to mark the status of a stripe
// class Stripe {
//   friend class Encoder;
//
//  public:
//   Stripe() = default;
//
//   const LogEntry& GetFragment(int i) { return fragments_[i]; }
//
//   int FragmentNum() const { return fragments_.size(); }
//
//   Slice NotEncodedSlice() { return fragments_[0].NotEncodedSlice(); }
//
//   void AddFragments(const LogEntry& frag) {
//     LOG(util::kRaft, "Stripe ent size=%d", fragments_.size());
//     fragments_.push_back(frag);
//   }
//
//   auto FragmentCount() -> int { return fragments_.size(); }
//
//   // K + M
//   int GetN() const { return n_; }
//
//   // K
//   int GetK() const { return k_; }
//
//   int FragmentLength() const { return frag_length_; }
//
//   // LogEntry::Header GetHeader() const { return fragments_[0].hdr; }
//   //
//   // void Remove(int i) { fragments_.erase(fragments_.begin() + i); }
//
//   // Since the stripe may contain entries of different k and m during collecting
//   // fragments process, we need first to filter those entries with different
//   // metadata.
//   void Filter();
//
//   void SetIndex(raft_index_t idx) { index_ = idx; }
//
//   void SetTerm(raft_term_t term) { term_ = term; }
//
//   raft_index_t GetIndex() const { return index_; }
//
//   raft_term_t GetTerm() const { return term_; }
//
//   void SetN(int n) { this->n_ = n; }
//
//   void SetK(int k) { this->k_ = k; }
//
//   size_t CommandLength() { return this->fragments_[0].CommandLength(); }
//
//   void SetFragLength(int length) { frag_length_ = length; }
//
//   void UpdateVersion(const Version& v) { version_ = v; }
//
//   const Version& GetVersion() const { return version_; }
//
//   void Init() {
//     fragments_.clear();
//     LOG(util::kRaft, "Stripe ent size=%d", fragments_.size());
//   }
//
//   void Remove(int i) {
//     // Delete an entry with specific frag id
//     auto iter =
//         std::find_if(fragments_.begin(), fragments_.end(), [=](const LogEntry& ent) {
//           return ent.GetVersion().GetFragmentId() == static_cast<uint16_t>(i);
//         });
//     if (iter != fragments_.end()) {
//       fragments_.erase(iter);
//     }
//   }
//
//  private:
//   // Meta information of this stripe
//   raft_index_t index_;
//   raft_term_t term_;
//
//   // a collection of fragments
//   // std::unordered_map<int, LogEntry> fragments_;
//   std::vector<LogEntry> fragments_;
//
//   // Encoding/Decoding parameters
//   // frag_cnt_ is the number of fragments encoded from the original entry,
//   // recover_req_cnt_ is the number of fragments used to recover the original
//   // entry. i.e. frag_cnt_ = k+m, recover_req_cnt_ = k
//   int k_, n_;
//
//   // The length of each fragment, all fragments are of the same length
//   int frag_length_;
//
//   // A version that needs to be updated for every encoding operation
//   Version version_;
// };

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

  Encoder(const Encoder&) = delete;
  Encoder& operator=(const Encoder&) = delete;

 public:
  // Encode a specified slice into fragments by setting input results vector, parameters
  // k and m is used to control this encoding process
  bool EncodeSlice(const Slice& slice, int k, int m, EncodingResults* results);

  // Decode a set of fragments to generate the full content, the corresponding k and m
  // parameters should be set before calling this method
  bool DecodeSlice(const EncodingResults& input, int k, int m, Slice* results);

  // A helper function: Decode entry to a data place specified by data, set the size to 
  // be data length after decoding
  bool DecodeSliceHelper(const EncodingResults& input, int k, int m, char* data,
                         int* size);

 private:
  // A built-in encoding matrix space. Since encoding matrix will not be changed
  // during encoding/decoding process, we can allocate this matrix during
  // initialization an EC controller and deallocate it in destructor.
  //
  // However, remember to reset the rs matrix coefficients for encode_matrix
  // every time calling EncodeEntry since k, m might have been changed
  unsigned char* encode_matrix_;

  unsigned char* decode_matrix_;

  unsigned char* errors_matrix_;

  unsigned char* invert_matrix_;

  // When decoding an array consists of k fragments, we need to check out which
  // rows should be picked in encode matrix and which are not
  std::vector<int> missing_rows_, valid_rows_;

  // An array that stores the start pointer of each input fragments to be
  // encoded
  unsigned char* encode_input_[kMaxK];

  // An array that stores the start pointer of each output fragments that have
  // been encoded
  unsigned char* encode_output_[kMaxM];

  unsigned char* decode_input_[kMaxK];

  unsigned char* decode_output_[kMaxK];
};
}  // namespace raft
