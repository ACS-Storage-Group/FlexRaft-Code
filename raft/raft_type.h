#pragma once
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sstream>

#include "RCF/RCF.hpp"

namespace raft {

using raft_index_t = uint32_t;

using raft_term_t = uint32_t;

using raft_node_id_t = uint32_t;

using raft_sequence_t = uint32_t;

using raft_frag_id_t = uint32_t;

using raft_encoding_param_t = uint32_t;

enum raft_entry_type { kNormal = 0, kFragments = 1, kTypeMax = 2 };

inline const char *EntryTypeToString(const raft_entry_type &type) {
  switch (type) {
    case (kNormal):
      return "kNormal";
    case (kFragments):
      return "kFragments";
    default:
      assert(0);
  }
  return "Unknown type";
}

// struct VersionNumber {
//   raft_term_t term;
//   uint32_t seq;
//
//   void SetTerm(raft_term_t term) { this->term = term; }
//   void SetSeq(uint32_t seq) { this->seq = seq; }
//
//   raft_term_t Term() const { return this->term; }
//   uint32_t Seq() const { return this->seq; }
//
//   // When comparing version number, compare term first, higher term means
//   // higher version number; then compare sequence, each sequence is generated
//   // within a leader term
//   int compare(const VersionNumber &rhs) const {
//     if (this->term == rhs.term) {
//       if (this->seq > rhs.seq) {
//         return 1;
//       } else if (this->seq == rhs.seq) {
//         return 0;
//       } else {
//         return -1;
//       }
//     } else {
//       if (this->term > rhs.term) {
//         return 1;
//       } else {
//         return -1;
//       }
//     }
//   }
//
//   bool operator==(const VersionNumber &rhs) { return this->compare(rhs) == 0;
//   }
//
//   std::string ToString() const {
//     char buf[256];
//     sprintf(buf, "VersionNumber{term=%d, seq=%d}", Term(), Seq());
//     return std::string(buf);
//   }
//
//   static VersionNumber Default() { return VersionNumber{0, 0}; }
// };
//
// // A version is a struct that records encoding-related version of an entry
// struct Version {
//   VersionNumber version_number;
//   // Encoding related data
//   int k, m;
//   raft_frag_id_t fragment_id;
//
//   static Version Default() { return {VersionNumber::Default(), 0, 0, 0}; }
//
//   VersionNumber GetVersionNumber() const { return version_number; }
//   int GetK() const { return k; }
//   int GetM() const { return m; }
//   raft_frag_id_t GetFragmentId() const { return fragment_id; }
//
//   void SetVersionNumber(const VersionNumber &v) { this->version_number = v; }
//   void SetK(int k) { this->k = k; }
//   void SetM(int m) { this->m = m; }
//   void SetFragmentId(raft_frag_id_t id) { this->fragment_id = id; }
//
//   // Dump the data
//   std::string ToString() const {
//     char buf[256];
//     sprintf(buf,
//             "Version{VersionNumber{Term=%d, Seq=%d}, K=%d, M=%d, FragID=%d}",
//             GetVersionNumber().Term(), GetVersionNumber().Seq(), GetK(),
//             GetM(), GetFragmentId());
//     return std::string(buf);
//   }
//
//   // For check simplicity
//   bool operator==(const Version &rhs) const {
//     return std::memcmp(this, &rhs, sizeof(Version)) == 0;
//   }
// };

// ChunkInfo is an associated encoding information with an encoded chunk.
// It specifies the encoding parameter: k. Note that m is always calculated
// by N-k; The ChunkId is determined directly by the follower's node id
struct ChunkInfo {
  raft_encoding_param_t k;
  raft_index_t raft_index;
  bool contain_original;  // Indicate if the original part of this chunk has been stored

  auto GetK() const -> raft_encoding_param_t { return this->k; }
  void SetK(int k) { this->k = k; }

  auto GetRaftIndex() const -> raft_index_t { return raft_index; }
  void SetRaftIndex(raft_index_t raft_index) { this->raft_index = raft_index; }

  std::string ToString() const {
    char buf[256];
    sprintf(buf, "ChunkInfo{k=%d, index=%d, contain=%d}", GetK(), GetRaftIndex(), contain_original);
    return std::string(buf);
  }

  bool operator==(const ChunkInfo &rhs) const {
    return this->GetK() == rhs.GetK() && this->GetRaftIndex() == rhs.GetRaftIndex();
  }
};

class Slice {
 public:
  static Slice Copy(const Slice &slice) {
    auto data = new char[slice.size() + 12];
    std::memcpy(data, slice.data(), slice.size());
    return Slice(data, slice.size());
  }

 public:
  Slice(char *data, size_t size) : data_(data), size_(size) {}
  Slice(const std::string &s) : data_(new char[s.size()]), size_(s.size()) {
    std::memcpy(data_, s.c_str(), size_);
  }

  Slice() = default;
  Slice(const Slice &) = default;
  Slice &operator=(const Slice &) = default;

  auto data() const -> char * { return data_; }
  auto size() const -> size_t { return size_; }
  auto valid() const -> bool { return data_ != nullptr && size_ > 0; }
  auto toString() const -> std::string { return std::string(data_, size_); }

  // Require both slice are valid
  auto compare(const Slice &slice) -> int {
    assert(valid() && slice.valid());
    auto cmp_len = std::min(size(), slice.size());
    auto cmp_res = std::memcmp(data(), slice.data(), cmp_len);
    if (cmp_res != 0 || size() == slice.size()) {
      return cmp_res;
    }
    return size() > slice.size() ? 1 : -1;
  }

  // Shard the slice into multiple equal-sized subslices:
  // Note that it must be size_ % k = 0:
  auto Shard(int k) const -> std::vector<Slice> {
    auto sub_sz = size() / k;
    std::vector<Slice> v;
    auto d = data();
    for (int i = 0; i < k; ++i) {
      v.push_back(Slice(d, sub_sz));
      d += sub_sz;
    }
    return v;
  }

  // Combine the contents of multiple slices into one single slice
  static auto Combine(const std::vector<Slice> &slices) -> Slice {
    size_t alloc_sz = 0;
    for (const auto &s : slices) alloc_sz += s.size();
    auto alloc_data = new char[alloc_sz];
    auto d = alloc_data;
    for (const auto &s : slices) {
      std::memcpy(d, s.data(), s.size());
      d += s.size();
    }
    return Slice(alloc_data, alloc_sz);
  }

 private:
  char *data_ = nullptr;
  size_t size_ = 0;
};

// Structs that are related to raft core algorithm

}  // namespace raft
