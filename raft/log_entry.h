#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <string>

#include "SF/Archive.hpp"
#include "raft_type.h"

namespace raft {

class Serializer;

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
  static auto Combine(const std::vector<Slice>& slices) -> Slice {
    size_t alloc_sz = 0;
    for (const auto& s : slices) alloc_sz += s.size();
    auto alloc_data = new char[alloc_sz];
    auto d = alloc_data;
    for (const auto& s : slices) {
      std::memcpy(d, s.data(), s.size());
      d += s.size();
    }
    return Slice(alloc_data, alloc_sz);
  }

 private:
  char *data_ = nullptr;
  size_t size_ = 0;
};

class Stripe;
class LogEntry {
  friend class Serializer;

 public:
  LogEntry() = default;
  LogEntry &operator=(const LogEntry &) = default;

  auto Index() const -> raft_index_t { return index; }
  void SetIndex(raft_index_t index) { this->index = index; }

  auto Term() const -> raft_term_t { return term; }
  void SetTerm(raft_term_t term) { this->term = term; }

  auto Type() const -> raft_entry_type { return type; }
  void SetType(raft_entry_type type) { this->type = type; }

  auto GetChunkInfo() const -> ChunkInfo { return chunk_info; }
  void SetChunkInfo(const ChunkInfo &chunk_info) { this->chunk_info = chunk_info; }

  auto StartOffset() const -> int { return start_fragment_offset; }
  void SetStartOffset(int off) { start_fragment_offset = off; }

  auto CommandData() const -> Slice { return Type() == kNormal ? command_data_ : Slice(); }
  auto CommandLength() const -> int { return command_size_; }
  void SetCommandLength(int size) { command_size_ = size; }

  void SetCommandData(const Slice &slice) {
    command_data_ = slice;
    command_size_ = slice.size();
  }

  auto NotEncodedSlice() const -> Slice {
    return Type() == kNormal ? CommandData() : not_encoded_slice_;
  }
  void SetNotEncodedSlice(const Slice &slice) { not_encoded_slice_ = slice; }

  auto FragmentSlice() const -> Slice { return Type() == kNormal ? Slice() : fragment_slice_; }
  void SetFragmentSlice(const Slice &slice) { fragment_slice_ = slice; }

  // Serialization function required by RCF
  // void serialize(SF::Archive &ar);
  //
  // Dump some important information
  std::string ToString() const {
    char buf[256];
    sprintf(buf,
            "LogEntry{term=%d, index=%d, type=%s, chunkinfo=%s, "
            "commandlen=%d, start_off=%d}",
            Term(), Index(), EntryTypeToString(Type()), chunk_info.ToString().c_str(),
            CommandLength(), StartOffset());

    return std::string(buf);
  }

 private:
  // These three attributes are allocated when creating a command
  raft_term_t term;
  raft_index_t index;
  raft_entry_type type;  // Full entry or fragments

  // Information of this chunk that is contained in this raft entry
  ChunkInfo chunk_info;

  // [REQUIRE] specified by user, indicating the start offset of command
  // data for encoding
  int start_fragment_offset;
  int command_size_;

  Slice command_data_;       // Spcified by user, valid iff type = normal
  Slice not_encoded_slice_;  // Command data not being encoded
  Slice fragment_slice_;     // Fragments of encoded data
};

auto operator==(const LogEntry &lhs, const LogEntry &rhs) -> bool;
}  // namespace raft
