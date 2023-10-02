#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <string>

#include "SF/Archive.hpp"
#include "chunk.h"
#include "raft_type.h"

namespace raft {

class Serializer;

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

  void SetOriginalChunkVector(const CODE_CONVERSION_NAMESPACE::ChunkVector &cv) { org_cv_ = cv; }
  auto GetOriginalChunkVector() const -> const CODE_CONVERSION_NAMESPACE::ChunkVector & {
    return org_cv_;
  }
  auto &OriginalChunkVectorRef() { return org_cv_; }

  void SetReservedChunkVector(const CODE_CONVERSION_NAMESPACE::ChunkVector &cv) {
    reserved_cv_ = cv;
  }
  auto GetReservedChunkVector() const -> const CODE_CONVERSION_NAMESPACE::ChunkVector& {
    return reserved_cv_;
  }
  auto &ReservedChunkVectorRef() { return reserved_cv_; }

  auto GetConcatenatedChunkVector() const {
    auto ret = GetOriginalChunkVector();
    ret.Concatenate(reserved_cv_);
    return ret;
  }

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

  // For compatability concern, we do not remove the fragment_slice_ attribute.
  // Instead, we add two additional attributes to differentiate the original ChunkVector and
  // Reserved ChunkVector
  CODE_CONVERSION_NAMESPACE::ChunkVector org_cv_;
  CODE_CONVERSION_NAMESPACE::ChunkVector reserved_cv_;
};

auto operator==(const LogEntry &lhs, const LogEntry &rhs) -> bool;
}  // namespace raft
