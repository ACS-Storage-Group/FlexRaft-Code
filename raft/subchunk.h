#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iterator>
#include <numeric>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "raft_type.h"
#include "util.h"

namespace raft {

#define CODE_CONVERSION_NAMESPACE code_conversion

namespace CODE_CONVERSION_NAMESPACE {
struct SubChunkInfo {
  static constexpr int16_t kInvalidId = (int16_t)-1;

  // chunk_id: The id of original chunk this subchunk is encoded from
  // subchunk_id: The id of subchunk in the encoded stripe
  int16_t chunk_id, subchunk_id;

  SubChunkInfo(int16_t chunk_id, int16_t subchunk_id)
      : chunk_id(chunk_id), subchunk_id(subchunk_id) {}
  SubChunkInfo() : chunk_id(kInvalidId), subchunk_id(kInvalidId) {}

  SubChunkInfo(const SubChunkInfo&) = default;
  SubChunkInfo& operator=(const SubChunkInfo&) = default;

  bool Valid() const { return chunk_id != kInvalidId && subchunk_id != kInvalidId; }

  auto ChunkId() const { return chunk_id; }
  auto SubChunkId() const { return subchunk_id; }

  static SubChunkInfo InvalidSubChunkInfo() { return SubChunkInfo(); }

  std::string ToString() const {
    std::stringstream ss;
    ss << "(" << std::setw(3) << chunk_id << "," << std::setw(3) << subchunk_id << ")";
    return ss.str();
  }

  friend bool operator==(const SubChunkInfo& l, const SubChunkInfo& r) {
    return l.chunk_id == r.chunk_id && l.subchunk_id == r.subchunk_id;
  }

  size_t SizeForSer() const { return sizeof(int16_t) * 2; }

  char* Serialize(char* d) const {
    *(int16_t*)d = chunk_id;
    *(int16_t*)(d + sizeof(int16_t)) = subchunk_id;
    return d + sizeof(int16_t) * 2;
  }

  const char* Deserialize(const char* s) {
    chunk_id = *(int16_t*)s;
    subchunk_id = *(int16_t*)(s + sizeof(int16_t));
    return s + sizeof(int16_t) * 2;
  }
};

struct SubChunk {
  SubChunkInfo subchunk_info_;
  Slice data_;

  SubChunk(SubChunkInfo info, Slice d) : subchunk_info_(info), data_(d) {}

  SubChunk() = default;
  SubChunk(const SubChunk&) = default;
  SubChunk& operator=(const SubChunk&) = default;

  char* data() const { return data_.data(); }
  size_t size() const { return data_.size(); }
  Slice slice() const { return data_; }

  size_t SizeForSer() const { return subchunk_info_.SizeForSer() + sizeof(int) + data_.size(); }

  char* Serialize(char* d) const {
    d = subchunk_info_.Serialize(d);
    *(int*)d = data_.size();
    d += sizeof(int);
    std::memcpy(d, data_.data(), data_.size());
    return d + data_.size();
  }

  Slice Serialize() const {
    auto alloc_sz = SizeForSer();

    auto d = new char[alloc_sz];
    auto b = Serialize(d);
    (void)b;

    return Slice(d, alloc_sz);
  }

  const char* Deserialize(const char* s) {
    s = subchunk_info_.Deserialize(s);
    size_t sz = *(int*)s;
    s += sizeof(int);
    data_ = Slice(const_cast<char*>(s), sz);
    return s + sz;
  }

  auto GetSubChunkInfo() { return subchunk_info_; }

  bool operator==(const SubChunk& r) const {
    return this->subchunk_info_ == r.subchunk_info_ && this->slice().compare(r.slice()) == 0;
  }

  bool operator!=(const SubChunk& r) const { return !operator==(r); }
};

struct SubChunkVector {
  std::vector<SubChunk> subchunks_;

  SubChunkVector() = default;
  SubChunkVector(const SubChunkVector&) = default;
  SubChunkVector& operator=(const SubChunkVector&) = default;
  ~SubChunkVector() = default;

  void AddSubChunk(SubChunkInfo info, Slice s) { subchunks_.emplace_back(info, s); }
  void AddSubChunk(SubChunk subchunk) { subchunks_.emplace_back(subchunk); }

  Slice Serialize() const;
  char* Serialize(char* d) const;

  bool Deserialize(const Slice& s);
  const char* Deserialize(const char* s);

  size_t SizeForSer() const {
    size_t alloc_sz = sizeof(int);
    for (const auto& c : subchunks_) {
      alloc_sz += c.SizeForSer();
    }
    return alloc_sz;
  }

  SubChunkVector SubVec(int l, int r) const {
    SubChunkVector ret;
    for (int i = l; i < std::min(r, (int)subchunks_.size()); ++i) {
      ret.subchunks_.emplace_back(subchunks_[i]);
    }
    return ret;
  }

  void Concatenate(const SubChunkVector& cv) {
    for (const auto& v : cv.subchunks_) {
      subchunks_.emplace_back(v);
    }
  }

  SubChunk at(int idx) const { return subchunks_[idx]; }
  size_t size() const { return subchunks_.size(); }

  const auto& as_vec() const { return subchunks_; }

  auto as_slice_vec() -> std::vector<Slice> const {
    std::vector<Slice> ret;
    for (const auto& c : subchunks_) {
      ret.emplace_back(c.slice());
    }
    return ret;
  }

  void clear() { subchunks_.clear(); }

  bool operator==(const SubChunkVector& rhs) const { return this->subchunks_ == rhs.subchunks_; }

  bool operator!=(const SubChunkVector& rhs) const { return !operator==(rhs); }
};

};  // namespace CODE_CONVERSION_NAMESPACE
};  // namespace raft