#include "subchunk.h"

namespace raft {
namespace CODE_CONVERSION_NAMESPACE {

char* SubChunkVector::Serialize(char* d) const {
  // Serialize the data
  *(int*)d = subchunks_.size();
  d += sizeof(int);
  for (const auto& c : subchunks_) {
    d = c.Serialize(d);
  }
  return d;
}

Slice SubChunkVector::Serialize() const {
  auto alloc_sz = SizeForSer();

  auto d = new char[alloc_sz];
  auto b = Serialize(d);
  (void)b;

  return Slice(d, alloc_sz);
}

bool SubChunkVector::Deserialize(const Slice& s) {
  Deserialize(s.data());
  return true;
}

const char* SubChunkVector::Deserialize(const char* s) {
  clear();
  int ent_cnt = *(int*)s;
  s += sizeof(int);
  SubChunk sub_chunk;
  for (int i = 0; i < ent_cnt; ++i) {
    s = sub_chunk.Deserialize(s);
    subchunks_.emplace_back(sub_chunk);
  }
  return s;
}
};  // namespace CODE_CONVERSION_NAMESPACE
};  // namespace raft