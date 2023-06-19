#pragma once
#include "RCF/ByteBuffer.hpp"
#include "log_entry.h"
#include "raft_struct.h"
#include "raft_type.h"

namespace raft {
class Serializer {
 public:
  static Serializer NewSerializer();

 public:
  void Serialize(const LogEntry* entry, RCF::ByteBuffer* buffer);
  void Deserialize(const RCF::ByteBuffer* buffer, LogEntry* entry);

  void Serialize(const RequestVoteArgs* args, RCF::ByteBuffer* buffer);
  void Deserialize(const RCF::ByteBuffer* buffer, RequestVoteArgs* args);

  void Serialize(const RequestVoteReply* reply, RCF::ByteBuffer* buffer);
  void Deserialize(const RCF::ByteBuffer* buffer, RequestVoteReply* reply);

  void Serialize(const AppendEntriesArgs* args, RCF::ByteBuffer* buffer);
  void Deserialize(const RCF::ByteBuffer* buffer, AppendEntriesArgs* args);

  void Serialize(const AppendEntriesReply* reply, RCF::ByteBuffer* buffer);
  void Deserialize(const RCF::ByteBuffer* buffer, AppendEntriesReply* reply);

  void Serialize(const RequestFragmentsArgs* args, RCF::ByteBuffer* buffer);
  void Deserialize(const RCF::ByteBuffer* buffer, RequestFragmentsArgs* args);

  void Serialize(const RequestFragmentsReply* reply, RCF::ByteBuffer* buffer);
  void Deserialize(const RCF::ByteBuffer* buffer, RequestFragmentsReply* reply);

  size_t getSerializeSize(const LogEntry& entry);
  size_t getSerializeSize(const RequestVoteArgs& args);
  size_t getSerializeSize(const RequestVoteReply& reply);
  size_t getSerializeSize(const AppendEntriesArgs& args);
  size_t getSerializeSize(const AppendEntriesReply& reply);
  size_t getSerializeSize(const RequestFragmentsArgs& args);
  size_t getSerializeSize(const RequestFragmentsReply& reply);

  // Put/Parse a slice in prefix-length format at specified buf position and returns
  // with a pointer to the next position
  char* PutPrefixLengthSlice(const Slice& slice, char* buf);
  const char* ParsePrefixLengthSlice(const char* buf, Slice* slice);
  const char* ParsePrefixLengthSliceWithBound(const char* buf, size_t len, Slice* slice);

  char* serialize_logentry_helper(const LogEntry* entry, char* dst);
  const char* deserialize_logentry_helper(const char* src, LogEntry* entry);

  // Parse serialized data within [src, src + len) into a LogEntry, if the parse
  // successed, returns the next position to parse; otherwise returns nullptr.
  const char* deserialize_logentry_withbound(const char* src, size_t len,
                                             LogEntry* entry);
};
}  // namespace raft
