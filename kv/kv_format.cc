#include "kv_format.h"

#include <cstddef>
#include <cstring>

#include "raft_type.h"
#include "serializer.h"
#include "type.h"
#include "util.h"

namespace kv {
size_t GetRawBytesSizeForRequest(const Request &request) {
  size_t hdr_size = RequestHdrSize();
  size_t key_size = sizeof(int) + request.key.size();
  size_t val_size = sizeof(int) + request.value.size();
  return hdr_size + key_size + val_size;
}

void RequestToRawBytes(const Request &request, char *bytes) {
  std::memcpy(bytes, &request, RequestHdrSize());
  bytes = MakePrefixLengthKey(request.key, bytes + RequestHdrSize());
  MakePrefixLengthKey(request.value, bytes);
}

void RawBytesToRequest(char *bytes, Request *request) {
  std::memcpy(request, bytes, RequestHdrSize());
  bytes = GetKeyFromPrefixLengthFormat(bytes + RequestHdrSize(), &(request->key));
  GetKeyFromPrefixLengthFormat(bytes, &(request->value));
}

void RaftEntryToRequest(const raft::LogEntry &ent, Request *request, raft::raft_node_id_t server_id,
                        int server_num) {
  if (ent.Type() == raft::kNormal) {
    auto bytes = ent.CommandData().data();
    std::memcpy(request, bytes, RequestHdrSize());

    bytes = GetKeyFromPrefixLengthFormat(bytes + RequestHdrSize(), &(request->key));

    char tmp_data[12];
    *reinterpret_cast<int *>(tmp_data) = 1;
    *reinterpret_cast<int *>(tmp_data + 4) = 0;
    *reinterpret_cast<int *>(tmp_data + 8) = 0;

    for (int i = 0; i < 12; ++i) {
      request->value.push_back(tmp_data[i]);
    }

    LOG(raft::util::kRaft, "RaftEnt To Request: k=%d,m=%d,frag_id=%d", 1, 0, 0);

    // value would be the prefix length key format
    auto remaining_size = ent.CommandData().size() - (bytes - ent.CommandData().data());
    request->value.append(bytes, remaining_size);
  } else {
    // construct the header and key
    std::memcpy(request, ent.NotEncodedSlice().data(), RequestHdrSize());
    auto key_data = ent.NotEncodedSlice().data() + RequestHdrSize();
    GetKeyFromPrefixLengthFormat(key_data, &(request->key));

    // Construct the value, in the following format:
    // k, m, fragment_id, value_contents
    request->value.reserve(sizeof(int) * 3 + ent.FragmentSlice().size());

    char tmp_data[12];
    int k = ent.GetChunkInfo().GetK();
    int m = server_num - k;
    *reinterpret_cast<int *>(tmp_data) = k;
    *reinterpret_cast<int *>(tmp_data + 4) = m;
    *reinterpret_cast<int *>(tmp_data + 8) = static_cast<int>(server_id);

    LOG(raft::util::kRaft, "RaftEnt To Request: k=%d,m=%d,frag_id=%d", k, m, server_id);

    for (int i = 0; i < 12; ++i) {
      request->value.push_back(tmp_data[i]);
    }

    // Append the value contents
    request->value.append(ent.FragmentSlice().data(), ent.FragmentSlice().size());
  }
}

void RaftEntryToRequestCodeConversion(const raft::LogEntry &ent, Request *request,
                                      raft::raft_node_id_t server_id, int server_num) {
  // [k, m, server_id]
  char hdr_data[12];
  *(int *)hdr_data = ent.Type() == raft::kNormal ? 1 : ent.GetChunkInfo().GetK();
  *(int *)(hdr_data + 4) = ent.Type() == raft::kNormal ? 0 : server_num - ent.GetChunkInfo().GetK();
  *(int *)(hdr_data + 8) = ent.Type() == raft::kNormal ? 0 : (int)server_id;
  request->value.append(hdr_data, 12);

  if (ent.Type() == raft::kNormal) {
    auto bytes = ent.CommandData().data();
    std::memcpy(request, bytes, RequestHdrSize());

    bytes = GetKeyFromPrefixLengthFormat(bytes + RequestHdrSize(), &(request->key));

    LOG(raft::util::kRaft, "[CC] RaftEnt To Request: Normal");

    // value would be the prefix length key format
    auto remaining_size = ent.CommandData().size() - (bytes - ent.CommandData().data());
    request->value.append(bytes, remaining_size);
  } else {
    // construct the header and key
    std::memcpy(request, ent.NotEncodedSlice().data(), RequestHdrSize());
    auto key_data = ent.NotEncodedSlice().data() + RequestHdrSize();
    GetKeyFromPrefixLengthFormat(key_data, &(request->key));

    // Construct the value, in the following format:
    // k, m, fragment_id, value_contents
    // The value_contents is the serialized ChunkVector (of both original part and reserved part)

    // auto cv = ent.GetOriginalChunkVector();
    // cv.Concatenate(ent.GetReservedChunkVector());


    // Append the serialized CV contents
    // auto slice = cv.Serialize();
    // request->value.append(slice.data(), slice.size());
    // delete slice.data();

    // LOG(raft::util::kRaft, "[CC] RaftEnt To Request: ChunkVector Size=%d", cv.size());
  }
}

void RaftEntryToRequest(const raft::LogEntry &ent, Request *request) {
  if (ent.Type() == raft::kNormal) {
    auto bytes = ent.CommandData().data();
    std::memcpy(request, bytes, RequestHdrSize());

    bytes = GetKeyFromPrefixLengthFormat(bytes + RequestHdrSize(), &(request->key));

    char tmp_data[12];
    *reinterpret_cast<int *>(tmp_data) = 1;
    *reinterpret_cast<int *>(tmp_data + 4) = 0;
    *reinterpret_cast<int *>(tmp_data + 8) = 0;

    for (int i = 0; i < 12; ++i) {
      request->value.push_back(tmp_data[i]);
    }

    LOG(raft::util::kRaft, "RaftEnt To Request: k=%d,m=%d,frag_id=%d", 1, 0, 0);

    // value would be the prefix length key format
    auto remaining_size = ent.CommandData().size() - (bytes - ent.CommandData().data());
    request->value.append(bytes, remaining_size);
  } else {
    // construct the header and key
    std::memcpy(request, ent.NotEncodedSlice().data(), RequestHdrSize());
    auto key_data = ent.NotEncodedSlice().data() + RequestHdrSize();
    GetKeyFromPrefixLengthFormat(key_data, &(request->key));

    // Construct the value, in the following format:
    // k, m, fragment_id, value_contents
    request->value.reserve(sizeof(int) * 3 + ent.FragmentSlice().size());

    char tmp_data[12];
    // *reinterpret_cast<int *>(tmp_data) = ent.GetVersion().GetK();
    // *reinterpret_cast<int *>(tmp_data + 4) = ent.GetVersion().GetM();
    // *reinterpret_cast<int *>(tmp_data + 8) =
    // ent.GetVersion().GetFragmentId();
    //
    // LOG(raft::util::kRaft, "RaftEnt To Request: k=%d,m=%d,frag_id=%d",
    //     ent.GetVersion().GetK(), ent.GetVersion().GetM(),
    //     ent.GetVersion().GetFragmentId());

    for (int i = 0; i < 12; ++i) {
      request->value.push_back(tmp_data[i]);
    }

    // Append the value contents
    request->value.append(ent.FragmentSlice().data(), ent.FragmentSlice().size());
  }
}

}  // namespace kv
