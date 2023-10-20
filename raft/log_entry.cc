#include "log_entry.h"

#include "RCF/ByteBuffer.hpp"

namespace raft {

/* void LogEntry::serialize(SF::Archive &ar) { */
/*   ar &term; */
/*   ar &index; */
/*   ar &type; */
/*   ar &seq; */
/*   ar &n; */
/*   ar &k; */
/*   ar &fragment_id; */
/*   ar &start_fragment_offset; */
/*  */
/*   if (ar.isWrite()) { // Serializing */
/*     switch (Type()) { */
/*     case kNormal: */
/*       ar &command_data_.toString(); */
/*       return; */
/*  */
/*     case kFragments: */
/*       ar &not_encoded_slice_.toString(); */
/*       ar &fragment_slice_.toString(); */
/*       return; */
/*  */
/*     // Can never reach this */
/*     case kTypeMax: */
/*       assert(false); */
/*     } */
/*   } else { // Deserializing */
/*     std::string command_data; */
/*     std::string not_encoded_data, fragment_data; */
/*     switch (Type()) { */
/*     case kNormal: */
/*       ar &command_data; */
/*       SetCommandData(Slice(command_data)); */
/*       return; */
/*  */
/*     case kFragments: */
/*       ar &not_encoded_data; */
/*       ar &fragment_data; */
/*       SetNotEncodedSlice(Slice(not_encoded_data)); */
/*       SetFragmentSlice(Slice(fragment_data)); */
/*       return; */
/*  */
/*     // Can never reach this */
/*     case kTypeMax: */
/*       assert(false); */
/*     } */
/*   } */
/* } */

// This function is basically only used to test if network transfer is ok
auto operator==(const LogEntry &lhs, const LogEntry &rhs) -> bool {
  auto hdr_equal = lhs.Index() == rhs.Index() && lhs.Term() == rhs.Term() &&
                   lhs.Type() == rhs.Type() && lhs.GetChunkInfo() == rhs.GetChunkInfo();

  if (!hdr_equal) {
    return false;
  }

  auto lhs_cmd_data = lhs.CommandData(), rhs_cmd_data = rhs.CommandData();
  auto cmd_data_equal = !lhs_cmd_data.valid() && !rhs_cmd_data.valid();
  if (lhs_cmd_data.valid() && rhs_cmd_data.valid()) {
    cmd_data_equal = lhs_cmd_data.compare(rhs_cmd_data) == 0;
  }

  if (!cmd_data_equal) {
    return false;
  }

  auto lhs_full_data = lhs.NotEncodedSlice(), rhs_full_data = rhs.NotEncodedSlice();
  auto full_data_equal = !lhs_full_data.valid() && !rhs_full_data.valid();
  if (lhs_full_data.valid() && rhs_full_data.valid()) {
    full_data_equal = lhs_full_data.compare(rhs_full_data) == 0;
  }
  if (!full_data_equal) {
    return false;
  }

  auto lhs_frag_data = lhs.FragmentSlice(), rhs_frag_data = rhs.FragmentSlice();
  auto frag_equal = !lhs_frag_data.valid() && !rhs_frag_data.valid();
  if (lhs_frag_data.valid() && rhs_frag_data.valid()) {
    frag_equal = lhs_frag_data.compare(rhs_frag_data) == 0;
  }

  if (!frag_equal) {
    return false;
  }

  // Compare the two ChunkVector
  return lhs.GetOriginalChunkVector() == rhs.GetOriginalChunkVector() &&
         lhs.GetReservedChunkVector() == rhs.GetReservedChunkVector() &&
         lhs.GetSubChunkVec() == rhs.GetSubChunkVec();

  // // Check if sizes equal to each other
  // auto cv_equal = lhs.GetOriginalChunkVector().size() == rhs.GetOriginalChunkVector().size();
  // if (!cv_equal) {
  //   return false;
  // }
  // // Check if each element equal to each other
  // for (int i = 0; i < lhs.GetOriginalChunkVector().size(); ++i) {
  //   if (lhs.GetOriginalChunkVector().chunk_at(i) != rhs.GetOriginalChunkVector().chunk_at(i)) {
  //     return false;
  //   }
  // }
  // return true;
}
}  // namespace raft
