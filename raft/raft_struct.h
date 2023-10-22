#pragma once
#include <vector>

#include "log_entry.h"
#include "raft_type.h"

namespace raft {
struct RequestVoteArgs {
  raft_term_t term;

  raft_node_id_t candidate_id;

  raft_index_t last_log_index;

  raft_term_t last_log_term;
};

struct RequestVoteReply {
  raft_term_t term;

  int vote_granted;

  raft_node_id_t reply_id;
};

struct AppendEntriesArgs {
  // Leader's term when sending this AppendEntries RPC.
  raft_term_t term;

  // The leader's identifier
  raft_node_id_t leader_id;

  raft_index_t prev_log_index;
  raft_term_t prev_log_term;
  raft_encoding_param_t prev_k;

  raft_index_t leader_commit;

  int64_t entry_cnt;
  std::vector<LogEntry> entries;
};

struct AppendEntriesReply {
  // The raft term of the server when processing one AppendEntries RPC call.
  // Used to update the leader's term
  raft_term_t term;

  // Denote if the follower successfully append specified log entries to its
  // own log manager. Return 1 if append is successful, otherwise returns 0
  int success;

  // The next raft index the raft peer wants the leader to send. If success is
  // true, the expect_index is prev_log_index + entry_cnt + 1; otherwise it is
  // the first index that differs from the leader
  raft_index_t expect_index;

  // The raft node id of the server that makes this reply
  raft_node_id_t reply_id;

  uint32_t padding;

  int chunk_info_cnt;
  std::vector<ChunkInfo> chunk_infos;
};

struct RequestFragmentsArgs {
  // The term when leader(Or pre-leader) sends out this RequestFragments RPC to
  // collect fragments in order to recover the original entry contents
  raft_term_t term;

  raft_node_id_t leader_id;

  // [start, last] specifies the range of fragments the pre-leader process
  // requires.
  raft_index_t start_index, last_index;
};

struct RequestFragmentsReply {
  raft_node_id_t reply_id;

  raft_term_t term;

  // If there is some replied fragment entries, the start index of it, it should
  // be exactly the same index as that is contained in corresponding
  // RequestFragmentsArgs
  raft_index_t start_index;

  // Request Fragments may fail, e.g. If requested server has higher term, which
  // invalidates the leadership of current leader
  int success;

  int entry_cnt;
  std::vector<LogEntry> fragments;
};

struct DeleteSubChunksArgs {
  // We use term to ensure that this message is not a out-of-date message
  raft_term_t term;

  // Identify the range of Log entries to be deleted
  raft_index_t start_index, last_index;

  // Any node, no matter followers or leaders can issue this RPC
  raft_node_id_t node_id;

  // Delete all subchunks encoded from this Chunk
  int chunk_id;
};

struct DeleteSubChunksReply {
  raft_node_id_t reply_id;

  raft_term_t term;

  int success;
};

// A struct that indicates the command specified by user of the raft cluster
struct CommandData {
  int start_fragment_offset;
  // The ownership of data contained in this command_data is handled to Raft, if
  // you call RaftState->Process(..)
  Slice command_data;
};

enum {
  // kAppendEntriesArgsHdrSize = sizeof(raft_term_t) * 2 + sizeof(raft_index_t)
  // * 2 +
  // sizeof(uint64_t) * 2 + sizeof(raft_node_id_t)
  kAppendEntriesArgsHdrSize = 32,
  kAppendEntriesReplyHdrSize = sizeof(raft_term_t) + sizeof(int) + sizeof(uint32_t) +
                               sizeof(raft_node_id_t) + sizeof(raft_index_t) + sizeof(int),
  kRequestFragmentsReplyHdrSize =
      sizeof(raft_node_id_t) + sizeof(raft_term_t) + sizeof(raft_index_t) + sizeof(int) * 2,
};

}  // namespace raft
