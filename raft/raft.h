#pragma once
#include <chrono>
#include <cstring>
#include <map>
#include <mutex>
#include <set>
#include <unordered_map>

#include "chunk.h"
#include "code_conversion.h"
#include "encoder.h"
#include "log_entry.h"
#include "log_manager.h"
#include "raft_struct.h"
#include "raft_type.h"
#include "rpc.h"
#include "rsm.h"
#include "util.h"

namespace raft {

class Storage;

enum RaftRole {
  kFollower = 1,
  kCandidate = 2,
  kPreLeader = 3,
  kLeader = 4,
};

namespace config {
const int64_t kHeartbeatInterval = 100;         // 100ms
const int64_t kCollectFragmentsInterval = 100;  // 100ms
const int64_t kReplicateInterval = 10;
const int64_t kElectionTimeoutMin = 500;  // 500ms
constexpr int kLivenessTimeoutInterval = 200;
const int64_t kElectionTimeoutMax = 1000;  // 800ms
};                                         // namespace config

struct RaftConfig {
  // The node id of curernt peer. A node id is the unique identifier to
  // distinguish different raft peers
  raft_node_id_t id;

  // The raft node id and corresponding network address of all raft peers
  // in current cluster. (including current server itself)
  std::unordered_map<raft_node_id_t, rpc::RpcClient *> rpc_clients;

  // Persistence storage, which is used to recover from failure, could be
  // nullptr. If storage is nullptr, any change to RaftState will not be
  // persisted
  Storage *storage;
  // Storage for storing reserved chunks
  Storage *reserve_storage;

  int64_t electionTimeMin, electionTimeMax;

  Rsm *rsm;
};

struct ProposeResult {
  raft_index_t propose_index;
  raft_term_t propose_term;
  bool is_leader;
};

// A monitor that records the number of server that is still alive in current
// cluster
struct LivenessMonitor {
  static constexpr int kMaxNodeNum = 10;
  int node_num;
  bool response[kMaxNodeNum];
  uint64_t response_time[kMaxNodeNum];
  raft_node_id_t me;  // current server's id
  util::Timer timer;

  // void Init() { std::memset(response, true, sizeof(response)); }

  void Init() {
    timer.Reset();
    response[me] = true;
    response_time[me] = 0;
  }

  // The input parameter num does not contain the leader itself.
  void SetLivenessNumber(int num) {
    assert(num <= node_num);
    int record_num = 0;
    for (int i = 0; i < node_num; ++i) {
      // use num-1 since "me" is always alive
      if (record_num < num - 1 && i != me) {
        response[i] = true;
        record_num++;
      } else {
        response[i] = false;
      }
      response_time[i] = timer.ElapseMilliseconds();
    }
    response[me] = true;
  }

  void UpdateLiveness(raft_node_id_t id) {
    response[id] = true;
    response_time[id] = timer.ElapseMilliseconds();

    // Update other server's state
    auto elapsed = response_time[id];
    for (int i = 0; i < node_num; ++i) {
      if (response[i] && (elapsed - response_time[i]) < config::kLivenessTimeoutInterval) {
        response[i] = true;
      } else {
        response[i] = false;
      }
    }
    // "me" is always alive
    response[me] = true;
  }

  int LiveNumber() const {
    int cnt = 0;
    for (int i = 0; i < node_num; ++i) {
      cnt += (response[i]);
    }
    return cnt;
  }

  // void UpdateLivenessState() {
  //   auto elapsed = timer.ElapseMilliseconds();
  //   for (int i = 0; i < node_num; ++i) {
  //     if (response[i] && (elapsed - response_time[i]) < 100) {
  //       response[i] = true;
  //     } else {
  //       response[i] = false;
  //     }
  //   }
  //   response[me] = true;
  // }

  bool IsAlive(raft_node_id_t target_id) const { return response[target_id]; }

  std::vector<bool> GetLivenessVector() const {
    return std::vector<bool>(response, response + node_num);
  }
};

struct SequenceGenerator {
 public:
  void Reset() { seq = 1; }
  uint64_t Next() { return seq++; }

 private:
  uint64_t seq;
};

struct PreLeaderStripeStore {
  PreLeaderStripeStore() = default;

  // [start_index, end_index] is the range of index that preLeader collects
  raft_index_t start_index, end_index;
  std::vector<Stripe> stripes;
  bool response_[15];
  int node_num;
  raft_node_id_t me;

  void InitRequestFragmentsTask(raft_index_t start, raft_index_t end, int node_num,
                                raft_node_id_t me) {
    this->start_index = start;
    this->end_index = end;
    this->node_num = node_num;
    this->me = me;
    stripes.clear();
    stripes.reserve(end - start + 1);

    int stripe_cnt = end - start + 1;
    for (int i = 0; i < stripe_cnt; ++i) {
      stripes.push_back(Stripe());
    }

    // Initiate stripe
    for (auto stripe : stripes) {
      // stripe.Init();
    }

    memset(response_, false, sizeof(response_));
    response_[me] = true;
  }

  void UpdateResponseState(raft_node_id_t id) { response_[id] = true; }

  bool IsCollected(raft_node_id_t id) const { return response_[id]; }

  int CollectedFragmentsCnt() const {
    int ret = 0;
    for (int i = 0; i < node_num; ++i) {
      ret += response_[i];
    }
    return ret;
  }

  void AddFragments(raft_index_t idx, const LogEntry &entry, raft_frag_id_t chunk_id) {
    if (idx < start_index || idx > end_index) {
      // NOTE: idx > end_index indicates that current leader receives an entry
      // with index higher than leader's last index, in that way, it simply cut
      // off these entries because the majority of the servers doesn't have this
      // entry(otherwise the leader won't win this election)
      return;
    }
    auto array_index = idx - start_index;
    stripes[array_index].collected_fragments.insert_or_assign(chunk_id, entry);
  }
};

// A raft peer maintains the necessary information in terms of "Logic" state
// of raft algorithm
class RaftPeer {
 public:
  RaftPeer() : next_index_(0), match_index_(0) {}

  raft_index_t NextIndex() const { return next_index_; }
  void SetNextIndex(raft_index_t next_index) { next_index_ = next_index; }

  raft_index_t MatchIndex() const { return match_index_; }
  void SetMatchIndex(raft_index_t match_index) { match_index_ = match_index; }

 public:
  raft_index_t next_index_, match_index_;
  std::unordered_map<raft_index_t, ChunkInfo> matchChunkInfo;
};

class RaftState {
 public:
  // Construct a RaftState instance from a specified configuration.
  static RaftState *NewRaftState(const RaftConfig &);
  static const raft_node_id_t kNotVoted = -1;

 public:
  RaftState() = default;

  RaftState(const RaftState &) = delete;
  RaftState &operator=(const RaftState &) = delete;

 public:
  // Process a bunch of RPC request or response, the first parameter is the
  // input of this process, the second parameter is the output.
  void Process(RequestVoteArgs *args, RequestVoteReply *reply);
  void Process(RequestVoteReply *reply);

  void Process(AppendEntriesArgs *args, AppendEntriesReply *reply);
  void Process(AppendEntriesReply *reply);

  void ProcessCodeConversion(AppendEntriesArgs *args, AppendEntriesReply *reply);
  void ProcessCodeConversion(AppendEntriesReply *reply);

  void Process(RequestFragmentsArgs *args, RequestFragmentsReply *reply);
  void Process(RequestFragmentsReply *reply);

  // This is a command from upper level application, the raft instance is
  // supposed to copy this entry to its own log and replicate it to other
  // followers
  ProposeResult Propose(const CommandData &command);

  raft::raft_index_t LastIndex() {
    std::scoped_lock<std::mutex> mtx(this->mtx_);
    return lm_->LastLogEntryIndex();
  }

 public:
  // Init all necessary status of raft state, including reset election timer
  void Init();

  // The driver clock periodically call the tick function to so that raft peer
  // make progress
  void Tick();

  raft_term_t CurrentTerm() const { return current_term_; }
  void SetCurrentTerm(raft_term_t term) { current_term_ = term; }

  raft_node_id_t VoteFor() const { return vote_for_; }
  void SetVoteFor(raft_node_id_t node) { vote_for_ = node; }

  RaftRole Role() const { return role_; }
  void SetRole(RaftRole role) { role_ = role; }

  // ALERT: This public interface should only be used in test case
  void SetVoteCnt(int cnt) { vote_me_cnt_ = cnt; }

  raft_index_t CommitIndex() const { return commit_index_; }
  void SetCommitIndex(raft_index_t raft_index) { commit_index_ = raft_index; }

  raft_index_t LastLogIndex() const { return lm_->LastLogEntryIndex(); }
  raft_term_t TermAt(raft_index_t raft_index) const { return lm_->TermAt(raft_index); }

  int GetClusterServerNumber() const { return peers_.size() + 1; }

  uint64_t CommitLatency(raft_index_t raft_index) const {
    if (commit_elapse_time_.count(raft_index) == 0) {
      return -1;
    } else {
      return commit_elapse_time_.at(raft_index);
    }
  }

 public:
  // Check specified raft_index and raft_term is newer than log entries stored
  // in current raft peer. Return true if it is, otherwise returns false
  bool isLogUpToDate(raft_index_t raft_index, raft_term_t raft_term);

  // Check if current raft peer has exactly an entry of specified raft_term at
  // specific raft_index
  bool containEntry(raft_index_t raft_index, raft_term_t raft_term, raft_encoding_param_t prev_k);

  // When receiving AppendEntries Reply, the raft peer checks all peers match
  // index condition and may update the commit_index field
  void tryUpdateCommitIndex();

  void tryApplyLogEntries();

  void tryApplyLogEntriesCodeConversion();

  // Encoding specified log entry with encoding parameter k, m, the results is
  // written into specified stripe
  void EncodeRaftEntry(raft_index_t raft_index, raft_encoding_param_t k, raft_encoding_param_t m,
                       Stripe *stripe);

  void EncodeRaftEntryForCodeConversion(raft_index_t raft_index, const std::vector<bool> &live_vec,
                                        CODE_CONVERSION_NAMESPACE::CodeConversionManagement *ccm,
                                        Stripe *stripe, StaticEncoder *encoder);
  // Adjust the Chunk distribution for a single entry at specific index position
  void AdjustChunkDistributionCodeConversion(raft_index_t raft_index,
                                             const std::vector<bool> &live_vec,
                                             raft_encoding_param_t code_conversion_k);

  // Decoding all fragments contained in a stripe into a complete log entry
  bool DecodingRaftEntry(Stripe *stripe, LogEntry *ent);

  bool NeedOverwriteLogEntry(const ChunkInfo &old_info, const ChunkInfo &new_info);

  void FilterDuplicatedCollectedFragments(Stripe &stripes);

  bool FindFullEntryInStripe(const Stripe *stripe, LogEntry *ent);

  // Iterate through the entries carried by input args and check if there is
  // conflicting entry: Same index but different term. If there is one, delete
  // all following entries. Add any new entries that are not in raft's log
  void CheckConflictEntryAndAppendNew(AppendEntriesArgs *args, AppendEntriesReply *reply);

  // A specialized version (for CodeConversion) of CheckConflictAndAppendNew
  void CheckConflictEntryAndAppendNewCodeConversion(AppendEntriesArgs *args,
                                                    AppendEntriesReply *reply);

  // Reset the next index and match index fields when current server becomes
  // leader
  void resetNextIndexAndMatchIndex();

  uint32_t NextSequence() { return seq_gen_.Next(); }

  void tickOnFollower();
  void tickOnCandidate();
  void tickOnLeader();
  void tickOnPreLeader();

  void resetElectionTimer();
  void resetHeartbeatTimer();
  void resetPreLeaderTimer();
  void resetReplicationTimer();

  void convertToFollower(raft_term_t term);
  void convertToCandidate();
  void convertToLeader();
  void convertToPreLeader();

  void PersistRaftState();

  // A private function that is used to start a new election
  void startElection();

  // Replicate entries to all other raft peers
  void broadcastHeartbeat();

  // Collect all needed fragments
  void collectFragments();

  void incrementVoteMeCnt() { vote_me_cnt_++; }

  // For a cluster consists of 2F+1 server, F is called the liveness
  // level, which is the maximum number of failure servers the cluster
  // can tolerant
  int livenessLevel() const { return peers_.size() / 2; }

  // Send heartbeat messages to target raft peer
  void sendHeartBeat(raft_node_id_t peer);

  // Send appendEntries messages to target raft peer
  void sendAppendEntries(raft_node_id_t peer);

  // Send appendEntries specialized for code conversion feature
  void sendAppendEntriesCodeConversion(raft_node_id_t peer);

  void initLivenessMonitorState() { live_monitor_.Init(); }

  // In flexibleK, the leader needs to send AppendEntries arguments in every
  // heartbeat round
  // void replicateEntries();

  // The preleader will try becoming leader if all requested fragments are
  // decoded into complete log entries
  void PreLeaderBecomeLeader();

  void DecodeCollectedStripe();

  // Replicate a new proposed entry indexed by specified raft_index to alive
  // servers
  // [Require]: Given entry has already been added into log
  void ReplicateNewProposeEntry(raft_index_t raft_index);

  // Replicate a new proposed entry in the code conversion manner
  void ReplicateNewProposeEntryCodeConversion(raft_index_t raft_index);

  // This process checks if re-encoding is needed for each uncommitted entry. If
  // it is, re-encoding and replicate entries to all followers; otherwise,
  // simply replicate entries according to the NextIndex of each followers
  void ReplicateEntries();

  void ReplicateEntriesCodeConversion();

  // Some re-encoding work might by needed due to number of alive servers has
  // been changed.
  void MaybeReEncodingAndReplicate();

  // The uncommitted entries may need to adjust their chunk distribution accordingly
  void MaybeAdjustDistributionAndReplicate(const std::vector<bool> &live_vec);

  void UpdateLastEncodingK(raft_index_t raft_index, raft_encoding_param_t k) {
    last_encoding_.insert_or_assign(raft_index, k);
  }

  auto GetLastEncodingK(raft_index_t raft_index) -> raft_encoding_param_t {
    // Returns 0 means this entry has not been encoded yet
    // There are two cases for a given raft index and its associated k
    // 1. The entry is complete, we shall see its current encoding k
    // 2. The entry is a chunk, we shall directly returns its k in chunk info
    auto ent = lm_->GetSingleLogEntry(raft_index);
    if (ent == nullptr) {
      return 0;
    }

    switch (ent->Type()) {
      case kNormal: {
        if (last_encoding_.count(raft_index) == 0) {
          return 0;
        }
        return last_encoding_[raft_index];
      }
      case kFragments: {
        return ent->GetChunkInfo().GetK();
      }
      default:
        return 0;
    }
  }

  int AliveServersOfLastPoint() const { return alive_servers_of_last_point_; }
  void UpdateAliveServers(int num) { alive_servers_of_last_point_ = num; }

  void SetLivenessNumber(int num) { live_monitor_.SetLivenessNumber(num); }

 public:
  // For concurrency control. A raft state instance might be accessed via
  // multiple threads, e.g. RPC thread that receives request; The state machine
  // thread that peridically apply committed log entries, and so on
  std::mutex mtx_;

  // The id of current raft peer
  raft_node_id_t id_;

  // Record current raft peer's state is Follower, or Candidate, or Leader
  RaftRole role_;

  // Current Term of raft peer, initiated to be 0 when first bootsup
  raft_term_t current_term_;

  // The peer that this peer has voted in current term, initiated to be -1
  // when first bootsup
  raft_node_id_t vote_for_;

  // The raft index of log entry that has been committed and applied to state
  // machine, does not need persistence
  raft_index_t commit_index_;
  raft_index_t last_applied_;

  // Manage all log entries
  LogManager *lm_;
  Storage *storage_;

  // LogManager and storage interface for reservation chunks
  LogManager *reserve_lm_;
  Storage *reserve_storage_;
  StaticEncoder *static_encoder_ = nullptr;

  // For FlexibleK and CRaft: We need to detect the number of live servers
  LivenessMonitor live_monitor_;
  Encoder encoder_;
  SequenceGenerator seq_gen_;
  // For each index, there is an associated stripe that contains the encoded
  // data
  std::map<raft_index_t, Stripe *> encoded_stripe_;

  // For each index, the leader records the distribution information of each chunk splitted
  // from the entry
  std::unordered_map<raft_index_t, CODE_CONVERSION_NAMESPACE::CodeConversionManagement *>
      cc_managment_;

  // For each index, last_encoding contains the most recent encoding parameters
  // k since it determines if there is a newer version of encoding
  std::unordered_map<raft_index_t, raft_encoding_param_t> last_encoding_;

  // A place for storing fragments come from RequestFragments
  PreLeaderStripeStore preleader_stripe_store_;

 public:
  std::set<raft_node_id_t> peers_;
  RaftPeer *raft_peer_[32] = {nullptr};
  rpc::RpcClient *rpc_clients_[32] = {nullptr};
  // std::unordered_map<raft_node_id_t, RaftPeer *> peers_;
  // std::unordered_map<raft_node_id_t, rpc::RpcClient *> rpc_clients_;

  util::Timer election_timer_;   // Record elapse time during election
  util::Timer heartbeat_timer_;  // Record elapse time since last heartbeat
  util::Timer preleader_timer_;  // Record fragments collection time
  util::Timer replicate_timer_;  // Record replication timer

  // Election time should be between [min, max), set by configuration
  int64_t electionTimeLimitMin_, electionTimeLimitMax_;
  // A randomized election timeout based on above interval
  int64_t election_time_out_;
  int64_t heartbeatTimeInterval;

  // For calculating the commit latency
  std::unordered_map<raft_index_t, util::TimePoint> commit_start_time_;

  // Elapse time of microseconds
  std::unordered_map<raft_index_t, uint64_t> commit_elapse_time_;

  int alive_servers_of_last_point_;

 private:
  int vote_me_cnt_;
  Rsm *rsm_;

  // Some report information about preleader phase
  util::TimePoint preleader_timepoint_;
  uint64_t preleader_recover_ent_cnt_ = 0;
};
}  // namespace raft
