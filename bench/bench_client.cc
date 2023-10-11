#include <gflags/gflags.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "chunk.h"
#include "client.h"
#include "config.h"
#include "kv_node.h"
#include "log_manager.h"
#include "rpc.h"
#include "type.h"
#include "util.h"

DEFINE_string(conf, "", "Path to the cluster configuration file");
DEFINE_int32(id, -1, "The client Id");
DEFINE_string(size, "", "The size of the values");
DEFINE_int32(write_num, 0, "The number of write operations to execute");

using KvPair = std::pair<std::string, std::string>;
const int kVerboseInterval = 100;

const int K = 5;
const int chunk_cnt = raft::code_conversion::get_chunk_count(K);

struct BenchConfiguration {
  std::string key_prefix;
  std::string value_prefix;
  int bench_put_cnt;
  int bench_put_size;
};

struct AnalysisResults {
  uint64_t op_latency_avg;
  uint64_t commit_latency_avg;
  uint64_t apply_latency_avg;
};

struct OperationStat {
  uint64_t op_latency;
  uint64_t commit_latency;
  uint64_t apply_latency;

  std::string ToString() const {
    char buf[512];
    sprintf(buf,
            "[OpLatency = %" PRIu64 " us][CommitLatency = %" PRIu64
            " us][ApplyLatency = "
            "%" PRIu64 " us]",
            op_latency, commit_latency, apply_latency);
    return std::string(buf);
  }
};

void Dump(const std::vector<OperationStat> &op_stat, std::ofstream &of) {
  for (const auto &stat : op_stat) {
    of << stat.ToString() << "\n";
  }
}

AnalysisResults Analysis(const std::vector<OperationStat> &collected_data) {
  uint64_t op_latency_sum = 0, op_latency_num = 0;
  uint64_t commit_latency_sum = 0, commit_latency_num = 0;
  uint64_t apply_latency_sum = 0, apply_latency_num = 0;
  std::for_each(collected_data.begin(), collected_data.end(), [&](const OperationStat &stat) {
    op_latency_sum += stat.op_latency;
    apply_latency_sum += stat.apply_latency;
    commit_latency_sum += stat.commit_latency;
  });
  return AnalysisResults{op_latency_sum / collected_data.size(),
                         commit_latency_sum / collected_data.size(),
                         apply_latency_sum / collected_data.size()};
}

void BuildBench(const BenchConfiguration &cfg, std::vector<KvPair> *bench) {
  auto val_sz = round_up(cfg.bench_put_size, chunk_cnt);
  for (int i = 1; i <= cfg.bench_put_cnt; ++i) {
    auto key = cfg.key_prefix + std::to_string(i);
    auto val = cfg.value_prefix + std::to_string(i);
    val.append(val_sz - val.size() - sizeof(int), '0');
    bench->push_back({key, val});
  }
}

void ExecuteBench(kv::KvServiceClient *client, const std::vector<KvPair> &bench) {
  std::vector<OperationStat> op_stats;

  std::printf("[Execution Process]\n");

  for (int i = 0; i < bench.size(); ++i) {
    const auto &p = bench[i];
    auto start = raft::util::NowTime();
    auto stat = client->Put(p.first, p.second);
    auto dura = raft::util::DurationToMicros(start, raft::util::NowTime());
    if (stat.err == kv::kOk) {
      op_stats.push_back(OperationStat{static_cast<uint64_t>(dura), stat.commit_elapse_time,
                                       stat.apply_elapse_time});
    } else {
      printf("[Error Number]: %d", stat.err);
      break;
    }
    int done_cnt = i + 1;
    if (done_cnt > 0 && done_cnt % kVerboseInterval == 0) {
      std::cout << "\r[Already Execute " << done_cnt << " Ops]" << std::flush;
    }
  }

  puts("");

  auto [avg_latency, avg_commit_latency, avg_apply_latency] = Analysis(op_stats);

  printf("[Client Id %d]\n", client->ClientId());
  printf("[Results][Succ Cnt=%" PRIu64 "][Average Latency = %" PRIu64
         " us][Average Commit "
         "Latency = "
         "%" PRIu64 " us][Average Apply Latency = %" PRIu64 " us]\n",
         op_stats.size(), avg_latency, avg_commit_latency, avg_apply_latency);
  fflush(stdout);

  int succ_cnt = 0;
  // Check if inserted value can be found
  for (const auto &p : bench) {
    std::string get_val;
    auto stat = client->Get(p.first, &get_val);
    if (stat.err == kv::kOk && get_val == p.second) {
      ++succ_cnt;
    }
    // No need to continue executing the benchmark
    if (stat.err == kv::kRequestExecTimeout) {
      break;
    }
  }
  printf("[Get Results][Succ Count=%d]\n", succ_cnt);

  // Dump the results to file
  std::ofstream of;
  of.open("results");
  Dump(op_stats, of);
}

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto cluster_cfg = ParseConfigurationFile(FLAGS_conf);
  int client_id = FLAGS_id;

  auto key_prefix = "key-" + std::to_string(client_id);
  auto value_prefix = "value-" + std::to_string(client_id) + "-";
  auto val_size = ParseCommandSize(FLAGS_size);
  auto put_cnt = FLAGS_write_num;

  std::vector<KvPair> bench;
  auto bench_cfg = BenchConfiguration{key_prefix, value_prefix, put_cnt, val_size};
  BuildBench(bench_cfg, &bench);

  auto client = new kv::KvServiceClient(cluster_cfg, client_id);
  ExecuteBench(client, bench);

  delete client;
  return 0;
}
