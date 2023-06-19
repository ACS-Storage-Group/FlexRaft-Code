#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "client.h"
#include "config.h"
#include "kv_node.h"
#include "log_manager.h"
#include "rpc.h"
#include "type.h"
#include "util.h"

using KvPair = std::pair<std::string, std::string>;
const int kVerboseInterval = 100;

enum YCSBOpType {
  kPut = 0,
  kGet = 1,
};

enum YCSBBenchType {
  YCSB_A = 0,
  YCSB_B = 1,
  YCSB_C = 2,
  YCSB_D = 3,
  YCSB_F = 4,
};

static const char *YCSBTypeToString(YCSBOpType type) {
  switch (type) {
  case kPut:
    return "Put";
  case kGet:
    return "Get";
  default:
    assert(false);
  }
}

struct YCSBOperation {
  YCSBOpType type;
  std::string key;
  std::string value;
};

struct BenchConfiguration {
  int bench_put_cnt;
  int bench_put_size;
  int put_proportion;
  YCSBBenchType bench_type;
};

struct AnalysisResults {
  uint64_t op_latency_avg_put;
  uint64_t commit_latency_avg_put;
  uint64_t apply_latency_avg_put;

  uint64_t op_latency_avg_get;
  uint64_t commit_latency_avg_get;
  uint64_t apply_latency_avg_get;
};

struct OperationStat {
  YCSBOpType type;
  uint64_t op_latency;
  uint64_t commit_latency;
  uint64_t apply_latency;

  std::string ToString() const {
    char buf[512];
    sprintf(buf,
            "[Type = %s][OpLatency = %llu us][CommitLatency = %llu "
            "us][ApplyLatency = "
            "%llu us]",
            YCSBTypeToString(type), op_latency, commit_latency, apply_latency);
    return std::string(buf);
  }
};

void Dump(const std::vector<OperationStat> &op_stat, std::ofstream &of) {
  for (const auto &stat : op_stat) {
    of << stat.ToString() << "\n";
  }
}

AnalysisResults Analysis(const std::vector<OperationStat> &collected_data) {
  uint64_t op_latency_sum_put = 0, op_latency_sum_get = 0;
  uint64_t commit_latency_sum_put = 0, commit_latency_sum_get = 0;
  uint64_t apply_latency_sum_put = 0, apply_latency_sum_get = 0;
  uint64_t put_cnt = 0, get_cnt = 0;
  std::for_each(collected_data.begin(), collected_data.end(),
                [&](const OperationStat &stat) {
                  if (stat.type == kPut) {
                    op_latency_sum_put += stat.op_latency;
                    apply_latency_sum_put += stat.apply_latency;
                    commit_latency_sum_put += stat.commit_latency;
                    put_cnt += 1;
                  } else {
                    op_latency_sum_get += stat.op_latency;
                    apply_latency_sum_get += stat.apply_latency;
                    commit_latency_sum_get += stat.commit_latency;
                    get_cnt += 1;
                  }
                });
  //  In case divide by zero exception
  if (put_cnt == 0)
    put_cnt = 1;
  if (get_cnt == 0)
    get_cnt = 1;
  return AnalysisResults{
      op_latency_sum_put / put_cnt,     commit_latency_sum_put / put_cnt,
      apply_latency_sum_put / put_cnt,  op_latency_sum_get / get_cnt,
      commit_latency_sum_get / get_cnt, apply_latency_sum_get / get_cnt,
  };
}

void BuildBench(const BenchConfiguration &cfg,
                std::vector<YCSBOperation> *bench) {
  const size_t kKeyRange = 1000;
  ScrambledZipfianGenerator generator(kKeyRange);
  for (int i = 1; i <= cfg.bench_put_cnt; ++i) {
    auto key_id = generator.Next();
    auto key = raft::util::MakeKey(key_id, 64);
    auto value = std::string();
    auto op = rand() % 100;
    YCSBOpType type;
    if (op < cfg.put_proportion) {
      if (cfg.bench_type == YCSB_F) {
        bench->push_back({kGet, key, value});
      }
      type = kPut;
      value = raft::util::MakeValue(key_id, cfg.bench_put_size);
    } else {
      type = kGet;
    }
    bench->push_back({type, key, value});
  }
}

void ExecuteBench(kv::KvServiceClient *client, int client_id, int interval,
                  std::vector<OperationStat> &op_stats,
                  const std::vector<YCSBOperation> &bench) {

  std::printf("[Start Executing YCSB Benchmark]\n");
  op_stats.reserve(bench.size() / interval + 1);

  decltype(bench.size()) idx = client_id;
  int done_cnt = 0;

  uint64_t total_size;

  while (idx < bench.size()) {
    const auto &op = bench[idx];
    switch (op.type) {
    case kPut: {
      total_size += (op.key.size() + op.value.size());

      auto start = raft::util::NowTime();
      auto stat = client->Put(op.key, op.value);
      auto dura = raft::util::DurationToMicros(start, raft::util::NowTime());
      if (stat.err == kv::kOk) {
        op_stats.push_back(OperationStat{kPut, static_cast<uint64_t>(dura),
                                         stat.commit_elapse_time,
                                         stat.apply_elapse_time});
      } else {
        printf("[Put][Error Number]: %d\n", stat.err);
        exit(1);
      }
      break;
    }
    case kGet: {
      std::string get_val;
      auto start = raft::util::NowTime();
      auto stat = client->Get(op.key, &get_val);
      auto dura = raft::util::DurationToMicros(start, raft::util::NowTime());

      if (stat.err == kv::kOk) {
        op_stats.push_back(OperationStat{kGet, static_cast<uint64_t>(dura),
                                         stat.commit_elapse_time,
                                         stat.apply_elapse_time});
        total_size += (op.key.size() + get_val.size());
      } else {
        printf("[Get][Error Number]: %d\n", stat.err);
        exit(1);
      }
      break;
    }
    }
    done_cnt += 1;
    idx += interval;
    printf("[Client %d] Already Execute %d / %lu\r", client_id, done_cnt,
           bench.size() / interval);
  }

  // Calculate the total throughput
  uint64_t total_time = 0;
  for (const auto &op_stat : op_stats) {
    total_time += op_stat.op_latency;
  }

  double thpt =
      (static_cast<double>(total_size) / (1024 * 1024)) / total_time * 1e6 * 8;
  printf("\n[Client %d] Throughput: %.2lf Mbps\n", client_id, thpt);
}

// Arg1: file name of the configuration file
// Arg2: Number of clients
// Arg3: Value size
// Arg4: operation count
// Arg5: YCSB operation Type
int main(int argc, char *argv[]) {
  if (argc < 4) {
    std::cerr << "[Error] Expect at least one parameter, get" << argc
              << std::endl;
    return 0;
  }
  auto cluster_cfg = ParseConfigurationFile(std::string(argv[1]));
  auto client_num = std::stoi(argv[2]);

  auto val_size = ParseCommandSize(std::string(argv[3]));
  auto op_cnt = ParseCommandSize(std::string(argv[4]));

  // Define YCSB Type
  auto bench_type = YCSB_A;
  auto put_prop = 50;
  if (strcmp(argv[5], "YCSB_A") == 0) {
    bench_type = YCSB_A;
    put_prop = 50;
  } else if (strcmp(argv[5], "YCSB_B") == 0) {
    bench_type = YCSB_B;
    put_prop = 5;
  } else if (strcmp(argv[5], "YCSB_C") == 0) {
    bench_type = YCSB_C;
    put_prop = 0;
  } else if (strcmp(argv[5], "YCSB_D") == 0) {
    bench_type = YCSB_D;
    put_prop = 5;
  } else if (strcmp(argv[5], "YCSB_F") == 0) {
    bench_type = YCSB_F;
    put_prop = 50;
  }

  std::vector<YCSBOperation> bench;
  auto bench_cfg = BenchConfiguration{op_cnt, val_size, put_prop, bench_type};
  BuildBench(bench_cfg, &bench);

  std::thread threads[32];
  std::vector<OperationStat> stats[32];
  std::ofstream of;
  of.open("results");

  // Do warmup
  auto client = new kv::KvServiceClient(cluster_cfg, 0);
  for (int i = 0; i < 1000; ++i) {
    auto stat = client->Put(raft::util::MakeKey(i, 64),
                            raft::util::MakeValue(i, val_size));
    assert(stat.err == kv::kOk);
  }
  delete client;

  printf(">>>>>>> Warmup is done\n");

  for (int i = 1; i <= client_num; ++i) {
    const auto client_id = i;
    auto client = new kv::KvServiceClient(cluster_cfg, client_id);
    threads[client_id] = std::thread([&]() {
      ExecuteBench(client, client_id, client_num, stats[client_id], bench);
    });
  }

  for (int i = 1; i <= client_num; ++i) {
    threads[i].join();
    Dump(stats[i], of);
  }

  of.close();

  return 0;
}
