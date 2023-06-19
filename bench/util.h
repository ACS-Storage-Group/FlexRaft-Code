#pragma once
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

#include <cassert>
#include <mutex>
#include <random>

#include "config.h"
#include "kv_node.h"
#include "rpc.h"

inline auto ParseNetAddress(const std::string &net_addr)
    -> raft::rpc::NetAddress {
  auto specifier = net_addr.find(":");
  auto port_str =
      net_addr.substr(specifier + 1, net_addr.size() - specifier - 1);
  return raft::rpc::NetAddress{
      net_addr.substr(0, specifier),
      static_cast<uint16_t>(std::stoi(port_str, nullptr))};
}

inline auto ParseCommandSize(const std::string &size_str) -> int {
  if (std::isdigit(size_str.back())) {
    return std::stoi(size_str);
  }
  auto num = std::stoi(size_str.substr(0, size_str.size() - 1));
  switch (size_str.back()) {
  case 'K':
  case 'k':
    return num * 1024;
  case 'M':
  case 'm':
    return num * 1024 * 1024;
  case 'G':
    return num * 1024 * 1024 * 1024;
  default:
    return 0;
  }
}

// This is an interface for parsing cluster configurations of config file
inline auto ParseConfigurationFile(const std::string &filename)
    -> kv::KvClusterConfig {
  std::ifstream cfg(filename);
  kv::KvClusterConfig cluster_cfg;

  std::string node_id;
  std::string raft_rpc_addr;
  std::string kv_rpc_addr;
  std::string logname;
  std::string dbname;

  while (cfg >> node_id >> raft_rpc_addr >> kv_rpc_addr >> logname >> dbname) {
    kv::KvServiceNodeConfig cfg;
    cfg.id = std::stoi(node_id);
    cfg.raft_rpc_addr = ParseNetAddress(raft_rpc_addr);
    auto addr = ParseNetAddress(kv_rpc_addr);
    cfg.kv_rpc_addr = kv::rpc::NetAddress{addr.ip, addr.port};
    cfg.raft_log_filename = logname;
    // cfg.raft_log_filename = "";
    cfg.kv_dbname = dbname;

    cluster_cfg.insert({cfg.id, cfg});
  }

  return cluster_cfg;
}

// An abstract interface that generates keys
class KeyGenerator {
public:
  virtual uint64_t Next() = 0;
};

class SequentialGenerator : public KeyGenerator {
public:
  SequentialGenerator(uint64_t min, uint64_t max)
      : min_(min), max_(max), current_(min) {}
  uint64_t Next() override {
    auto ret = current_;
    current_ = (current_ + 1 - min_) % (max_ - min_) + min_;
    return ret;
  }

  uint64_t min_;
  uint64_t max_;
  uint64_t current_;
};

class RandomGenerator : public KeyGenerator {
public:
  RandomGenerator(uint64_t min, uint64_t max)
      : distr_(min, max), generator_(rand_dev_()) {}

  uint64_t Next() override { return distr_(generator_); }

private:
  std::uniform_int_distribution<uint64_t> distr_;
  std::random_device rand_dev_;
  std::mt19937 generator_;
};

namespace utils {

const uint64_t kFNVOffsetBasis64 = 0xCBF29CE484222325ull;
const uint64_t kFNVPrime64 = 1099511628211ull;

inline uint64_t FNVHash64(uint64_t val) {
  uint64_t hash = kFNVOffsetBasis64;

  for (int i = 0; i < 8; i++) {
    uint64_t octet = val & 0x00ff;
    val = val >> 8;

    hash = hash ^ octet;
    hash = hash * kFNVPrime64;
  }
  return hash;
}

inline uint64_t Hash(uint64_t val) { return FNVHash64(val); }

inline uint32_t ThreadLocalRandomInt() {
  static thread_local std::random_device rd;
  static thread_local std::minstd_rand rn(rd());
  return rn();
}

inline double ThreadLocalRandomDouble(double min = 0.0, double max = 1.0) {
  static thread_local std::random_device rd;
  static thread_local std::minstd_rand rn(rd());
  static thread_local std::uniform_real_distribution<double> uniform(min, max);
  return uniform(rn);
}
} // namespace utils

class ZipfianGenerator : public KeyGenerator {
public:
  constexpr static const double kZipfianConst = 0.99;
  static const uint64_t kMaxNumItems = (UINT64_MAX >> 24);

  ZipfianGenerator(uint64_t min, uint64_t max,
                   double zipfian_const = kZipfianConst)
      : num_items_(max - min + 1), base_(min), theta_(zipfian_const),
        zeta_n_(0), n_for_zeta_(0) {
    assert(num_items_ >= 2 && num_items_ < kMaxNumItems);
    zeta_2_ = Zeta(2, theta_);
    alpha_ = 1.0 / (1.0 - theta_);
    RaiseZeta(num_items_);
    eta_ = Eta();

    Next();
  }

  ZipfianGenerator(uint64_t num_items)
      : ZipfianGenerator(0, num_items - 1, kZipfianConst) {}

  uint64_t Next(uint64_t num) {
    assert(num >= 2 && num < kMaxNumItems);
    // std::lock_guard<std::mutex> lock(mutex_);

    if (num > n_for_zeta_) { // Recompute zeta_n and eta
      RaiseZeta(num);
      eta_ = Eta();
    }

    double u = utils::ThreadLocalRandomDouble();
    double uz = u * zeta_n_;

    if (uz < 1.0) {
      return last_value_ = 0;
    }

    if (uz < 1.0 + std::pow(0.5, theta_)) {
      return last_value_ = 1;
    }

    return last_value_ = base_ + num * std::pow(eta_ * u - eta_ + 1, alpha_);
  }

  uint64_t Next() override { return Next(num_items_); }

  uint64_t Last() {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_value_;
  }

private:
  ///
  /// Compute the zeta constant needed for the distribution.
  /// Remember the number of items, so if it is changed, we can recompute zeta.
  ///
  void RaiseZeta(uint64_t num) {
    assert(num >= n_for_zeta_);
    zeta_n_ = Zeta(n_for_zeta_, num, theta_, zeta_n_);
    n_for_zeta_ = num;
  }

  double Eta() {
    return (1 - std::pow(2.0 / num_items_, 1 - theta_)) /
           (1 - zeta_2_ / zeta_n_);
  }

  ///
  /// Calculate the zeta constant needed for a distribution.
  /// Do this incrementally from the last_num of items to the cur_num.
  /// Use the zipfian constant as theta. Remember the new number of items
  /// so that, if it is changed, we can recompute zeta.
  ///
  static double Zeta(uint64_t last_num, uint64_t cur_num, double theta,
                     double last_zeta) {
    double zeta = last_zeta;
    for (uint64_t i = last_num + 1; i <= cur_num; ++i) {
      zeta += 1 / std::pow(i, theta);
    }
    return zeta;
  }

  static double Zeta(uint64_t num, double theta) {
    return Zeta(0, num, theta, 0);
  }

  uint64_t num_items_;
  uint64_t base_; /// Min number of items to generate

  // Computed parameters for generating the distribution
  double theta_, zeta_n_, eta_, alpha_, zeta_2_;
  uint64_t n_for_zeta_; /// Number of items used to compute zeta_n
  uint64_t last_value_;
  std::mutex mutex_;
};

class ScrambledZipfianGenerator : public KeyGenerator {
public:
  ScrambledZipfianGenerator(
      uint64_t min, uint64_t max,
      double zipfian_const = ZipfianGenerator::kZipfianConst)
      : base_(min), num_items_(max - min + 1),
        generator_(min, max, zipfian_const) {}

  ScrambledZipfianGenerator(uint64_t num_items)
      : ScrambledZipfianGenerator(0, num_items - 1) {}

  uint64_t Scramble(uint64_t value) const {
    return base_ + utils::FNVHash64(value) % num_items_;
  }
  uint64_t Next() override { return Scramble(generator_.Next()); }
  uint64_t Last() { return Scramble(generator_.Last()); }

private:
  const uint64_t base_;
  const uint64_t num_items_;
  ZipfianGenerator generator_;
};
