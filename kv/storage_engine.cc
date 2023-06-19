#include "storage_engine.h"

#include <iostream>
#include <string>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "rocksdb/db.h"
#include "util.h"

namespace kv {

class RocksDBEngine final : public StorageEngine {
public:
  explicit RocksDBEngine(const std::string &dbname) {
    rocksdb::Options options;
    options.create_if_missing = true;

    auto stat = rocksdb::DB::Open(options, dbname, &dbptr_);
    if (!stat.ok()) {
      std::cout << stat.ToString() << std::endl;
    }
    assert(stat.ok());
  }

  ~RocksDBEngine() { delete dbptr_; }

  std::string EngineName() const override {
    return std::string("RocksDBEngine");
  }

  bool Put(const std::string &key, const std::string &value) override {
    // NOTE: Should we use wo.sync=true or wo.sync=false, there is a huge
    // performance difference between these two choices
    auto wo = rocksdb::WriteOptions();
    wo.sync = false;
    auto stat = dbptr_->Put(wo, key, value);
    return stat.ok();
  }

  bool Delete(const std::string &key) override {
    auto stat = dbptr_->Delete(rocksdb::WriteOptions(), key);
    return stat.ok();
  }

  bool Get(const std::string &key, std::string *value) override {
    auto stat = dbptr_->Get(rocksdb::ReadOptions(), key, value);
    return stat.ok();
  }

  void Close() override { dbptr_->Close(); }

private:
  rocksdb::DB *dbptr_;
};

StorageEngine *StorageEngine::Default(const std::string &dbname) {
  return NewRocksDBEngine(dbname);
}

StorageEngine *StorageEngine::Warmup(const std::string &dbname) {
  auto ret = Default(dbname);
  const int kKeyRange = 5000;
  const int kKeySize = 64;
  const int kValueSize = 2ULL * 1024 * 1024;
  // In FlexRaft, each value must be prefixed with (k, m, fragment id)
  // information
  char value_prefix[12];
  *reinterpret_cast<int *>(value_prefix) = 1;
  *reinterpret_cast<int *>(value_prefix + 4) = 0;
  *reinterpret_cast<int *>(value_prefix + 8) = 0;

  std::string prefix;
  for (int i = 0; i < 12; ++i) {
    prefix.push_back(value_prefix[i]);
  }
  assert(prefix.size() == 12);

  for (int i = 0; i < kKeyRange; ++i) {
    auto key = raft::util::MakeKey(i, kKeySize);
    auto value = prefix + raft::util::MakeValue(i, kValueSize);
    ret->Put(key, value);
  }
  return ret;
}

StorageEngine *StorageEngine::NewRocksDBEngine(const std::string &name) {
  return new RocksDBEngine(name);
}
} // namespace kv
