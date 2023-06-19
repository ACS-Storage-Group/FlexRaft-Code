#pragma once
#include <string>
namespace kv {
class StorageEngine {
 public:
  StorageEngine() = default;

  StorageEngine(const StorageEngine&) = delete;
  StorageEngine& operator=(const StorageEngine&) = delete;

  virtual ~StorageEngine() = default;

  // Create and return a storage engine pointer using specified database name
  static StorageEngine* Default(const std::string& dbname);
  static StorageEngine* Warmup(const std::string& dbname);
  static StorageEngine* NewLevelDBEngine(const std::string&name);
  static StorageEngine* NewRocksDBEngine(const std::string&name);

  virtual std::string EngineName() const = 0;

  // Makes sure the database successfully exits
  virtual void Close() = 0;

  // Put a key-value pair into local storage engine, return true if the
  // operation succeed, returns false if any error occurred. The input key &
  // value must maintain liveness during insertion
  virtual bool Put(const std::string& key, const std::string& value) = 0;

  // Search for a specific key and associated value. Return true if the key
  // exists and fill in value with associated value, otherwise return false
  virtual bool Get(const std::string& key, std::string* value) = 0;

  // Delete the record with specific key, return true if the key is successfully
  // deleted, otherwise return false
  virtual bool Delete(const std::string& key) = 0;
};
}  // namespace kv
