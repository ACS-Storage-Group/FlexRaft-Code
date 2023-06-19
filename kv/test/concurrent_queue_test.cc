#include "concurrent_queue.h"

#include <algorithm>
#include <cstdlib>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
namespace kv {
class ConcurrentQueueTest : public ::testing::Test {
 public:
  template <typename T>
  void LaunchProducerThread(ConcurrentQueue<T>* queue, const std::vector<T>& data) {
    auto producer = [=]() {
      std::for_each(data.begin(), data.end(), [=](const T& ent) { queue->Push(ent); });
    };
    threads_.push_back(new std::thread(producer));
  }

  void WaitThreadsExit() {
    std::for_each(threads_.begin(), threads_.end(), [](std::thread* t) {
      t->join();
      delete t;
    });
  }

  void AddThread(std::thread* t) { threads_.push_back(t); }

 private:
  std::vector<std::thread*> threads_;
};

TEST_F(ConcurrentQueueTest, TestSingleConsumerSingleProducer) {
  auto queue = new ConcurrentQueue<int>(10);
  std::vector<int> data = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  LaunchProducerThread(queue, data);

  auto consumer = [=, &data]() {
    for (const auto& val : data) {
      ASSERT_EQ(queue->Pop(), val);
    }
  };
  AddThread(new std::thread(consumer));
  WaitThreadsExit();
  delete queue;
}

TEST_F(ConcurrentQueueTest, TestVeryLargeSizeTransfer) {
  const int test_size = 1000000;

  auto queue = new ConcurrentQueue<int>(100);
  std::vector<int> data;
  for (int i = 0; i < test_size; ++i) {
    data.push_back(rand());
  }

  LaunchProducerThread(queue, data);

  auto consumer = [=, &data]() {
    for (const auto& val : data) {
      ASSERT_EQ(queue->Pop(), val);
    }
  };
  AddThread(new std::thread(consumer));
  WaitThreadsExit();
  delete queue;
}

TEST_F(ConcurrentQueueTest, TestIfTryPopWorks) {
  const int test_size = 1000000;

  auto queue = new ConcurrentQueue<int>(100);
  std::vector<int> data;
  for (int i = 0; i < test_size; ++i) {
    data.push_back(rand());
  }

  LaunchProducerThread(queue, data);

  auto consumer = [=, &data]() {
    for (const auto& val : data) {
      ASSERT_EQ(queue->Pop(), val);
    }
    int val;
    ASSERT_FALSE(queue->TryPop(val));
  };
  AddThread(new std::thread(consumer));
  WaitThreadsExit();
  delete queue;
}
}  // namespace kv
