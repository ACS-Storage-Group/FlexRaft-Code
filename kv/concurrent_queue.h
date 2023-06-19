#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <mutex>
namespace kv {
// A simple thread-safe queue that supports Push and Pop method.
// When the queue is empty, Pop operation should be blocked; When queue
// is full, Push operation should be blocked
// Note this queue only supports single producer and single consummer
template <typename T>
class ConcurrentQueue {
 public:
  ConcurrentQueue(size_t capacity) : capacity_(capacity), head_(0), tail_(0), size_(0) {
    elems = new T[capacity_];
  }
  ConcurrentQueue() : ConcurrentQueue(1) {}
  ~ConcurrentQueue() { delete[] elems; }

  // [MAY BLOCKED]
  void Push(const T& ent) {
    std::unique_lock<std::mutex> lck(enq_mtx);
    while (size_.load() == capacity_) {
      not_full_condition_.wait(lck);
    }
    elems[tail_] = ent;
    tail_ = (tail_ + 1) % capacity_;
    auto prev_size = size_.fetch_add(1);
    if (prev_size == 0) {
      std::unique_lock<std::mutex> deq_lock(deq_mtx);
      not_empty_condition_.notify_all();
    }
  }

  // [MAY BLOCKED]
  T Pop() {
    std::unique_lock<std::mutex> deq_lock(deq_mtx);
    while (size_.load() == 0) {
      not_empty_condition_.wait(deq_lock);
    }

    auto ret = elems[head_];
    head_ = (head_ + 1) % capacity_;
    auto prev_size = size_.fetch_sub(1);

    if (prev_size == capacity_) {
      std::unique_lock<std::mutex> enq_lock(enq_mtx);
      not_full_condition_.notify_all();
    }
    return ret;
  }

  // [NOT BLOCK]
  bool TryPop(T& ent) {
    std::unique_lock<std::mutex> deq_lock(deq_mtx);
    if (size_.load() == 0) {
      return false;
    } else {
      auto ret = elems[head_];
      head_ = (head_ + 1) % capacity_;
      auto prev_size = size_.fetch_sub(1);

      if (prev_size == capacity_) {
        std::unique_lock<std::mutex> enq_lock(enq_mtx);
        not_full_condition_.notify_all();
      }

      ent = ret;
      return true;
    }
  }

  size_t Size() const { return size_.load(); }

 private:
  T* elems;
  size_t capacity_;
  int head_, tail_;
  std::atomic<size_t> size_;
  std::condition_variable not_full_condition_;
  std::condition_variable not_empty_condition_;
  std::mutex enq_mtx;
  std::mutex deq_mtx;
};
}  // namespace kv
