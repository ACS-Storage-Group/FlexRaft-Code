#pragma once
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

namespace raft {
namespace util {

template <typename Container>
struct ContainerIterator;

template <typename Container, typename Predicate>
struct PredicateIterator;

template <typename Container>
struct ContainerIterator {
  using Iter = decltype(std::begin(std::declval<Container&>()));
  using Value = decltype(*std::begin(std::declval<Container&>()));
  Iter begin_, end_;

  ContainerIterator(Container& container) : begin_(container.begin()), end_(container.end()) {}

  ContainerIterator(const ContainerIterator& container) = default;
  ContainerIterator& operator=(const ContainerIterator& container) = default;

  void next() { begin_ = std::next(begin_); }
  bool valid() { return begin_ != end_; }
  Value value() { return *begin_; }

  template <typename Predicate>
  PredicateIterator<Container, Predicate> filter(Predicate pred);

  template <typename UnaryFunction>
  void for_each(UnaryFunction act) {
    while (valid()) {
      act(*begin_);
      next();
    }
  }
};

template <typename Container, typename Predicate>
struct PredicateIterator {
  ContainerIterator<Container> c_iter_;
  Predicate pred_;

  PredicateIterator(ContainerIterator<Container> c_iter, Predicate pred)
      : c_iter_(c_iter), pred_(pred) {}

  void next() {
    c_iter_.next();
    while (c_iter_.valid() && !pred_(c_iter_.value())) {
      c_iter_.next();
    }
  }

  bool valid() { return c_iter_.valid(); }
  decltype(c_iter_.value()) value() { return c_iter_.value(); }

  template <typename UnaryFunction>
  void for_each(UnaryFunction act) {
    while (valid()) {
      act(value());
      next();
    }
  }
};

template <typename T>
struct Enumerator {
  T start_, end_, sep_, curr_;
  Enumerator(T start, T end, T sep) : start_(start), end_(end), sep_(sep), curr_(start_) {}
  Enumerator(const Enumerator&) = default;
  Enumerator& operator=(const Enumerator&) = default;

  void next() { curr_ += sep_; }
  bool valid() { return curr_ < end_; }
  T value() { return curr_; }
  void Reset() { curr_ = start_; }

  template <typename UnaryFunction>
  void for_each(UnaryFunction act) {
    while (valid()) {
      act(value());
      next();
    }
  }
};

template <typename Iter>
struct NestedIterator {
  Iter iter1_, iter2_;
  NestedIterator(Iter iter1, Iter iter2) : iter1_(iter1), iter2_(iter2) {}

  template <typename UnaryFunction>
  void for_each(UnaryFunction act) {
    while (iter1_.valid()) {
      iter2_.Reset();
      while (iter2_.valid()) {
        act(iter1_.value(), iter2_.value());
        iter2_.next();
      }
      iter1_.next();
    }
  }
};

template <typename Container>
template <typename Predicate>
PredicateIterator<Container, Predicate> ContainerIterator<Container>::filter(Predicate pred) {
  return PredicateIterator<Container, Predicate>(*this, pred);
}
}  // namespace util
};  // namespace raft

// Abstract class for iterator
template <typename Value>
struct Iter {
  // Check if this iterator is valid
  virtual bool valid() = 0;

  // Move this iterator to the next position
  virtual void next() = 0;

  // Reset this iterator to its start position
  virtual void reset() = 0;

  // Return the value that this iterator points to
  virtual Value value() = 0;

  template <typename Predicate>
  Iter<Value>* filter(Predicate pred);

  template <typename UnaryFunction>
  Iter<Value>* map_as(UnaryFunction act);

  template <typename UnaryFunction>
  Iter<Value>* for_each(UnaryFunction act) {
    while (valid()) {
      act(value());
      next();
    }
    this->reset();
    return this;
  }

  template <typename Container>
  Container collect() {
    Container ret;
    reset();
    while (valid()) {
      std::back_inserter(ret) = value();
      next();
    }
    // This iterator should no more exist
    delete this;
    return ret;
  }

  virtual ~Iter() = default;
};

template <typename Container, typename Value = typename Container::value_type>
struct ContainerIterator : public Iter<Value> {
  using InternalIter = decltype(std::begin(std::declval<Container&>()));
  InternalIter begin_, end_, curr_;

  ContainerIterator(Container& container)
      : begin_(container.begin()), end_(container.end()), curr_(begin_) {}

  ContainerIterator(const ContainerIterator& container) = default;
  ContainerIterator& operator=(const ContainerIterator& container) = default;

  void next() override { curr_ = std::next(curr_); }
  bool valid() override { return curr_ != end_; }
  Value value() override { return *curr_; }
  void reset() override { curr_ = begin_; }

  ~ContainerIterator() = default;
};

// Construct the iterator from existing container
template <typename Container, typename Value = typename Container::value_type>
Iter<Value>* NewContainerIter(Container& container) {
  return new ContainerIterator(container);
}

template <typename Value, typename Predicate>
struct PredicateIterator : public Iter<Value> {
  Iter<Value>* c_iter_;
  Predicate pred_;

  PredicateIterator(Iter<Value>* c_iter, Predicate pred) : c_iter_(c_iter), pred_(pred) {
    MoveToFirstValidEntry();
  }

  void next() override {
    c_iter_->next();
    while (c_iter_->valid() && !pred_(c_iter_->value())) {
      c_iter_->next();
    }
  }

  bool valid() override { return c_iter_->valid(); }
  Value value() override { return c_iter_->value(); }
  void reset() override {
    c_iter_->reset();
    MoveToFirstValidEntry();
  }

 private:
  void MoveToFirstValidEntry() {
    while (valid() && !pred_(value())) {
      next();
    }
  }

  ~PredicateIterator() { delete c_iter_; }
};

template <typename Value, typename MapFunction>
struct MapIterator : public Iter<Value> {
  Iter<Value>* c_iter_;
  MapFunction map_;

  MapIterator(Iter<Value>* c_iter, MapFunction map) : c_iter_(c_iter), map_(map) {}

  void next() override { c_iter_->next(); }
  bool valid() override { return c_iter_->valid(); }
  Value value() override { return map_(c_iter_->value()); }
  void reset() override { c_iter_->reset(); }

  ~MapIterator() { delete c_iter_; }
};

template <typename Value>
template <typename Predicate>
Iter<Value>* Iter<Value>::filter(Predicate pred) {
  return new PredicateIterator<Value, Predicate>(this, pred);
}

template <typename Value>
template <typename MapFunction>
Iter<Value>* Iter<Value>::map_as(MapFunction map_func) {
  return new MapIterator<Value, MapFunction>(this, map_func);
}
