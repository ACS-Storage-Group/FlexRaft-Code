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

namespace iter {
template <typename Container, typename Value>
struct ContainerIterator;

template <typename Iterator, typename Value, typename Predicate>
struct PredicateIterator;

template <typename Iterator, typename Value, typename Map>
struct MapIterator;

template <typename Container, typename Value = typename Container::value_type>
struct ContainerIterator {
  using InternalIter = decltype(std::begin(std::declval<Container&>()));
  using container_type = Container;

  InternalIter curr_, begin_, end_;

  ContainerIterator(Container& container)
      : curr_(container.begin()), begin_(container.begin()), end_(container.end()) {}
  ContainerIterator() = default;

  ContainerIterator(const ContainerIterator&) = default;
  ContainerIterator& operator=(const ContainerIterator&) = default;

  ~ContainerIterator() = default;

  void next() {
    if (valid()) {
      curr_ = std::next(curr_);
    }
  }

  bool valid() const { return curr_ != end_; }
  Value value() const { return *curr_; }
  void reset() { curr_ = begin_; }

  template <typename Predicate>
  PredicateIterator<ContainerIterator, Value, Predicate> filter(Predicate pred);

  template <typename Map, typename MapValue = typename std::invoke_result<Map>::type>
  MapIterator<ContainerIterator, MapValue, Map> map(Map map);
};

template <typename Container, typename Value = typename Container::value_type>
ContainerIterator<Container, Value> NewContainerIterator(Container& a) {
  return ContainerIterator<Container, Value>(a);
}

template <typename Iterator, typename Value, typename Predicate>
struct PredicateIterator {
  Iterator iter_;
  Predicate pred_;

  PredicateIterator(const Iterator& iter, Predicate pred) : iter_(iter), pred_(pred) {
    MovingToNextValid();
  }
  PredicateIterator() = delete;

  PredicateIterator(const PredicateIterator&) = default;
  PredicateIterator& operator=(const PredicateIterator&) = default;
  ~PredicateIterator() = default;

  // PredicateIterator(PredicateIterator &&p_iter)
  //     : iter_(std::move(p_iter.iter_)), pred_(std::move(p_iter.pred_)) {}

  // PredicateIterator &operator=(PredicateIterator &&p_iter) {
  //   this->iter_ = std::move(p_iter.iter_);
  //   this->pred_ = std::move(p_iter.pred_);
  // }

  void next() {
    iter_.next();
    MovingToNextValid();
  }

  bool valid() const { return iter_.valid(); }

  Value value() const { return iter_.value(); }

  void reset() {
    iter_.reset();
    MovingToNextValid();
  }

  template <typename UnaryFunction>
  void for_each(UnaryFunction action) {
    while (valid()) {
      action(value());
      next();
    }
  }

  template <typename NestedPredicate>
  PredicateIterator<PredicateIterator, Value, NestedPredicate> filter(NestedPredicate pred2);

  template <typename Map, typename MapValue = typename std::invoke_result<Map, Value>::type>
  MapIterator<PredicateIterator, MapValue, Map> map(Map map);

 private:
  void MovingToNextValid() {
    while (iter_.valid() && !pred_(iter_.value())) {
      iter_.next();
    }
  }
};

template <typename Iterator, typename Value, typename Map>
struct MapIterator {
  Iterator iter_;
  Map map_;

  MapIterator(const Iterator& iter, Map map) : iter_(iter), map_(map) {}
  MapIterator(const MapIterator&) = default;
  MapIterator& operator=(const MapIterator&) = default;

  ~MapIterator() = default;

  bool valid() const { return iter_.valid(); }
  void next() { iter_.next(); }
  Value value() const { return map_(iter_.value()); }
  void reset() { iter_.reset(); }

  template <typename Predicate>
  PredicateIterator<MapIterator, Value, Predicate> filter(Predicate pred);
};

template <typename Container, typename Value>
template <typename Predicate>
PredicateIterator<ContainerIterator<Container, Value>, Value, Predicate>
ContainerIterator<Container, Value>::filter(Predicate pred) {
  return PredicateIterator<ContainerIterator<Container, Value>, Value, Predicate>(*this, pred);
}

template <typename Iterator, typename Value, typename Predicate>
template <typename NestedPredicate>
PredicateIterator<PredicateIterator<Iterator, Value, Predicate>, Value, NestedPredicate>
PredicateIterator<Iterator, Value, Predicate>::filter(NestedPredicate pred2) {
  return PredicateIterator<PredicateIterator<Iterator, Value, Predicate>, Value, NestedPredicate>(
      *this, pred2);
}

template <typename Iterator, typename Value, typename Map>
template <typename Predicate>
PredicateIterator<MapIterator<Iterator, Value, Map>, Value, Predicate>
MapIterator<Iterator, Value, Map>::filter(Predicate pred) {
  return PredicateIterator<MapIterator<Iterator, Value, Map>, Value, Predicate>(*this, pred);
}

template <typename Container, typename Value>
template <typename Map, typename MapValue>
MapIterator<ContainerIterator<Container, Value>, MapValue, Map>
ContainerIterator<Container, Value>::map(Map map) {
  return MapIterator<ContainerIterator<Container, Value>, MapValue, Map>(*this, map);
}

template <typename Iterator, typename Value, typename Predicate>
template <typename Map, typename MapValue>
MapIterator<PredicateIterator<Iterator, Value, Predicate>, MapValue, Map>
PredicateIterator<Iterator, Value, Predicate>::map(Map map) {
  return MapIterator<PredicateIterator<Iterator, Value, Predicate>, MapValue, Map>(*this, map);
}

};  // namespace iter
