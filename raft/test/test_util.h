#pragma once
#include <cstdlib>
#include "raft_type.h"

namespace raft {
namespace test {
  Slice GenerateRandomSlice(size_t sz) {
    // Add 16 so that the data can be accessed
    auto rand_data = new char[sz];
    for (int i = 0; i < sz; ++i) {
      rand_data[i] = rand();
    }
    return Slice(rand_data, sz);
  }

  // Generate a size in the range [size_min, size_max)
  size_t GenRandSize(size_t size_min, size_t size_max) {
    return rand() % (size_max - size_min) + size_min;
  }
};
};