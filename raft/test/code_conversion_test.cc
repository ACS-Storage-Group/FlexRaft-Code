#include "code_conversion.h"

#include <cstdlib>

#include "gtest/gtest.h"

namespace raft {
class PlacementTest : public ::testing::Test {};

TEST_F(PlacementTest, TestNoServerFailure) {
  int N = 7, F = 3, k = 4, m = 3;
  int r = get_chunk_count(k) / k;

  std::vector<bool> aliveness(N, true);
  auto res = generate_placement(aliveness, N, F, k, m, r);

  // When there is no failure, each server carries the
  // same number of data chunks
  for (const auto& vec : res) {
    ASSERT_EQ(vec.size(), r);
  }

  for (int i = 0; i < N; ++i) {
    for (int j = 0; j < r; ++j) {
      ASSERT_EQ(res[i][j], raft_chunk_index_t(i, j));
    }
  }
}

TEST_F(PlacementTest, TestOneServerFailure) {
  int N = 7, F = 3, k = 4, m = 3;
  int r = get_chunk_count(k) / k;

  std::vector<bool> aliveness(N, true);
  // Generate a random failure
  int f = rand() % N;
  aliveness[f] = false;

  auto res = generate_placement(aliveness, N, F, k, m, r);

  // When there is no failure, each server carries the
  // same number of data chunks
  for (const auto& vec : res) {
    ASSERT_EQ(vec.size(), r);
  }

  for (int i = 0; i < N; ++i) {
    for (int j = 0; j < r; ++j) {
      ASSERT_EQ(res[i][j], raft_chunk_index_t(i, j));
    }
  }
}

};  // namespace raft
