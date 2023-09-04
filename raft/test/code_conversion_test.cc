#include "code_conversion.h"

#include <algorithm>
#include <cstdlib>

#include "gtest/gtest.h"

namespace raft {

using namespace CODE_CONVERSION_NAMESPACE;

class PlacementTest : public ::testing::Test {};

TEST_F(PlacementTest, TestNoServerFailure) {
  int N = 7, F = 3, k = 4, m = 3;
  int r = get_chunk_count(k) / k;

  ChunkDistribution cd;

  std::vector<bool> aliveness(N, true);
  auto res = cd.GeneratePlacement(aliveness, F, k, r).as_vec();

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

  ASSERT_EQ(r, 6);

  // One original server fails
  std::vector<bool> aliveness(N, true);

  // Generate a failure
  int f = 1;
  aliveness[f] = false;

  ChunkDistribution cd;

  auto res = cd.GeneratePlacement(aliveness, F, k, r).as_vec();
  auto redistr = r / (k - 1);
  ASSERT_EQ(redistr, 2);

  // Check the original data chunk that comes from the entry
  ASSERT_EQ(res[0].size(), 8);
  ASSERT_EQ(res[1].size(), 0);
  ASSERT_EQ(res[2].size(), 8);
  ASSERT_EQ(res[3].size(), 8);
  ASSERT_EQ(res[4].size(), 6);
  ASSERT_EQ(res[5].size(), 6);
  ASSERT_EQ(res[6].size(), 6);

  for (int i = 0; i < N; ++i) {
    if (i == f) {
      continue;
    }
    for (int j = 0; j < r; ++j) {
      ASSERT_EQ(res[i][j], raft_chunk_index_t(i, j));
    }
  }

  // Check the redistributed data chunks:
  ASSERT_EQ(res[0][r], raft_chunk_index_t(1, 0));
  ASSERT_EQ(res[0][r + 1], raft_chunk_index_t(1, 1));

  ASSERT_EQ(res[2][r], raft_chunk_index_t(1, 2));
  ASSERT_EQ(res[2][r + 1], raft_chunk_index_t(1, 3));

  ASSERT_EQ(res[3][r], raft_chunk_index_t(1, 4));
  ASSERT_EQ(res[3][r + 1], raft_chunk_index_t(1, 5));
}

TEST_F(PlacementTest, TestOneParityServerFailure) {
  int N = 7, F = 3, k = 4, m = 3;
  int r = get_chunk_count(k) / k;

  ASSERT_EQ(r, 6);

  // One original server fails
  std::vector<bool> aliveness(N, true);

  // Generate a failure
  int f = 5;
  aliveness[f] = false;

  ChunkDistribution cd;

  auto res = cd.GeneratePlacement(aliveness, F, k, r).as_vec();
  auto redistr = r / (k - 1);
  ASSERT_EQ(redistr, 2);

  // Check the original data chunk that comes from the entry
  ASSERT_EQ(res[0].size(), 8);
  ASSERT_EQ(res[1].size(), 8);
  ASSERT_EQ(res[2].size(), 8);
  ASSERT_EQ(res[3].size(), 6);
  ASSERT_EQ(res[4].size(), 6);
  ASSERT_EQ(res[5].size(), 0);
  ASSERT_EQ(res[6].size(), 6);

  for (int i = 0; i < N; ++i) {
    if (i == f) {
      continue;
    }
    for (int j = 0; j < r; ++j) {
      ASSERT_EQ(res[i][j], raft_chunk_index_t(i, j));
    }
  }

  // Check the redistributed data chunks:
  ASSERT_EQ(res[0][r], raft_chunk_index_t(5, 0));
  ASSERT_EQ(res[0][r + 1], raft_chunk_index_t(5, 1));

  ASSERT_EQ(res[1][r], raft_chunk_index_t(5, 2));
  ASSERT_EQ(res[1][r + 1], raft_chunk_index_t(5, 3));

  ASSERT_EQ(res[2][r], raft_chunk_index_t(5, 4));
  ASSERT_EQ(res[2][r + 1], raft_chunk_index_t(5, 5));
}

TEST_F(PlacementTest, TestTwoServersFailure) {
  int N = 7, F = 3, k = 4, m = 3;
  int r = get_chunk_count(k) / k;

  ASSERT_EQ(r, 6);

  // One original server fails
  std::vector<bool> aliveness(N, true);

  // Generate a failure
  int f1 = 1, f2 = 5;
  aliveness[f1] = false;
  aliveness[f2] = false;

  ChunkDistribution cd;

  auto res = cd.GeneratePlacement(aliveness, F, k, r).as_vec();
  auto redistr = r / (k - 2);
  ASSERT_EQ(redistr, 3);

  // Check the original data chunk that comes from the entry
  ASSERT_EQ(res[0].size(), 12);
  ASSERT_EQ(res[1].size(), 0);
  ASSERT_EQ(res[2].size(), 12);
  ASSERT_EQ(res[3].size(), 6);
  ASSERT_EQ(res[4].size(), 6);
  ASSERT_EQ(res[5].size(), 0);
  ASSERT_EQ(res[6].size(), 6);

  for (int i = 0; i < N; ++i) {
    if (i == f1 || i == f2) {
      continue;
    }
    for (int j = 0; j < r; ++j) {
      ASSERT_EQ(res[i][j], raft_chunk_index_t(i, j));
    }
  }

  // Check the redistributed data chunks:
  ASSERT_EQ(res[0][r], raft_chunk_index_t(1, 0));
  ASSERT_EQ(res[0][r + 1], raft_chunk_index_t(1, 1));
  ASSERT_EQ(res[0][r + 2], raft_chunk_index_t(1, 2));

  ASSERT_EQ(res[0][r + 3], raft_chunk_index_t(5, 0));
  ASSERT_EQ(res[0][r + 4], raft_chunk_index_t(5, 1));
  ASSERT_EQ(res[0][r + 5], raft_chunk_index_t(5, 2));

  ASSERT_EQ(res[2][r], raft_chunk_index_t(1, 3));
  ASSERT_EQ(res[2][r + 1], raft_chunk_index_t(1, 4));
  ASSERT_EQ(res[2][r + 2], raft_chunk_index_t(1, 5));

  ASSERT_EQ(res[2][r + 3], raft_chunk_index_t(5, 3));
  ASSERT_EQ(res[2][r + 4], raft_chunk_index_t(5, 4));
  ASSERT_EQ(res[2][r + 5], raft_chunk_index_t(5, 5));
}

};  // namespace raft
