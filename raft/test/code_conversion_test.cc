#include "code_conversion.h"

#include <algorithm>
#include <cstdlib>
#include <unordered_map>

#include "gtest/gtest.h"
#include "raft_type.h"

namespace raft {

using namespace CODE_CONVERSION_NAMESPACE;

class PlacementTest : public ::testing::Test {};
class ChunkDistributionTest : public ::testing::Test {
 public:
  size_t GetRandomSizeDivide(int l_min, int l_max, int k) {
    auto s = rand() % (l_max - l_min) + l_min;
    return s % k == 0 ? s : s + (k - s % k);
  }

  Slice GenerateRandomSlice(size_t sz) {
    // Add 16 so that the data can be accessed
    auto rand_data = new char[sz];
    for (int i = 0; i < sz; ++i) {
      rand_data[i] = rand();
    }
    return Slice(rand_data, sz);
  }

  Slice GenerateSliceOfRandomSize(int l_min, int l_max, int k = 1) {
    return GenerateRandomSlice(GetRandomSizeDivide(l_min, l_max, k));
  }

  ChunkIndex GenerateRandomChunkIndex() { return ChunkIndex(rand() % 100, rand() % 100); }

  template <typename T>
  void RandomDrop(T& container, int limit) {
    while (container.size() > limit) {
      auto remove_iter = std::next(std::begin(container), rand() % container.size());
      container.erase(remove_iter);
    }
  }
};

TEST_F(PlacementTest, DISABLED_TestNoServerFailure) {
  int N = 7, F = 3, k = 4, m = 3;
  int r = get_chunk_count(k) / k;

  ChunkDistribution cd;

  std::vector<bool> aliveness(N, true);
  auto res = cd.GeneratePlacement(aliveness, F, k, r);

  ASSERT_EQ(res.replenish_server_num(), 0);
  ASSERT_EQ(res.parity_server_num(), 0);
  ASSERT_EQ(res.replenish_chunk_cnt(), 0);

  // When there is no failure, each server carries the
  // same number of data chunks
  for (const auto& vec : res.as_vec()) {
    ASSERT_EQ(vec.size(), r);
  }

  for (int i = 0; i < N; ++i) {
    for (int j = 0; j < r; ++j) {
      ASSERT_EQ(res.as_vec()[i][j], raft_chunk_index_t(i, j));
    }
  }
}

TEST_F(PlacementTest, DISABLED_TestOneServerFailure) {
  int N = 7, F = 3, k = 4, m = 3;
  int r = get_chunk_count(k) / k;

  ASSERT_EQ(r, 6);

  // One original server fails
  std::vector<bool> aliveness(N, true);

  // Generate a failure
  int f = 1;
  aliveness[f] = false;

  ChunkDistribution cd;

  auto res = cd.GeneratePlacement(aliveness, F, k, r);

  ASSERT_EQ(res.get_parity_servers(), std::vector<raft_node_id_t>({4, 5, 6}));
  ASSERT_EQ(res.replenish_chunk_cnt(), 2);

  // Check the original data chunk that comes from the entry
  ASSERT_EQ(res.as_vec()[0].size(), 8);
  ASSERT_EQ(res.as_vec()[1].size(), 0);
  ASSERT_EQ(res.as_vec()[2].size(), 8);
  ASSERT_EQ(res.as_vec()[3].size(), 8);
  ASSERT_EQ(res.as_vec()[4].size(), 6);
  ASSERT_EQ(res.as_vec()[5].size(), 6);
  ASSERT_EQ(res.as_vec()[6].size(), 6);

  for (int i = 0; i < N; ++i) {
    if (i == f) {
      continue;
    }
    for (int j = 0; j < r; ++j) {
      ASSERT_EQ(res.as_vec()[i][j], raft_chunk_index_t(i, j));
    }
  }

  // Check the redistributed data chunks:
  ASSERT_EQ(res.as_vec()[0][r], raft_chunk_index_t(1, 0));
  ASSERT_EQ(res.as_vec()[0][r + 1], raft_chunk_index_t(1, 1));

  ASSERT_EQ(res.as_vec()[2][r], raft_chunk_index_t(1, 2));
  ASSERT_EQ(res.as_vec()[2][r + 1], raft_chunk_index_t(1, 3));

  ASSERT_EQ(res.as_vec()[3][r], raft_chunk_index_t(1, 4));
  ASSERT_EQ(res.as_vec()[3][r + 1], raft_chunk_index_t(1, 5));
}

TEST_F(PlacementTest, DISABLED_TestOneParityServerFailure) {
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

TEST_F(PlacementTest, DISABLED_TestTwoServersFailure) {
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

TEST_F(ChunkDistributionTest, DISABLED_TestEncodeResultsWithoutServerFailure) {
  const int TestN = 7, TestK = 4, TestF = 3;
  auto total_chunk_num = get_chunk_count(TestK), r = total_chunk_num / TestK;

  // Check some parameters
  ASSERT_EQ(total_chunk_num, 24);
  ASSERT_EQ(r, 6);

  ChunkDistribution cd(TestF, TestK, r);
  std::vector<bool> is_alive(TestN, true);

  // Generate placement
  cd.GeneratePlacement(is_alive);

  // Generate a random slice
  auto slice = GenerateSliceOfRandomSize(512 * 1024, 1024 * 1024, total_chunk_num);

  // Do encoding
  cd.EncodeForPlacement(slice);

  // Check the chunk vector size:
  for (int i = 0; i < TestN; ++i) {
    ASSERT_EQ(cd.GetChunkVector(i).as_vec().size(), r);
  }
}

TEST_F(ChunkDistributionTest, DISABLED_TestRecoverTheWholeEntryWithOneServerFailure) {
  const int TestN = 7, TestK = 4, TestF = 3;
  auto total_chunk_num = get_chunk_count(TestK), r = total_chunk_num / TestK;

  ChunkDistribution cd(TestF, TestK, r);
  std::vector<bool> is_alive(TestN, true);
  auto f = 1;
  is_alive[f] = false;

  auto slice = GenerateSliceOfRandomSize(512 * 1024, 1024 * 1024, total_chunk_num);
  cd.GeneratePlacement(is_alive);
  cd.EncodeForPlacement(slice);

  std::unordered_map<raft_node_id_t, ChunkDistribution::ChunkVector> data;
  for (int i = 0; i < TestN; ++i) {
    data.emplace(i, cd.GetChunkVector(i));
  }

  // Randomly drop some data of failed servers
  ASSERT_EQ(data.size(), TestN);
  RandomDrop(data, TestK);

  Slice recover_ent;
  auto b = cd.Decode(data, &recover_ent);

  ASSERT_TRUE(b);
  ASSERT_EQ(recover_ent.compare(slice), 0);
}

TEST_F(ChunkDistributionTest, TestRecoverTheWholeEntryWithTwoServerFailure) {
  const int TestN = 7, TestK = 4, TestF = 3;
  auto total_chunk_num = get_chunk_count(TestK), r = total_chunk_num / TestK;

  // Check some parameters
  ASSERT_EQ(total_chunk_num, 24);
  ASSERT_EQ(r, 6);

  ChunkDistribution cd(TestF, TestK, r);
  std::vector<bool> is_alive(TestN, true);
  auto f1 = 1, f2 = 3;
  is_alive[f1] = false;
  is_alive[f2] = false;

  auto slice = GenerateSliceOfRandomSize(512 * 1024, 1024 * 1024, total_chunk_num);
  cd.GeneratePlacement(is_alive);
  cd.EncodeForPlacement(slice);

  std::unordered_map<raft_node_id_t, ChunkDistribution::ChunkVector> data;
  for (int i = 0; i < TestN; ++i) {
    data.emplace(i, cd.GetChunkVector(i));
  }

  // Randomly drop some data of failed servers
  ASSERT_EQ(data.size(), TestN);
  RandomDrop(data, TestK);

  Slice recover_ent;
  auto b = cd.Decode(data, &recover_ent);

  ASSERT_TRUE(b);
  ASSERT_EQ(recover_ent.compare(slice), 0);
}

TEST_F(ChunkDistributionTest, TestChunkVectorSerialization) {
  // Construct the ChunkVector first
  const int elem_cnt = 10;
  ChunkDistribution::ChunkVector cv;
  for (int i = 0; i < elem_cnt; ++i) {
    cv.AddChunk(GenerateRandomChunkIndex(), GenerateRandomChunkIndex(),
                GenerateRandomSlice(512 * 1024));
  }

  auto s_cv = cv.Serialize();
  ASSERT_NE(s_cv.data(), nullptr);
  ASSERT_GE(s_cv.size(), 0);

  // Deserialize the slice
  ChunkDistribution::ChunkVector d_cv;
  auto b = d_cv.Deserialize(s_cv);
  ASSERT_TRUE(b);
  ASSERT_EQ(d_cv.chunks_.size(), cv.chunks_.size());

  // Compare that each vector element equals
  for (int i = 0; i < cv.as_vec().size(); ++i) {
    ASSERT_EQ(d_cv.as_vec()[i].idx1, cv.as_vec()[i].idx1);
    ASSERT_EQ(d_cv.as_vec()[i].idx2, cv.as_vec()[i].idx2);
    ASSERT_EQ(cv.as_vec()[i].data.compare(d_cv.as_vec()[i].data), 0);
  }
}

};  // namespace raft
