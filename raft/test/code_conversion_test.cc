#include "code_conversion.h"

#include <algorithm>
#include <cstdlib>
#include <unordered_map>

#include "gtest/gtest.h"
#include "raft_type.h"

namespace raft {

using namespace CODE_CONVERSION_NAMESPACE;

class CodeConversionManagementTest : public ::testing::Test {
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

  template <typename T>
  void RandomDrop(T& container, int limit) {
    while (container.size() > limit) {
      auto remove_iter = std::next(std::begin(container), rand() % container.size());
      container.erase(remove_iter);
    }
  }

  void CheckProcess(int TestN, int TestK, int TestF, int r, const std::vector<bool>& aliveness,
                    const std::vector<raft_node_id_t>& fail_servers) {
    CodeConversionManagement ccm(TestK, TestF, r);
    // Generate a random slice
    auto slice = GenerateSliceOfRandomSize(512 * 1024, 1024 * 1024, r * TestK);
    int alive_cnt = TestN - fail_servers.size();
    int reserved_server_cnt = alive_cnt - TestF;
    int reserve_for_each = r / reserved_server_cnt * fail_servers.size();

    // Do encoding
    ccm.EncodeForPlacement(slice, aliveness);

    // Gather the data
    std::map<raft_node_id_t, ChunkVector> data;
    for (int i = 0; i < TestN; ++i) {
      data.emplace(i, ccm.GetOriginalChunkVector(i));
      data[i].Concatenate(ccm.GetReservedChunkVector(i));
    }

    for (int i = 0; i < TestN; ++i) {
      if (std::find(fail_servers.begin(), fail_servers.end(), i) != fail_servers.end()) {
        ASSERT_EQ(data[i].size(), 0);
      } else {
        ASSERT_EQ(data[i].size(), r + reserve_for_each);
      }
    }

    // Randomly drop some data of failed servers
    ASSERT_EQ(data.size(), TestN);
    RandomDrop(data, TestK);

    Slice recover_ent;
    auto b = ccm.DecodeCollectedChunkVec(data, &recover_ent);

    ASSERT_TRUE(b);
    ASSERT_EQ(recover_ent.compare(slice), 0);
  }
};

TEST_F(CodeConversionManagementTest, TestRecoverTheWholeEntryWithNoServerFailure) {
  const int TestN = 7, TestK = 4, TestF = 3;
  auto total_chunk_num = get_chunk_count(TestK), r = total_chunk_num / TestK;

  ASSERT_EQ(r, 6);

  std::vector<bool> is_alive(TestN, true);

  CheckProcess(TestN, TestK, TestF, r, is_alive, {});
}

TEST_F(CodeConversionManagementTest, TestRecoverTheWholeEntryWithOneServerFailure) {
  const int TestN = 7, TestK = 4, TestF = 3;
  auto total_chunk_num = get_chunk_count(TestK), r = total_chunk_num / TestK;

  std::vector<bool> is_alive(TestN, true);
  raft_node_id_t f = 1;
  is_alive[f] = false;

  CheckProcess(TestN, TestK, TestF, r, is_alive, std::vector<raft_node_id_t>({f}));
}

TEST_F(CodeConversionManagementTest, TestRecoverTheWholeEntryWithTwoServerFailure) {
  const int TestN = 7, TestK = 4, TestF = 3;
  auto total_chunk_num = get_chunk_count(TestK), r = total_chunk_num / TestK;

  std::vector<bool> is_alive(TestN, true);
  raft_node_id_t f1 = 1, f2 = 3;
  is_alive[f1] = is_alive[f2] = false;

  CheckProcess(TestN, TestK, TestF, r, is_alive, std::vector<raft_node_id_t>({f1, f2}));
}

TEST_F(CodeConversionManagementTest, TestAdjustChunkDistribution) {
  const int TestN = 7, TestK = 4, TestF = 3;
  auto total_chunk_num = get_chunk_count(TestK), r = total_chunk_num / TestK;

  std::vector<bool> is_alive(TestN, true);
  raft_node_id_t f1 = 1;
  is_alive[f1] = false;

  CodeConversionManagement ccm(TestK, TestF, r);
  // Generate a random slice
  auto slice = GenerateSliceOfRandomSize(512 * 1024, 1024 * 1024, r * TestK);

  // Do encoding
  ccm.EncodeForPlacement(slice, is_alive);

  // Gather the data
  std::map<raft_node_id_t, ChunkVector> data;

  /// !!!! Add another failure, adjust the data distribution !!!!
  raft_node_id_t f2 = 5;
  is_alive[f2] = false;
  ccm.AdjustChunkDistribution(is_alive);

  data.clear();
  for (int i = 0; i < TestN; ++i) {
    data.emplace(i, ccm.GetOriginalChunkVector(i));
    data[i].Concatenate(ccm.GetReservedChunkVector(i));
  }

  // Randomly drop some data of failed servers
  ASSERT_EQ(data.size(), TestN);
  RandomDrop(data, TestK);
  Slice recover_ent;

  auto b = ccm.DecodeCollectedChunkVec(data, &recover_ent);

  ASSERT_TRUE(b);
  ASSERT_EQ(recover_ent.compare(slice), 0);
}

};  // namespace raft
