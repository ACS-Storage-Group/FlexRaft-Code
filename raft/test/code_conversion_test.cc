#include "code_conversion.h"

#include <algorithm>
#include <cstdlib>
#include <numeric>
#include <unordered_map>

#include "encoder.h"
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

  // void CheckProcess(int TestN, int TestK, int TestF, int r, const std::vector<bool>& aliveness,
  //                   const std::vector<raft_node_id_t>& fail_servers) {
  //   CodeConversionManagement ccm(TestK, TestF, r);
  //   // Generate a random slice
  //   auto slice = GenerateSliceOfRandomSize(512 * 1024, 1024 * 1024, r * TestK);
  //   int alive_cnt = TestN - fail_servers.size();
  //   int reserved_server_cnt = alive_cnt - TestF;
  //   int reserve_for_each = r / reserved_server_cnt * fail_servers.size();

  //   // Do encoding
  //   ccm.EncodeForPlacement(slice, aliveness, nullptr);

  //   // Gather the data
  //   std::map<raft_node_id_t, ChunkVector> data;
  //   for (int i = 0; i < TestN; ++i) {
  //     data.emplace(i, ccm.GetOriginalChunkVector(i));
  //     data[i].Concatenate(ccm.GetReservedChunkVector(i));
  //   }

  //   for (int i = 0; i < TestN; ++i) {
  //     if (std::find(fail_servers.begin(), fail_servers.end(), i) != fail_servers.end()) {
  //       ASSERT_EQ(data[i].size(), 0);
  //     } else {
  //       ASSERT_EQ(data[i].size(), r + reserve_for_each);
  //     }
  //   }

  //   // Randomly drop some data of failed servers
  //   ASSERT_EQ(data.size(), TestN);
  //   RandomDrop(data, TestK);

  //   Slice recover_ent;
  //   auto b = ccm.DecodeCollectedChunkVec(data, &recover_ent);

  //   ASSERT_TRUE(b);
  //   ASSERT_EQ(recover_ent.compare(slice), 0);
  // }
};

// TEST_F(CodeConversionManagementTest, DISABLED_TestRecoverTheWholeEntryWithNoServerFailure) {
//   const int TestN = 7, TestK = 4, TestF = 3;
//   auto total_chunk_num = get_chunk_count(TestK), r = total_chunk_num / TestK;

//   ASSERT_EQ(r, 6);

//   std::vector<bool> is_alive(TestN, true);

//   CheckProcess(TestN, TestK, TestF, r, is_alive, {});
// }

// TEST_F(CodeConversionManagementTest, DISABLED_TestRecoverTheWholeEntryWithOneServerFailure) {
//   const int TestN = 7, TestK = 4, TestF = 3;
//   auto total_chunk_num = get_chunk_count(TestK), r = total_chunk_num / TestK;

//   std::vector<bool> is_alive(TestN, true);
//   raft_node_id_t f = 1;
//   is_alive[f] = false;

//   CheckProcess(TestN, TestK, TestF, r, is_alive, std::vector<raft_node_id_t>({f}));
// }

// TEST_F(CodeConversionManagementTest, DISABLED_TestRecoverTheWholeEntryWithTwoServerFailure) {
//   const int TestN = 7, TestK = 4, TestF = 3;
//   auto total_chunk_num = get_chunk_count(TestK), r = total_chunk_num / TestK;

//   std::vector<bool> is_alive(TestN, true);
//   raft_node_id_t f1 = 1, f2 = 3;
//   is_alive[f1] = is_alive[f2] = false;

//   CheckProcess(TestN, TestK, TestF, r, is_alive, std::vector<raft_node_id_t>({f1, f2}));
// }

// TEST_F(CodeConversionManagementTest, DISABLED_TestAdjustChunkDistribution) {
//   const int TestN = 7, TestK = 4, TestF = 3;
//   auto total_chunk_num = get_chunk_count(TestK), r = total_chunk_num / TestK;

//   std::vector<bool> is_alive(TestN, true);
//   raft_node_id_t f1 = 1;
//   is_alive[f1] = false;

//   CodeConversionManagement ccm(TestK, TestF, r);
//   // Generate a random slice
//   auto slice = GenerateSliceOfRandomSize(512 * 1024, 1024 * 1024, r * TestK);

//   // Do encoding
//   StaticEncoder encoder;
//   encoder.Init(TestK, TestF);
//   ccm.EncodeForPlacement(slice, is_alive, &encoder);

//   // Gather the data
//   std::map<raft_node_id_t, ChunkVector> data;

//   /// !!!! Add another failure, adjust the data distribution !!!!
//   raft_node_id_t f2 = 5;
//   is_alive[f2] = false;
//   ccm.AdjustChunkDistribution(is_alive);

//   data.clear();
//   for (int i = 0; i < TestN; ++i) {
//     data.emplace(i, ccm.GetOriginalChunkVector(i));
//     data[i].Concatenate(ccm.GetReservedChunkVector(i));
//   }

//   // Randomly drop some data of failed servers
//   ASSERT_EQ(data.size(), TestN);
//   RandomDrop(data, TestK);
//   Slice recover_ent;

//   auto b = ccm.DecodeCollectedChunkVec(data, &recover_ent);

//   ASSERT_TRUE(b);
//   ASSERT_EQ(recover_ent.compare(slice), 0);
// }

TEST_F(CodeConversionManagementTest, TestEncodeAndDecode) {
  const int TestN = 7, TestK = 4, TestF = 3;
  auto total_chunk_num = get_chunk_count(TestK), r = total_chunk_num / TestK;
  StaticEncoder encoder;
  encoder.Init(TestK, TestF);

  auto test_case = [&](int failed_num) {
    std::vector<bool> is_alive(TestN, true);

    // Construct Failed servers:
    std::vector<raft_node_id_t> failed_servers(TestN);
    std::iota(failed_servers.begin(), failed_servers.end(), 0);
    RandomDrop(failed_servers, failed_num);

    auto is_failed = [&](int i) -> bool {
      return std::find(failed_servers.begin(), failed_servers.end(), i) != failed_servers.end();
    };

    std::for_each(failed_servers.begin(), failed_servers.end(),
                  [&](auto id) { is_alive[id] = false; });

    CodeConversionManagement ccm(TestK, TestF, r);

    // Generate a random slice
    auto slice = GenerateSliceOfRandomSize(512 * 1024, 1024 * 1024, r * TestK);

    ccm.Encode(slice, is_alive, &encoder);

    for (int i = 0; i < TestN; ++i) {
      if (is_failed(i)) {
        continue;
      }
      ASSERT_EQ(ccm.GetAssignedChunk(i).size(), slice.size() / TestK);
      ASSERT_EQ(ccm.GetAssignedSubChunkVector(i).size(), failed_num);
    }

    std::map<raft_node_id_t, DecodeInput> data;
    for (int i = 0; i < TestN; ++i) {
      data.insert_or_assign(
          i, std::make_pair(ccm.GetAssignedChunk(i), ccm.GetAssignedSubChunkVector(i)));
    }

    ASSERT_EQ(data.size(), TestN);

    // Drop some nodes' data to check if original slice can be recovered
    for (int i = 0; i < TestN - TestK; ++i) {
      if (!is_failed(i)) {
        data.erase(i);
      }
    }

    Slice recover_ent;
    auto b = ccm.Decode(data, &recover_ent);

    ASSERT_TRUE(b);
    ASSERT_EQ(recover_ent.compare(slice), 0);
  };

  test_case(0);  // No server failed
  test_case(1);  // One server failed
  test_case(2);  // two servers failed
  test_case(3);  // three servers failed
}

TEST_F(CodeConversionManagementTest, TestAdjustNewLivenessCase) {
  const int TestN = 7, TestK = 4, TestF = 3;
  auto total_chunk_num = get_chunk_count(TestK), r = total_chunk_num / TestK;
  StaticEncoder encoder;
  encoder.Init(TestK, TestF);

  auto test_case = [&](int failed_num) {
    std::vector<bool> is_alive(TestN, true);

    // Generate a random slice
    auto slice = GenerateSliceOfRandomSize(512 * 1024, 1024 * 1024, r * TestK);

    // Construct Failed servers:
    std::vector<raft_node_id_t> failed_servers(TestN);
    std::iota(failed_servers.begin(), failed_servers.end(), 0);
    RandomDrop(failed_servers, failed_num);

    auto is_failed = [&](int i) -> bool {
      return std::find(failed_servers.begin(), failed_servers.end(), i) != failed_servers.end();
    };

    std::for_each(failed_servers.begin(), failed_servers.end(),
                  [&](auto id) { is_alive[id] = false; });

    CodeConversionManagement ccm(TestK, TestF, r);

    ccm.Encode(slice, is_alive, &encoder);

    for (int i = 0; i < TestN; ++i) {
      if (is_failed(i)) {
        continue;
      }
      ASSERT_EQ(ccm.GetAssignedChunk(i).size(), slice.size() / TestK);
      ASSERT_EQ(ccm.GetAssignedSubChunkVector(i).size(), failed_num);
    }

    // Adjust the data placement
    for (int i = 0; i < TestN; ++i) {
      if (!is_failed(i)) {
        failed_servers.emplace_back(i);
        is_alive[i] = false;
        break;
      }
    }

    ccm.AdjustNewLivenessVector(is_alive);

    std::map<raft_node_id_t, DecodeInput> data;
    for (int i = 0; i < TestN; ++i) {
      data.insert_or_assign(
          i, std::make_pair(ccm.GetAssignedChunk(i), ccm.GetAssignedSubChunkVector(i)));
    }

    ASSERT_EQ(data.size(), TestN);

    // Drop some nodes' data to check if original slice can be recovered
    for (int i = 0; i < TestN - TestK; ++i) {
      if (!is_failed(i)) {
        data.erase(i);
      }
    }

    Slice recover_ent;
    auto b = ccm.Decode(data, &recover_ent);

    ASSERT_TRUE(b);
    ASSERT_EQ(recover_ent.compare(slice), 0);
  };

  test_case(0);
  test_case(1);
  test_case(2);
}

TEST_F(CodeConversionManagementTest, TestRemoveChunks) {
  const int TestN = 7, TestK = 4, TestF = 3;
  auto total_chunk_num = get_chunk_count(TestK), r = total_chunk_num / TestK;
  StaticEncoder encoder;
  encoder.Init(TestK, TestF);

  std::vector<bool> is_alive(TestN, true);

  int f1 = 1, f2 = 2;
  is_alive[f1] = is_alive[f2] = false;

  // Generate a random slice
  auto slice = GenerateSliceOfRandomSize(512 * 1024, 1024 * 1024, r * TestK);

  // Construct Failed servers:
  CodeConversionManagement ccm(TestK, TestF, r);

  ccm.Encode(slice, is_alive, &encoder);

  for (int i = 0; i < TestN; ++i) {
    if (i == f1 || i == f2) {
      continue;
    }
    ASSERT_EQ(ccm.GetAssignedChunk(i).size(), slice.size() / TestK);
    ASSERT_EQ(ccm.GetAssignedSubChunkVector(i).size(), 2);

    auto scv = ccm.GetAssignedSubChunkVector(i);
    scv.RemoveSubChunk(f1);

    // After remove:
    ASSERT_EQ(scv.size(), 1);

    for (auto iter = scv.Iter(); iter != scv.EndIter(); ++iter) {
      ASSERT_EQ(iter->GetSubChunkInfo().ChunkId(), f2);
    }
  }
}

};  // namespace raft
