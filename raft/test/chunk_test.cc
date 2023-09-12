#include "chunk.h"

#include "gtest/gtest.h"
#include "raft_type.h"

class ChunkTest : public ::testing::Test {};

using namespace raft;
using namespace CODE_CONVERSION_NAMESPACE;

TEST_F(ChunkTest, DISABLED_TestNoServerFailure) {
  int N = 7, F = 3, k = 4, m = 3;
  int r = get_chunk_count(k) / k;

  ChunkDistribution cd(k, F, r);

  std::vector<bool> aliveness(N, true);
  cd.GenerateChunkDistribution(aliveness);

  ASSERT_EQ(cd.GetPlacementInfo().get_fail_servers_num(), 0);
  ASSERT_EQ(cd.GetPlacementInfo().get_reservation_server_num(), 0);
  ASSERT_EQ(cd.GetReservedChunkCountForEach(), 0);

  for (int i = 0; i < N; ++i) {
    ASSERT_EQ(cd.GetAssignedOrgChunks(i).size(), r);
  }

  for (int node_id = 0; node_id < N; ++node_id) {
    for (int c_idx = 0; c_idx < r; ++c_idx) {
      ASSERT_EQ(cd.GetAssignedOrgChunks(node_id)[c_idx], ChunkIndex(node_id, c_idx));
    }
  }
}

TEST_F(ChunkTest, TestOneServerFailure) {
  int N = 7, F = 3, k = 4, m = 3;
  int r = get_chunk_count(k) / k;

  ASSERT_EQ(r, 6);

  ChunkDistribution cd(k, F, r);

  int f = 1;
  std::vector<bool> aliveness(N, true);
  aliveness[f] = false;
  cd.GenerateChunkDistribution(aliveness);

  ASSERT_EQ(cd.GetPlacementInfo().get_fail_servers_num(), 1);
  ASSERT_EQ(cd.GetPlacementInfo().get_reservation_server_num(), 3);
  ASSERT_EQ(cd.GetReservedChunkCountForEach(), 2);

  ASSERT_EQ(cd.GetPlacementInfo().get_reservation_servers(),
            std::vector<raft_node_id_t>({0, 2, 3}));

  for (int node_id = 0; node_id < N; ++node_id) {
    if (node_id == f) continue;
    for (int cidx = 0; cidx < r; ++cidx) {
      ASSERT_EQ(cd.GetAssignedOrgChunks(node_id).at(cidx), ChunkIndex(node_id, cidx));
    }
  }

  ASSERT_EQ(cd.GetAssignedReserveChunks(0).at(0), ChunkIndex(1, 0));
  ASSERT_EQ(cd.GetAssignedReserveChunks(0).at(1), ChunkIndex(1, 1));

  ASSERT_EQ(cd.GetAssignedReserveChunks(2).at(0), ChunkIndex(1, 2));
  ASSERT_EQ(cd.GetAssignedReserveChunks(2).at(1), ChunkIndex(1, 3));

  ASSERT_EQ(cd.GetAssignedReserveChunks(3).at(0), ChunkIndex(1, 4));
  ASSERT_EQ(cd.GetAssignedReserveChunks(3).at(1), ChunkIndex(1, 5));
}

TEST_F(ChunkTest, TestTwoServersFailure) {
  int N = 7, F = 3, k = 4, m = 3;
  int r = get_chunk_count(k) / k;

  ASSERT_EQ(r, 6);

  ChunkDistribution cd(k, F, r);

  int f1 = 1, f2 = 5;
  std::vector<bool> aliveness(N, true);
  aliveness[f1] = aliveness[f2] = false;
  cd.GenerateChunkDistribution(aliveness);

  ASSERT_EQ(cd.GetPlacementInfo().get_fail_servers_num(), 2);
  ASSERT_EQ(cd.GetPlacementInfo().get_reservation_server_num(), 2);
  ASSERT_EQ(cd.GetReservedChunkCountForEach(), 6);

  ASSERT_EQ(cd.GetPlacementInfo().get_reservation_servers(), std::vector<raft_node_id_t>({0, 2}));

  for (int node_id = 0; node_id < N; ++node_id) {
    if (node_id == f1 || node_id == f2) continue;
    for (int cidx = 0; cidx < r; ++cidx) {
      ASSERT_EQ(cd.GetAssignedOrgChunks(node_id).at(cidx), ChunkIndex(node_id, cidx));
    }
  }

  ASSERT_EQ(cd.GetAssignedReserveChunks(0).at(0), ChunkIndex(f1, 0));
  ASSERT_EQ(cd.GetAssignedReserveChunks(0).at(1), ChunkIndex(f1, 1));
  ASSERT_EQ(cd.GetAssignedReserveChunks(0).at(2), ChunkIndex(f1, 2));

  ASSERT_EQ(cd.GetAssignedReserveChunks(2).at(0), ChunkIndex(f1, 3));
  ASSERT_EQ(cd.GetAssignedReserveChunks(2).at(1), ChunkIndex(f1, 4));
  ASSERT_EQ(cd.GetAssignedReserveChunks(2).at(2), ChunkIndex(f1, 5));

  ASSERT_EQ(cd.GetAssignedReserveChunks(0).at(3), ChunkIndex(f2, 0));
  ASSERT_EQ(cd.GetAssignedReserveChunks(0).at(4), ChunkIndex(f2, 1));
  ASSERT_EQ(cd.GetAssignedReserveChunks(0).at(5), ChunkIndex(f2, 2));

  ASSERT_EQ(cd.GetAssignedReserveChunks(2).at(3), ChunkIndex(f2, 3));
  ASSERT_EQ(cd.GetAssignedReserveChunks(2).at(4), ChunkIndex(f2, 4));
  ASSERT_EQ(cd.GetAssignedReserveChunks(2).at(5), ChunkIndex(f2, 5));
}