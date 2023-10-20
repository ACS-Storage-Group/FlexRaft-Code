#include "subchunk.h"

#include "gtest/gtest.h"
#include "test_util.h"

using namespace raft;
using namespace CODE_CONVERSION_NAMESPACE;

class SubChunkVectorTest : public ::testing::Test {
 public:
  SubChunkInfo GenerateRandomSubChunkInfo() { return SubChunkInfo(rand() % 100, rand() % 100); }

  SubChunk ConstructRandomSubChunk(size_t min_sz, size_t max_sz) {
    return SubChunk(GenerateRandomSubChunkInfo(),
                    test::GenerateRandomSlice(test::GenRandSize(min_sz, max_sz)));
  }
};

TEST_F(SubChunkVectorTest, SerializeSubChunkTest) {
  auto sub_chunk = ConstructRandomSubChunk(512 * 1024,  1024 * 1024);
  auto slice = sub_chunk.Serialize();
  SubChunk recover_chunk;
  recover_chunk.Deserialize(slice.data());
  ASSERT_EQ(recover_chunk, sub_chunk);
}

TEST_F(SubChunkVectorTest, SerializeTest) {
  SubChunkVector vec;
  int test_cnt = 10;

  for (int i = 0; i < test_cnt; ++i) {
    vec.AddSubChunk(ConstructRandomSubChunk(512 * 1024, 1024 * 1024));
  }

  // Ser
  auto slice = vec.Serialize();

  SubChunkVector recover_vec;
  recover_vec.Deserialize(slice.data());

  // Assert that these two vectors are identical
  ASSERT_EQ(recover_vec, vec);
}
