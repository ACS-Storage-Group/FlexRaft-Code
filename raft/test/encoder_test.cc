#include "encoder.h"
#include <cstdlib>
#include <iterator>

#include "gtest/gtest.h"
#include "log_entry.h"
#include "raft_type.h"

namespace raft {
class EncoderTest : public ::testing::Test {
 public:
  auto GenerateRandomSlice(int min_len, int max_len) -> Slice {
    auto rand_size = rand() % (max_len - min_len) + min_len;
    // Add 16 so that the data can be accessed
    auto rand_data = new char[rand_size + 16];
    for (decltype(rand_size) i = 0; i < rand_size; ++i) {
      rand_data[i] = rand();
    }
    return Slice(rand_data, rand_size);
  }
};

TEST_F(EncoderTest, TestSimpleEncodingDecoding) {
  const int kTestK = 2;
  const int kTestM = 1;

  Encoder encoder;
  Encoder::EncodingResults results;
  Slice ent = GenerateRandomSlice(512, 1024);

  // Encoding
  encoder.EncodeSlice(ent, kTestK, kTestM, &results);

  // Decoding
  Slice recover_ent;
  encoder.DecodeSlice(results, kTestK, kTestM, &recover_ent);

  Slice origin_ent(recover_ent.data(), ent.size());
  ASSERT_EQ(origin_ent.compare(ent), 0);
}

TEST_F(EncoderTest, TestKEqualsToOne) {
  const int kTestK = 1;
  const int kTestM = 4;

  Encoder encoder;
  Encoder::EncodingResults results;
  Slice ent = GenerateRandomSlice(512, 1024);

  // Encoding
  encoder.EncodeSlice(ent, kTestK, kTestM, &results);

  // Decoding
  Slice recover_ent;
  encoder.DecodeSlice(results, kTestK, kTestM, &recover_ent);

  Slice origin_ent(recover_ent.data(), ent.size());
  ASSERT_EQ(origin_ent.compare(ent), 0);
}

TEST_F(EncoderTest, TestMIsBigger) {
  const int kTestK = 2;
  const int kTestM = 5;

  Encoder encoder;
  Encoder::EncodingResults results;
  Slice ent = GenerateRandomSlice(512, 1024);

  // Encoding
  encoder.EncodeSlice(ent, kTestK, kTestM, &results);

  results.erase(0);
  results.erase(1);
  results.erase(4);

  // Decoding
  Slice recover_ent;
  encoder.DecodeSlice(results, kTestK, kTestM, &recover_ent);

  Slice origin_ent(recover_ent.data(), ent.size());
  ASSERT_EQ(origin_ent.compare(ent), 0);
}

TEST_F(EncoderTest, TestDecodingAfterRemoveSomeFragments) {
  const int TestK = 5;
  const int TestM = 3;

  Encoder encoder;
  Encoder::EncodingResults results;
  Slice ent = GenerateRandomSlice(512, 1024);

  // Encoding
  encoder.EncodeSlice(ent, TestK, TestM, &results);

  // remove some fragments 
  while (results.size() > TestK) {
    auto remove_iter = std::next(std::begin(results), rand() % results.size());
    results.erase(remove_iter);
  }

  // Decoding
  Slice recover_ent;
  encoder.DecodeSlice(results, TestK, TestM, &recover_ent);

  Slice origin_ent(recover_ent.data(), ent.size());
  ASSERT_EQ(origin_ent.compare(ent), 0);
}

TEST_F(EncoderTest, TestEncodingDecodingLargeSlice) {
  const int TestK = 9;
  const int TestM = 9;

  Encoder encoder;
  Encoder::EncodingResults results;
  Slice ent = GenerateRandomSlice(512 * 1024, 1024 * 1024);

  // Encoding
  encoder.EncodeSlice(ent, TestK, TestM, &results);

  // remove some fragments 
  while (results.size() > TestK) {
    auto remove_iter = std::next(std::begin(results), rand() % results.size());
    results.erase(remove_iter);
  }

  // Decoding
  Slice recover_ent;
  encoder.DecodeSlice(results, TestK, TestM, &recover_ent);

  Slice origin_ent(recover_ent.data(), ent.size());
  ASSERT_EQ(origin_ent.compare(ent), 0);
}

}  // namespace raft
