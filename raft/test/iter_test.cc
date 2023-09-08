#include "iter.h"

#include <numeric>

#include "gtest/gtest.h"

TEST(TestIter, TestFilterIterator) {
  std::vector<int> vec(10);
  std::iota(vec.begin(), vec.end(), 1);
  auto res = NewContainerIter(vec)
                 ->filter([](int a) { return a % 2 == 1; })
                 ->filter([](int a) { return a >= 5; })
                 ->map_as([](int a) { return a + 10; })
                 ->map_as([](int a) { return a * a; });
  ASSERT_TRUE(res->valid());

  for (int i = 5; i <= 10; i += 2) {
    ASSERT_EQ((i + 10) * (i + 10), res->value());
    res->next();
  }

  auto c = res->collect<std::vector<int>>();
  
  ASSERT_EQ(c.size(), 3);
  ASSERT_EQ(c[0], 15 * 15);
  ASSERT_EQ(c[1], 17 * 17);
  ASSERT_EQ(c[2], 19 * 19);
}
