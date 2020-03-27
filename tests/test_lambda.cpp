
#include <gtest/gtest.h>

TEST(lambda, ref) {
  int i = 0;
  auto f = [&i]() {
    i = 10;
  };
  f();
  ASSERT_TRUE(i == 10);
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}