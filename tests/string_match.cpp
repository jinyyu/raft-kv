#include <regex>
#include <gtest/gtest.h>
#include <raft-kv/server/RedisServer.h>
using namespace kv;

TEST(match, match)
{
    struct Test
    {
        std::string pattern;
        std::string key;
        int match;
    };

    std::vector<Test> tests;
    tests.push_back(Test{.pattern = "*", .key = "abc", .match = 1});
    tests.push_back(Test{.pattern = "a*c", .key = "abc", .match = 1});
    tests.push_back(Test{.pattern = "a*", .key = "abc", .match = 1});
    tests.push_back(Test{.pattern = "b*", .key = "abc", .match = 0});
    tests.push_back(Test{.pattern = "*c", .key = "abc", .match = 1});
    tests.push_back(Test{.pattern = "*a", .key = "abc", .match = 0});
    tests.push_back(Test{.pattern = "*b*", .key = "abc", .match = 1});
    tests.push_back(Test{.pattern = "", .key = "abc", .match = 0});


    for (Test& t :  tests) {
        int match = string_match_len(t.pattern.data(), t.pattern.size(), t.key.data(), t.key.size(), 0);
        ASSERT_TRUE(match == t.match);

    }
}

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
