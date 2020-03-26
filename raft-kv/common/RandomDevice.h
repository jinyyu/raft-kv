#pragma once
#include <random>

namespace kv
{

class RandomDevice
{
public:
    explicit RandomDevice(uint32_t min, std::uint32_t max)
        : gen_(rd_()),
          distribution_(min, max)
    {

    }

    uint32_t gen();

private:
    std::random_device rd_;
    std::mt19937 gen_;
    std::uniform_int_distribution<> distribution_;
};

}
