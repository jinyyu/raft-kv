#pragma once
#include <stdint.h>

const uint8_t TransportTypeDebug = 0;

namespace kvd
{
#pragma pack(1)
struct TransportMeta
{
    uint8_t type;
    uint32_t len;
    uint8_t data[0];
};
#pragma pack()

}
