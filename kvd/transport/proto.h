#pragma once
#include <stdint.h>

const uint8_t TransportTypeDebug = 5;

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

#pragma pack(1)
struct DebugMessage
{
    uint32_t a;
    uint32_t b;
};
#pragma pack()

}
