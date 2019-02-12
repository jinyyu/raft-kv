#include <kvd/common/RandomDevice.h>


namespace kvd
{
uint32_t RandomDevice::gen()
{
    return static_cast<uint32_t>(distribution_(gen_));
}

}


