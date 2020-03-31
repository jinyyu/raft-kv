#include <raft-kv/common/random_device.h>

namespace kv {
uint32_t RandomDevice::gen() {
  return static_cast<uint32_t>(distribution_(gen_));
}

}


