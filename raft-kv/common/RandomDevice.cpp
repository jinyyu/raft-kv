#include <raft-kv/common/RandomDevice.h>

namespace kv {
uint32_t RandomDevice::gen() {
  return static_cast<uint32_t>(distribution_(gen_));
}

}


