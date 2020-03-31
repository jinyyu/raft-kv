#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <iostream>
#include <raft-kv/common/status.h>

namespace kv {

Status::~Status() {
  if (status_) {
    free(status_);
  }
}

Status::Status(const Status& s) {
  status_ = copy(s);
}

Status& Status::operator=(const Status& s) {
  if (status_ != nullptr) {
    free(status_);
  }
  status_ = copy(s);
  return *this;
}

char* Status::copy(const Status& s) {
  if (s.status_ == nullptr) {
    return nullptr;
  } else {
    uint32_t len;
    memcpy(&len, s.status_, sizeof(uint32_t));

    char* status = (char*) malloc(len + 5);
    memcpy(status, s.status_, len + 5);
    return status;
  }
}

std::string Status::to_string() const {
  if (is_ok()) {
    return "ok";
  }

  const char* str;
  char tmp[30];
  Code c = code();
  switch (c) {
    case Code::OK :str = "ok";
      break;
    case Code::NotFound :str = "not found:";
      break;
    case Code::NotSupported :str = "not supported:";
      break;
    case Code::InvalidArgument :str = "invalid argument:";
      break;
    case Code::IOError :str = "io error:";
      break;
    default: {
      snprintf(tmp, sizeof(tmp), "Unknown code(%d):", c);
      str = tmp;
    }
  }

  std::string ret(str);
  uint32_t length;
  memcpy(&length, status_, sizeof(length));
  if (length > 0) {
    ret.append(status_ + 5, length);

  } else {
    ret.pop_back();
  }

  return ret;

}

Status::Status(Code code, const char* msg) {
  uint32_t len;
  if (msg == nullptr) {
    len = 0;
  } else {
    len = strlen(msg);
  }
  status_ = (char*) malloc(len + 5);
  memcpy(status_, &len, sizeof(uint32_t));
  status_[4] = code;
  memcpy(status_ + 5, msg, len);
}

}
