#pragma once
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
namespace kv {
namespace wal {

// exists returns true if there are any files in a given directory.
bool exists(const std::string& dir);

}
}
