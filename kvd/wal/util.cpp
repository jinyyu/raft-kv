#include <kvd/wal/util.h>

namespace kvd
{
namespace wal
{

bool exists(const std::string& dir)
{
    using namespace boost;
    if (!filesystem::exists(dir)) {
        return false;
    }
    filesystem::directory_iterator end; // default construction yields past-the-end
    for (filesystem::directory_iterator it(dir); it != end; ++it) {
        filesystem::path name = it->path().filename();
        if (name.has_extension()) {
            if (name.extension() == ".wal") {
                return true;
            }
        }
    }
    return false;
}

}
}