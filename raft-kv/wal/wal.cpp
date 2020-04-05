#include <raft-kv/wal/wal.h>
#include <raft-kv/common/log.h>
#include <boost/filesystem.hpp>
#include <fcntl.h>
#include <raft-kv/raft/util.h>
#include <sstream>

namespace kv {

static const WAL_type wal_EntryType = 0;
static const WAL_type wal_StateType = 1;
static const WAL_type wal_CrcType = 2;
static const WAL_type wal_snapshot_Type = 3;
static const int SegmentSizeBytes = 64 * 1000 * 1000; // 64MB

static std::string wal_name(uint64_t seq, uint64_t index) {
  char buffer[64];
  snprintf(buffer, sizeof(buffer), "0x%016lu-0x%016lu.wal", seq, index);
  return buffer;
}

class WAL_File {
 public:
  WAL_File(const char* path, const char* mode, int64_t seq)
      : seq(seq),
        file_size(0) {
    fp = fopen(path, mode);
    if (!fp) {
      LOG_FATAL("fopen error %s", strerror(errno));
    }
  }

  void preallocate() {
    int fd = fileno(fp);

    if (!posix_fallocate(fd, 0, SegmentSizeBytes)) {
      LOG_WARN("posix_fallocate error %s", strerror(errno));
    }
  }

  ~WAL_File() {
    fclose(fp);
  }

  void append(WAL_type type, const uint8_t* data, size_t len) {
    WAL_Record record;
    record.type = type;
    record.crc = compute_crc32((char*) data, len);
    set_WAL_Record_len(record, len);
    uint8_t* ptr = (uint8_t*) &record;
    data_buffer.insert(data_buffer.end(), ptr, ptr + sizeof(record));
    data_buffer.insert(data_buffer.end(), data, data + len);
  }

  void sync() {
    if (data_buffer.empty()) {
      return;
    }

    size_t bytes = fwrite(data_buffer.data(), 1, data_buffer.size(), fp);
    if (bytes != data_buffer.size()) {
      LOG_FATAL("fwrite error %s", strerror(errno));
    }
    data_buffer.clear();
  }

  std::vector<uint8_t> data_buffer;
  int64_t seq;
  uint64_t file_size;
  FILE* fp;
};

WAL_ptr WAL::create(const std::string& dir) {
  using namespace boost;

  filesystem::path walFile = filesystem::path(dir) / wal_name(0, 0);
  std::string tmpPath = walFile.string() + ".tmp";

  if (filesystem::exists(tmpPath)) {
    filesystem::remove(tmpPath);
  }

  {
    std::shared_ptr<WAL_File> wal(new WAL_File(tmpPath.c_str(), "w", 0));
    wal->preallocate();
    WAL_Snapshot snap;
    snap.term = 0;
    snap.index = 0;
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, snap);
    wal->append(wal_snapshot_Type, (uint8_t*) sbuf.data(), sbuf.size());
    wal->sync();
  }

  filesystem::rename(tmpPath, walFile);

  WAL_Snapshot snap;
  snap.index = 0;
  snap.term = 0;
  return WAL::open(dir, snap);
}

WAL_ptr WAL::open(const std::string& dir, const WAL_Snapshot& snap) {
  WAL_ptr w(new WAL(dir));

  std::vector<std::string> names;
  w->get_wal_names(dir, names);

  return w;
}

Status WAL::read_all(proto::HardState& hs, std::vector<proto::EntryPtr>& ents) {
  return Status::ok();
}

Status WAL::save(proto::HardState hs, const std::vector<proto::EntryPtr>& ents) {
  return Status::ok();
}

Status WAL::save_snapshot(const WAL_Snapshot& snap) {
  return Status::ok();
}

void WAL::get_wal_names(const std::string& dir, std::vector<std::string>& names) {
  using namespace boost;

  filesystem::directory_iterator end;
  for (boost::filesystem::directory_iterator it(dir); it != end; it++) {
    filesystem::path filename = (*it).path().filename();
    filesystem::path extension = filename.extension();
    if (extension != ".wal") {
      continue;
    }
    names.push_back(filename.string());
  }
  std::sort(names.begin(), names.end(), std::less<std::string>());
}

bool WAL::parse_wal_name(const std::string& name, uint64_t* seq, uint64_t* index) {
  *seq = 0;
  *index = 0;

  boost::filesystem::path path(name);
  if (path.extension() != ".wal") {
    return false;
  }

  std::string filename = name.substr(0, name.size() - 4); // trim ".wal"
  size_t pos = filename.find('-');
  if (pos == std::string::npos) {
    return false;
  }

  try {
    {
      std::string str = filename.substr(0, pos);
      std::stringstream ss;
      ss << std::hex << str;
      ss >> *seq;
    }

    {
      if (pos == filename.size() - 1) {
        return false;
      }
      std::string str = filename.substr(pos + 1, filename.size() - pos - 1);
      std::stringstream ss;
      ss << std::hex << str;
      ss >> *index;
    }
  } catch (...) {
    return false;
  }
  return true;
}

// searchIndex returns the last array index of names whose raft index section is
// equal to or smaller than the given index.
// The given names MUST be sorted.
bool WAL::search_index(const std::vector<std::string>& names, uint64_t index, uint64_t* name_index) {

  for (size_t i = names.size() - 1; i >= 0; --i) {
    const std::string& name = names[i];
    uint64_t seq;
    uint64_t curIndex;
    if (!parse_wal_name(name, &seq, &curIndex)) {
      LOG_FATAL("invalid wal name %s", name.c_str());
    }

    if (index >= curIndex) {
      *name_index = i;
      return true;
    }
    if (i == 0) {
      break;
    }
  }
  *name_index = -1;
  return false;
}

}
