#include <raft-kv/wal/wal.h>
#include <raft-kv/common/log.h>
#include <boost/filesystem.hpp>
#include <fcntl.h>
#include <raft-kv/raft/util.h>
#include <sstream>
#include <inttypes.h>
#include <raft-kv/raft/util.h>

namespace kv {

static const WAL_type wal_InvalidType = 0;
static const WAL_type wal_EntryType = 1;
static const WAL_type wal_StateType = 2;
static const WAL_type wal_CrcType = 3;
static const WAL_type wal_snapshot_Type = 4;
static const int SegmentSizeBytes = 64 * 1000 * 1000; // 64MB

static std::string wal_name(uint64_t seq, uint64_t index) {
  char buffer[64];
  snprintf(buffer, sizeof(buffer), "%016" PRIx64 "-%016" PRIx64 ".wal", seq, index);
  return buffer;
}

class WAL_File {
 public:
  WAL_File(const char* path, int64_t seq)
      : seq(seq),
        file_size(0) {
    fp = fopen(path, "a+");
    if (!fp) {
      LOG_FATAL("fopen error %s", strerror(errno));
    }

    file_size = ftell(fp);
    if (file_size == -1) {
      LOG_FATAL("ftell error %s", strerror(errno));
    }

    if (fseek(fp, 0L, SEEK_SET) == -1) {
      LOG_FATAL("fseek error %s", strerror(errno));
    }
  }

  ~WAL_File() {
    fclose(fp);
  }

  void truncate(size_t offset) {
    if (ftruncate(fileno(fp), offset) != 0) {
      LOG_FATAL("ftruncate error %s", strerror(errno));
    }

    if (fseek(fp, offset, SEEK_SET) == -1) {
      LOG_FATAL("fseek error %s", strerror(errno));
    }

    file_size = offset;
    data_buffer.clear();
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

    file_size += data_buffer.size();
    data_buffer.clear();
  }

  void read_all(std::vector<char>& out) {
    char buffer[1024];
    while (true) {
      size_t bytes = fread(buffer, 1, sizeof(buffer), fp);
      if (bytes <= 0) {
        break;
      }

      out.insert(out.end(), buffer, buffer + bytes);
    }

    fseek(fp, 0L, SEEK_END);
  }

  std::vector<uint8_t> data_buffer;
  int64_t seq;
  long file_size;
  FILE* fp;
};

void WAL::create(const std::string& dir) {
  using namespace boost;

  filesystem::path walFile = filesystem::path(dir) / wal_name(0, 0);
  std::string tmpPath = walFile.string() + ".tmp";

  if (filesystem::exists(tmpPath)) {
    filesystem::remove(tmpPath);
  }

  {
    std::shared_ptr<WAL_File> wal(new WAL_File(tmpPath.c_str(), 0));
    WAL_Snapshot snap;
    snap.term = 0;
    snap.index = 0;
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, snap);
    wal->append(wal_snapshot_Type, (uint8_t*) sbuf.data(), sbuf.size());
    wal->sync();
  }

  filesystem::rename(tmpPath, walFile);
}

WAL_ptr WAL::open(const std::string& dir, const WAL_Snapshot& snap) {
  WAL_ptr w(new WAL(dir));

  std::vector<std::string> names;
  w->get_wal_names(dir, names);
  if (names.empty()) {
    LOG_FATAL("wal not found");
  }

  uint64_t nameIndex;
  if (!WAL::search_index(names, snap.index, &nameIndex)) {
    LOG_FATAL("wal not found");
  }

  std::vector<std::string> check_names(names.begin() + nameIndex, names.end());
  if (!WAL::is_valid_seq(check_names)) {
    LOG_FATAL("invalid wal seq");
  }

  for (const std::string& name: check_names) {
    uint64_t seq;
    uint64_t index;
    if (!parse_wal_name(name, &seq, &index)) {
      LOG_FATAL("invalid wal name %s", name.c_str());
    }

    boost::filesystem::path path = boost::filesystem::path(w->dir_) / name;
    std::shared_ptr<WAL_File> file(new WAL_File(path.string().c_str(), seq));
    w->files_.push_back(file);
  }

  memcpy(&w->start_, &snap, sizeof(snap));
  return w;
}

Status WAL::read_all(proto::HardState& hs, std::vector<proto::EntryPtr>& ents) {
  std::vector<char> data;
  for (auto file : files_) {
    data.clear();
    file->read_all(data);
    size_t offset = 0;
    bool matchsnap = false;

    while (offset < data.size()) {
      size_t left = data.size() - offset;
      size_t record_begin_offset = offset;

      if (left < sizeof(WAL_Record)) {
        file->truncate(record_begin_offset);
        LOG_WARN("invalid record len %lu", left);
        break;
      }

      WAL_Record record;
      memcpy(&record, data.data() + offset, sizeof(record));

      left -= sizeof(record);
      offset += sizeof(record);

      if (record.type == wal_InvalidType) {
        break;
      }

      uint32_t record_data_len = WAL_Record_len(record);
      if (left < record_data_len) {
        file->truncate(record_begin_offset);
        LOG_WARN("invalid record data len %lu, %u", left, record_data_len);
        break;
      }

      char* data_ptr = data.data() + offset;
      uint32_t crc = compute_crc32(data_ptr, record_data_len);

      left -= record_data_len;
      offset += record_data_len;

      if (record.crc != 0 && crc != record.crc) {
        file->truncate(record_begin_offset);
        LOG_WARN("invalid record crc %u, %u", record.crc, crc);
        break;
      }

      handle_record_wal_record(record.type, data_ptr, record_data_len, matchsnap, hs, ents);

      if (record.type == wal_snapshot_Type) {
        matchsnap = true;
      }
    }

    if (!matchsnap) {
      LOG_FATAL("wal: snapshot not found");
    }
  }

  return Status::ok();
}

void WAL::handle_record_wal_record(WAL_type type,
                                   const char* data,
                                   size_t data_len,
                                   bool& matchsnap,
                                   proto::HardState& hs,
                                   std::vector<proto::EntryPtr>& ents) {

  switch (type) {
    case wal_EntryType: {
      proto::EntryPtr entry(new proto::Entry());
      msgpack::object_handle oh = msgpack::unpack(data, data_len);
      oh.get().convert(*entry);

      if (entry->index > start_.index) {
        ents.resize(entry->index - start_.index - 1);
        ents.push_back(entry);
      }

      enti_ = entry->index;
      break;
    }

    case wal_StateType: {
      msgpack::object_handle oh = msgpack::unpack(data, data_len);
      oh.get().convert(hs);
      break;
    }

    case wal_snapshot_Type: {
      WAL_Snapshot snap;
      msgpack::object_handle oh = msgpack::unpack(data, data_len);
      oh.get().convert(snap);

      if (snap.index == start_.index) {
        if (snap.term != start_.term) {
          LOG_FATAL("wal: snapshot mismatch");
        }
        matchsnap = true;
      }
      break;
    }

    case wal_CrcType: {
      LOG_FATAL("wal crc type");
      break;
    }
    default: {
      LOG_FATAL("invalid record type %d", type);
    }
  }
}

Status WAL::save(proto::HardState hs, const std::vector<proto::EntryPtr>& ents) {
  // short cut, do not call sync
  if (hs.is_empty_state() && ents.empty()) {
    return Status::ok();
  }

  bool mustSync = is_must_sync(hs, state_, ents.size());
  Status status;

  for (const proto::EntryPtr& entry: ents) {
    status = save_entry(*entry);
    if (!status.is_ok()) {
      return status;
    }
  }

  status = save_hard_state(hs);
  if (!status.is_ok()) {
    return status;
  }

  if (files_.back()->file_size < SegmentSizeBytes) {
    if (mustSync) {
      files_.back()->sync();
    }
    return Status::ok();
  }

  return cut();
}

Status WAL::cut() {
  files_.back()->sync();
  return Status::ok();
}

Status WAL::save_snapshot(const WAL_Snapshot& snap) {
  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, snap);

  files_.back()->append(wal_snapshot_Type, (uint8_t*)sbuf.data(), sbuf.size());
  if (enti_ < snap.index) {
    enti_ = snap.index;
  }
  files_.back()->sync();
  return Status::ok();
}

Status WAL::save_entry(const proto::Entry& entry) {
  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, entry);

  files_.back()->append(wal_EntryType, (uint8_t*)sbuf.data(), sbuf.size());
  enti_ = entry.index;
  return Status::ok();
}

Status WAL::save_hard_state(const proto::HardState& hs) {
  if (hs.is_empty_state()) {
    return Status::ok();
  }
  state_ = hs;

  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, hs);
  files_.back()->append(wal_StateType, (uint8_t*)sbuf.data(), sbuf.size());
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

Status WAL::release_to(uint64_t index) {
  return Status::ok();
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

bool WAL::is_valid_seq(const std::vector<std::string>& names) {
  uint64_t lastSeq = 0;
  for (const std::string& name: names) {
    uint64_t curSeq;
    uint64_t i;
    if (!WAL::parse_wal_name(name, &curSeq, &i)) {
      LOG_FATAL("parse correct name should never fail %s", name.c_str());
    }

    if (lastSeq != 0 && lastSeq != curSeq - 1) {
      return false;
    }
    lastSeq = curSeq;
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
