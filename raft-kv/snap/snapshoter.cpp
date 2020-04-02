#include <raft-kv/snap/snapshoter.h>
#include <boost/filesystem.hpp>
#include <raft-kv/common/log.h>
#include <msgpack.hpp>
#include <raft-kv/raft/util.h>

namespace kv {

struct SnapshotRecord {
  uint32_t data_len;
  uint32_t crc32;
  char data[0];
};

Status Snapshoter::load(proto::Snapshot& snapshot) {
  std::vector<std::string> names;
  get_snap_names(names);

  for (std::string& filename : names) {
    Status status = load_snap(filename, snapshot);
    if (status.is_ok()) {
      return Status::ok();
    }
  }

  return Status::not_found("snap not found");
}

Status Snapshoter::save_snap(const proto::Snapshot& snapshot) {
  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, snapshot);
  SnapshotRecord* record = (SnapshotRecord*) malloc(sbuf.size() + sizeof(SnapshotRecord));
  record->data_len = sbuf.size();

  char save_path[128];
  snprintf(save_path,
           sizeof(save_path),
           "%s/0x%016lu-%016lu.snap",
           dir_.c_str(),
           snapshot.metadata.term,
           snapshot.metadata.index);

  record->crc32 = compute_crc32(record->data, record->data_len);
  FILE* fp = fopen(save_path, "w");
  if (!fp) {
    free(record);
    return Status::io_error(strerror(errno));
  }
  fwrite(&record, 1, sizeof(SnapshotRecord) + record->data_len, fp);
  free(record);
  fclose(fp);
  return Status::ok();
}

void Snapshoter::get_snap_names(std::vector<std::string>& names) {
  using namespace boost;

  filesystem::directory_iterator end;
  for (boost::filesystem::directory_iterator it(dir_); it != end; it++) {
    filesystem::path filename = (*it).path().filename();
    filesystem::path extension = filename.extension();
    if (extension != ".snap") {
      continue;
    }
    names.push_back(filename.string());
  }
  std::sort(names.begin(), names.end(), std::greater<std::string>());
}

Status Snapshoter::load_snap(const std::string& filename, proto::Snapshot& snapshot) {
  using namespace boost;
  SnapshotRecord snap_hdr;
  std::vector<char> data;
  filesystem::path path = filesystem::path(dir_) / filename;
  FILE* fp = fopen(path.c_str(), "r");

  if (!fp) {
    goto invalid_snap;
  }
  if (fread(&snap_hdr, 1, sizeof(SnapshotRecord), fp) != sizeof(SnapshotRecord)) {
    goto invalid_snap;
  }

  if (snap_hdr.data_len == 0 || snap_hdr.crc32 == 0) {
    goto invalid_snap;
  }

  data.resize(snap_hdr.data_len);
  if (fread(data.data(), 1, snap_hdr.data_len, fp) != snap_hdr.data_len) {
    goto invalid_snap;
  }

  fclose(fp);
  fp = NULL;
  if (compute_crc32(data.data(), data.size()) != snap_hdr.crc32) {
    goto invalid_snap;
  }

  try {

    msgpack::object_handle oh = msgpack::unpack((const char*) data.data(), data.size());
    oh.get().convert(snapshot);
    return Status::ok();

  } catch (std::exception& e) {
    goto invalid_snap;
  }

invalid_snap:
  if (fp) {
    fclose(fp);
  }
  LOG_INFO("broken snapshot %s", path.string().c_str());
  filesystem::rename(path, path.string() + ".broken");
  return Status::io_error("unexpected empty snapshot");
}

}
