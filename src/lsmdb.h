// lsmdb: a log-structured merge-tree storage engine.
//
// Design (single-threaded core, crash-safe by construction):
//
//   put/del --> WAL (fsync per batch) --> memtable (std::map)
//                                            | size threshold
//                                            v
//                                     SSTable L0 (sorted, immutable)
//                                            | L0 count threshold
//                                            v
//                                  compaction -> L1 (single sorted run)
//
// Durability contract: an acknowledged write (Put/Delete returning after
// Sync) survives kill -9 at ANY point. Recovery = load SSTables listed in
// the MANIFEST, then replay the WAL tail. Torn/corrupt WAL records at the
// tail are detected by CRC and discarded (they were never acknowledged).
//
// File formats, all little-endian:
//   WAL record : u32 crc | u32 klen | u32 vlen | u8 kind | key | value
//                (crc covers klen..value; kind: 1=put, 0=delete)
//   SSTable    : records (u32 klen | u32 vlen(-1 = tombstone) | key | value)*
//                then index (u32 count | (u32 klen | key | u64 offset)*)
//                then footer (u64 index_offset | u64 magic)
//   MANIFEST   : text: "level file_id\n" per live SSTable, written to a temp
//                file and renamed (atomic on POSIX).
#pragma once

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <unistd.h>
#include <vector>

namespace lsmdb {

namespace fs = std::filesystem;

// CRC32 (Castagnoli polynomial, table-driven).
inline uint32_t crc32c(const void *data, size_t n, uint32_t seed = 0) {
    static uint32_t table[256];
    static bool init = [] {
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int k = 0; k < 8; k++)
                c = (c & 1) ? 0x82F63B78u ^ (c >> 1) : c >> 1;
            table[i] = c;
        }
        return true;
    }();
    (void)init;
    uint32_t crc = ~seed;
    const uint8_t *p = (const uint8_t *)data;
    for (size_t i = 0; i < n; i++)
        crc = table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    return ~crc;
}

// A value or a tombstone.
struct Entry {
    std::string value;
    bool tombstone = false;
};

// ---------- WAL ----------
class Wal {
public:
    explicit Wal(const std::string &path) : path_(path) {
        fd_ = ::open(path.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
    }
    ~Wal() { if (fd_ >= 0) ::close(fd_); }

    bool ok() const { return fd_ >= 0; }

    // Append one record. Returns false on write error.
    bool Append(const std::string &key, const std::string &value, bool tombstone) {
        const uint32_t klen = (uint32_t)key.size();
        const uint32_t vlen = (uint32_t)value.size();
        const uint8_t kind = tombstone ? 0 : 1;
        std::string buf;
        buf.reserve(13 + klen + vlen);
        auto put32 = [&buf](uint32_t v) { buf.append((const char *)&v, 4); };
        put32(0); // crc placeholder
        put32(klen);
        put32(vlen);
        buf.push_back((char)kind);
        buf.append(key);
        buf.append(value);
        const uint32_t crc = crc32c(buf.data() + 4, buf.size() - 4);
        memcpy(buf.data(), &crc, 4);
        return ::write(fd_, buf.data(), buf.size()) == (ssize_t)buf.size();
    }

    // fsync. An acked write = Append + Sync returned true.
    bool Sync() { return ::fsync(fd_) == 0; }

    // Replay records into fn until EOF or a corrupt/torn record.
    // Returns the number of valid records applied.
    template <typename Fn>
    static size_t Replay(const std::string &path, Fn fn) {
        FILE *f = fopen(path.c_str(), "rb");
        if (!f) return 0;
        size_t applied = 0;
        std::string key, value;
        for (;;) {
            uint32_t hdr[3];
            if (fread(hdr, 4, 3, f) != 3) break;
            uint8_t kind;
            if (fread(&kind, 1, 1, f) != 1) break;
            const uint32_t crc = hdr[0], klen = hdr[1], vlen = hdr[2];
            if (klen > (1u << 30) || vlen > (1u << 30)) break; // insane = torn
            key.resize(klen);
            value.resize(vlen);
            if (klen && fread(key.data(), 1, klen, f) != klen) break;
            if (vlen && fread(value.data(), 1, vlen, f) != vlen) break;
            // recompute crc over klen|vlen|kind|key|value
            uint32_t c = crc32c(&hdr[1], 8);
            c = crc32c(&kind, 1, c);
            if (klen) c = crc32c(key.data(), klen, c);
            if (vlen) c = crc32c(value.data(), vlen, c);
            if (c != crc) break; // torn tail: stop, rest was never acked
            fn(key, value, kind == 0);
            applied++;
        }
        fclose(f);
        return applied;
    }

private:
    std::string path_;
    int fd_ = -1;
};

// ---------- SSTable ----------
class SSTable {
public:
    // Write a sorted map to path atomically (tmp + rename + fsync).
    static bool Write(const std::string &path,
                      const std::map<std::string, Entry> &entries) {
        const std::string tmp = path + ".tmp";
        FILE *f = fopen(tmp.c_str(), "wb");
        if (!f) return false;
        std::vector<std::pair<std::string, uint64_t>> index;
        uint64_t off = 0;
        auto w32 = [&](uint32_t v) { fwrite(&v, 4, 1, f); off += 4; };
        for (const auto &[k, e] : entries) {
            index.emplace_back(k, off);
            w32((uint32_t)k.size());
            w32(e.tombstone ? 0xFFFFFFFFu : (uint32_t)e.value.size());
            fwrite(k.data(), 1, k.size(), f);
            off += k.size();
            if (!e.tombstone) {
                fwrite(e.value.data(), 1, e.value.size(), f);
                off += e.value.size();
            }
        }
        const uint64_t index_off = off;
        uint32_t count = (uint32_t)index.size();
        fwrite(&count, 4, 1, f);
        for (const auto &[k, o] : index) {
            uint32_t klen = (uint32_t)k.size();
            fwrite(&klen, 4, 1, f);
            fwrite(k.data(), 1, k.size(), f);
            fwrite(&o, 8, 1, f);
        }
        fwrite(&index_off, 8, 1, f);
        const uint64_t magic = 0x6C736D6462766F31ull; // "lsmdbvo1"
        fwrite(&magic, 8, 1, f);
        if (fflush(f) != 0 || fsync(fileno(f)) != 0) { fclose(f); return false; }
        fclose(f);
        if (rename(tmp.c_str(), path.c_str()) != 0) return false;
        // fsync the directory so the rename is durable
        int dfd = ::open(fs::path(path).parent_path().c_str(), O_RDONLY);
        if (dfd >= 0) { ::fsync(dfd); ::close(dfd); }
        return true;
    }

    // Open + load the index into memory.
    bool Open(const std::string &path) {
        path_ = path;
        FILE *f = fopen(path.c_str(), "rb");
        if (!f) return false;
        if (fseek(f, -16, SEEK_END) != 0) { fclose(f); return false; }
        uint64_t index_off, magic;
        if (fread(&index_off, 8, 1, f) != 1 || fread(&magic, 8, 1, f) != 1 ||
            magic != 0x6C736D6462766F31ull) { fclose(f); return false; }
        if (fseek(f, (long)index_off, SEEK_SET) != 0) { fclose(f); return false; }
        uint32_t count;
        if (fread(&count, 4, 1, f) != 1) { fclose(f); return false; }
        index_.reserve(count);
        for (uint32_t i = 0; i < count; i++) {
            uint32_t klen;
            if (fread(&klen, 4, 1, f) != 1) { fclose(f); return false; }
            std::string k(klen, '\0');
            uint64_t o;
            if (fread(k.data(), 1, klen, f) != klen || fread(&o, 8, 1, f) != 1) {
                fclose(f);
                return false;
            }
            index_.emplace_back(std::move(k), o);
        }
        fclose(f);
        return true;
    }

    // Point lookup via binary search on the in-memory index.
    std::optional<Entry> Get(const std::string &key) const {
        auto it = std::lower_bound(index_.begin(), index_.end(), key,
                                   [](const auto &a, const std::string &k) {
                                       return a.first < k;
                                   });
        if (it == index_.end() || it->first != key) return std::nullopt;
        FILE *f = fopen(path_.c_str(), "rb");
        if (!f) return std::nullopt;
        fseek(f, (long)it->second, SEEK_SET);
        uint32_t klen, vlen;
        if (fread(&klen, 4, 1, f) != 1 || fread(&vlen, 4, 1, f) != 1) {
            fclose(f);
            return std::nullopt;
        }
        fseek(f, klen, SEEK_CUR);
        Entry e;
        if (vlen == 0xFFFFFFFFu) {
            e.tombstone = true;
        } else {
            e.value.resize(vlen);
            if (vlen && fread(e.value.data(), 1, vlen, f) != vlen) {
                fclose(f);
                return std::nullopt;
            }
        }
        fclose(f);
        return e;
    }

    // Full scan (for compaction).
    bool Scan(std::map<std::string, Entry> &out) const {
        FILE *f = fopen(path_.c_str(), "rb");
        if (!f) return false;
        for (const auto &[k, off] : index_) {
            fseek(f, (long)off, SEEK_SET);
            uint32_t klen, vlen;
            if (fread(&klen, 4, 1, f) != 1 || fread(&vlen, 4, 1, f) != 1) break;
            std::string key(klen, '\0');
            if (klen && fread(key.data(), 1, klen, f) != klen) break;
            Entry e;
            if (vlen == 0xFFFFFFFFu) {
                e.tombstone = true;
            } else {
                e.value.resize(vlen);
                if (vlen && fread(e.value.data(), 1, vlen, f) != vlen) break;
            }
            out.emplace(std::move(key), std::move(e)); // no overwrite: caller orders newest-first
        }
        fclose(f);
        return true;
    }

    const std::string &path() const { return path_; }

private:
    std::string path_;
    std::vector<std::pair<std::string, uint64_t>> index_;
};

// ---------- DB ----------
struct Options {
    size_t memtable_bytes = 4 << 20;  // flush threshold
    size_t l0_compact_trigger = 4;    // L0 files before compaction into L1
    bool sync_writes = true;          // fsync WAL on every write batch
};

class DB {
public:
    static std::optional<DB> Open(const std::string &dir, Options opts = {}) {
        DB db(dir, opts);
        if (!db.Recover()) return std::nullopt;
        return db;
    }

    // Durable when this returns true (WAL appended + fsynced).
    bool Put(const std::string &key, const std::string &value) {
        return Write(key, value, false);
    }
    bool Delete(const std::string &key) { return Write(key, "", true); }

    std::optional<std::string> Get(const std::string &key) {
        // memtable first
        if (auto it = mem_.find(key); it != mem_.end())
            return it->second.tombstone ? std::nullopt
                                        : std::optional(it->second.value);
        // L0 newest-first, then L1
        for (auto t = l0_.rbegin(); t != l0_.rend(); ++t)
            if (auto e = t->Get(key))
                return e->tombstone ? std::nullopt : std::optional(e->value);
        for (const auto &t : l1_)
            if (auto e = t.Get(key))
                return e->tombstone ? std::nullopt : std::optional(e->value);
        return std::nullopt;
    }

    // Flush memtable to a new L0 SSTable, truncate the WAL, maybe compact.
    bool Flush() {
        if (mem_.empty()) return true;
        const uint64_t id = next_file_id_++;
        const std::string path = TablePath(0, id);
        if (!SSTable::Write(path, mem_)) return false;
        SSTable t;
        if (!t.Open(path)) return false;
        l0_.push_back(std::move(t));
        manifest_.emplace_back(0, id);
        if (!WriteManifest()) return false;
        mem_.clear();
        mem_bytes_ = 0;
        // WAL contents are now durable in the SSTable: start a fresh WAL
        wal_ = std::make_unique<Wal>(WalPath());
        ::truncate(WalPath().c_str(), 0);
        if (l0_.size() >= opts_.l0_compact_trigger) return Compact();
        return true;
    }

    // Merge all of L0 + L1 into a single new L1 run; drop tombstones.
    bool Compact() {
        std::map<std::string, Entry> merged;
        // L1 oldest data first, then L0 in order: later inserts overwrite.
        for (const auto &t : l1_) {
            std::map<std::string, Entry> m;
            if (!t.Scan(m)) return false;
            for (auto &[k, e] : m) merged[k] = std::move(e);
        }
        for (auto &t : l0_) {
            std::map<std::string, Entry> m;
            if (!t.Scan(m)) return false;
            for (auto &[k, e] : m) merged[k] = std::move(e);
        }
        // Drop tombstones: nothing below L1 can resurrect them.
        for (auto it = merged.begin(); it != merged.end();)
            it = it->second.tombstone ? merged.erase(it) : std::next(it);

        const uint64_t id = next_file_id_++;
        const std::string path = TablePath(1, id);
        if (!SSTable::Write(path, merged)) return false;

        // Swap in the new manifest, then delete the old files.
        std::vector<std::string> old_files;
        for (const auto &t : l0_) old_files.push_back(t.path());
        for (const auto &t : l1_) old_files.push_back(t.path());
        l0_.clear();
        l1_.clear();
        SSTable t;
        if (!t.Open(path)) return false;
        l1_.push_back(std::move(t));
        manifest_.clear();
        manifest_.emplace_back(1, id);
        if (!WriteManifest()) return false;
        for (const auto &p : old_files) ::unlink(p.c_str());
        return true;
    }

    size_t l0_count() const { return l0_.size(); }
    size_t l1_count() const { return l1_.size(); }
    size_t mem_count() const { return mem_.size(); }

private:
    DB(const std::string &dir, Options opts) : dir_(dir), opts_(opts) {
        fs::create_directories(dir);
    }

    std::string WalPath() const { return dir_ + "/wal.log"; }
    std::string ManifestPath() const { return dir_ + "/MANIFEST"; }
    std::string TablePath(int level, uint64_t id) const {
        return dir_ + "/L" + std::to_string(level) + "-" + std::to_string(id) + ".sst";
    }

    bool Write(const std::string &key, const std::string &value, bool tomb) {
        if (!wal_->Append(key, value, tomb)) return false;
        if (opts_.sync_writes && !wal_->Sync()) return false;
        mem_bytes_ += key.size() + value.size() + 16;
        mem_[key] = Entry{value, tomb};
        if (mem_bytes_ >= opts_.memtable_bytes) return Flush();
        return true;
    }

    bool WriteManifest() {
        const std::string tmp = ManifestPath() + ".tmp";
        FILE *f = fopen(tmp.c_str(), "w");
        if (!f) return false;
        for (const auto &[level, id] : manifest_)
            fprintf(f, "%d %llu\n", level, (unsigned long long)id);
        if (fflush(f) != 0 || fsync(fileno(f)) != 0) { fclose(f); return false; }
        fclose(f);
        if (rename(tmp.c_str(), ManifestPath().c_str()) != 0) return false;
        int dfd = ::open(dir_.c_str(), O_RDONLY);
        if (dfd >= 0) { ::fsync(dfd); ::close(dfd); }
        return true;
    }

    bool Recover() {
        // 1. load manifest
        FILE *f = fopen(ManifestPath().c_str(), "r");
        if (f) {
            int level;
            unsigned long long id;
            while (fscanf(f, "%d %llu", &level, &id) == 2) {
                manifest_.emplace_back(level, id);
                next_file_id_ = std::max(next_file_id_, (uint64_t)id + 1);
            }
            fclose(f);
        }
        for (const auto &[level, id] : manifest_) {
            SSTable t;
            if (!t.Open(TablePath(level, id))) return false;
            (level == 0 ? l0_ : l1_).push_back(std::move(t));
        }
        // 2. replay WAL tail into the memtable (CRC guards torn records)
        Wal::Replay(WalPath(), [this](const std::string &k, const std::string &v,
                                      bool tomb) {
            mem_bytes_ += k.size() + v.size() + 16;
            mem_[k] = Entry{v, tomb};
        });
        // 3. reopen WAL for appending
        wal_ = std::make_unique<Wal>(WalPath());
        return wal_->ok();
    }

    std::string dir_;
    Options opts_;
    std::unique_ptr<Wal> wal_;
    std::map<std::string, Entry> mem_;
    size_t mem_bytes_ = 0;
    std::vector<SSTable> l0_, l1_;
    std::vector<std::pair<int, uint64_t>> manifest_;
    uint64_t next_file_id_ = 1;
};

}  // namespace lsmdb
