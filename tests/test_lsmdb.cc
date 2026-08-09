// Unit tests for lsmdb: roundtrip, flush, compaction, reopen, tombstones,
// torn WAL tail, overwrite semantics across levels.
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <string>

#include "lsmdb.h"

namespace fs = std::filesystem;
using lsmdb::DB;
using lsmdb::Options;

static int g_tests = 0;
#define TEST(name) \
    void name();   \
    struct name##_reg { name##_reg() { g_tests++; printf("== %s\n", #name); name(); } } name##_inst; \
    void name()

static std::string TmpDir(const char *name) {
    std::string d = std::string("build/test-") + name;
    fs::remove_all(d);
    return d;
}

TEST(Roundtrip) {
    auto db = DB::Open(TmpDir("roundtrip"));
    assert(db);
    assert(db->Put("a", "1"));
    assert(db->Put("b", "2"));
    assert(db->Get("a") == "1");
    assert(db->Get("b") == "2");
    assert(!db->Get("c").has_value());
    assert(db->Put("a", "1v2"));  // overwrite
    assert(db->Get("a") == "1v2");
}

TEST(DeleteAndTombstone) {
    auto db = DB::Open(TmpDir("del"));
    assert(db->Put("k", "v"));
    assert(db->Delete("k"));
    assert(!db->Get("k").has_value());
    // delete of a nonexistent key is fine
    assert(db->Delete("never"));
    assert(!db->Get("never").has_value());
}

TEST(FlushAndReadFromSST) {
    auto db = DB::Open(TmpDir("flush"));
    for (int i = 0; i < 100; i++)
        assert(db->Put("key" + std::to_string(i), "val" + std::to_string(i)));
    assert(db->Flush());
    assert(db->mem_count() == 0);
    assert(db->l0_count() == 1);
    for (int i = 0; i < 100; i++)
        assert(db->Get("key" + std::to_string(i)) == "val" + std::to_string(i));
}

TEST(ReopenRecoversFromWal) {
    std::string dir = TmpDir("reopen-wal");
    {
        auto db = DB::Open(dir);
        assert(db->Put("persist", "me"));
        assert(db->Put("and", "me too"));
        // no flush: data only in WAL + memtable
    }
    auto db = DB::Open(dir);
    assert(db);
    assert(db->Get("persist") == "me");
    assert(db->Get("and") == "me too");
}

TEST(ReopenRecoversFromSSTablesAndWal) {
    std::string dir = TmpDir("reopen-mixed");
    {
        auto db = DB::Open(dir);
        assert(db->Put("flushed", "1"));
        assert(db->Flush());
        assert(db->Put("unflushed", "2"));
    }
    auto db = DB::Open(dir);
    assert(db->Get("flushed") == "1");
    assert(db->Get("unflushed") == "2");
}

TEST(NewestLevelWins) {
    std::string dir = TmpDir("levels");
    auto db = DB::Open(dir);
    assert(db->Put("k", "old"));
    assert(db->Flush());          // k=old in L0 file 1
    assert(db->Put("k", "newer"));
    assert(db->Flush());          // k=newer in L0 file 2
    assert(db->Put("k", "newest")); // in memtable
    assert(db->Get("k") == "newest");
    // and after dropping the memtable version via reopen-with-flush
    assert(db->Flush());
    assert(db->Get("k") == "newest");
}

TEST(CompactionMergesAndDropsTombstones) {
    std::string dir = TmpDir("compact");
    Options o;
    o.l0_compact_trigger = 100; // manual compaction only
    auto db = DB::Open(dir, o);
    assert(db->Put("stay", "1"));
    assert(db->Put("die", "2"));
    assert(db->Flush());
    assert(db->Delete("die"));
    assert(db->Flush());
    assert(db->l0_count() == 2);
    assert(db->Compact());
    assert(db->l0_count() == 0);
    assert(db->l1_count() == 1);
    assert(db->Get("stay") == "1");
    assert(!db->Get("die").has_value());
    // reopen after compaction
    auto db2 = DB::Open(dir, o);
    assert(db2->Get("stay") == "1");
    assert(!db2->Get("die").has_value());
}

TEST(AutoCompactionTriggers) {
    std::string dir = TmpDir("autocompact");
    Options o;
    o.l0_compact_trigger = 3;
    auto db = DB::Open(dir, o);
    for (int f = 0; f < 3; f++) {
        assert(db->Put("f" + std::to_string(f), "v"));
        assert(db->Flush());
    }
    assert(db->l0_count() == 0); // compacted on the 3rd flush
    assert(db->l1_count() == 1);
    for (int f = 0; f < 3; f++)
        assert(db->Get("f" + std::to_string(f)) == "v");
}

TEST(TornWalTailDiscarded) {
    std::string dir = TmpDir("torn");
    {
        auto db = DB::Open(dir);
        assert(db->Put("good", "1"));
        assert(db->Put("alsogood", "2"));
    }
    // Corrupt the tail: append garbage that looks like a partial record.
    FILE *f = fopen((dir + "/wal.log").c_str(), "ab");
    const char garbage[] = "\x11\x22\x33\x44partial-record-torn-midw";
    fwrite(garbage, 1, sizeof garbage - 1, f);
    fclose(f);

    auto db = DB::Open(dir);
    assert(db); // recovery must not fail
    assert(db->Get("good") == "1");
    assert(db->Get("alsogood") == "2");
    // and the db is still writable after truncating the junk
    assert(db->Put("after", "3"));
    assert(db->Get("after") == "3");
}

TEST(CorruptMiddleRecordStopsReplayAtCorruption) {
    std::string dir = TmpDir("corrupt-mid");
    {
        auto db = DB::Open(dir);
        assert(db->Put("first", "1"));
        assert(db->Put("second", "2"));
    }
    // Flip one byte in the middle of the file (inside record 1's value).
    FILE *f = fopen((dir + "/wal.log").c_str(), "r+b");
    fseek(f, 14, SEEK_SET); // inside the first record's payload
    int c = fgetc(f);
    fseek(f, 14, SEEK_SET);
    fputc(c ^ 0xFF, f);
    fclose(f);

    auto db = DB::Open(dir);
    // Record 1 is corrupt: CRC fails, replay stops. NOTHING after it applies
    // (conservative: suffix after corruption is untrusted).
    assert(!db->Get("first").has_value());
    assert(!db->Get("second").has_value());
}

TEST(LargeValuesAndBinaryKeys) {
    auto db = DB::Open(TmpDir("binary"));
    std::string key("\x00\x01\xFFkey", 6);
    std::string big(1 << 20, 'x'); // 1 MB value
    big[12345] = '\x07';
    assert(db->Put(key, big));
    assert(db->Get(key) == big);
    assert(db->Flush());
    assert(db->Get(key) == big);
}

int main() {
    printf("%d tests passed\n", g_tests);
    return 0;
}
