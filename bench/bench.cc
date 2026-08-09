// Benchmark: write throughput (synced vs unsynced), read latency from
// memtable/L0/L1, recovery time. JSON to stdout, progress to stderr.
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <random>
#include <string>
#include <vector>

#include "lsmdb.h"

namespace fs = std::filesystem;
using lsmdb::DB;
using lsmdb::Options;
using Clock = std::chrono::steady_clock;

static double Secs(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double>(b - a).count();
}

int main() {
    const int N = 100000;
    const size_t VAL = 100;
    std::string value(VAL, 'x');

    printf("{\n");

    // --- synced writes (the durability path) ---
    {
        fs::remove_all("build/bench-sync");
        auto db = DB::Open("build/bench-sync");
        auto t0 = Clock::now();
        const int NS = 5000; // fsync per write is slow; keep it honest but short
        for (int i = 0; i < NS; i++)
            db->Put("key" + std::to_string(i), value);
        auto t1 = Clock::now();
        double s = Secs(t0, t1);
        printf("  \"put_synced\": {\"n\": %d, \"seconds\": %.3f, \"ops_per_sec\": %.0f},\n",
               NS, s, NS / s);
        fprintf(stderr, "synced puts: %.0f ops/s\n", NS / s);
    }

    // --- unsynced writes (WAL appended, fsync off: group-commit ceiling) ---
    {
        fs::remove_all("build/bench-nosync");
        Options o;
        o.sync_writes = false;
        auto db = DB::Open("build/bench-nosync", o);
        auto t0 = Clock::now();
        for (int i = 0; i < N; i++)
            db->Put("key" + std::to_string(i), value);
        auto t1 = Clock::now();
        double s = Secs(t0, t1);
        printf("  \"put_unsynced\": {\"n\": %d, \"seconds\": %.3f, \"ops_per_sec\": %.0f},\n",
               N, s, N / s);
        fprintf(stderr, "unsynced puts: %.0f ops/s\n", N / s);
    }

    // --- reads across storage tiers ---
    {
        fs::remove_all("build/bench-read");
        Options o;
        o.sync_writes = false;
        o.l0_compact_trigger = 1000; // manual control
        auto db = DB::Open("build/bench-read", o);
        // L1 tier: write, flush, compact
        for (int i = 0; i < N; i++)
            db->Put("l1-" + std::to_string(i), value);
        db->Flush();
        db->Compact();
        // L0 tier
        for (int i = 0; i < N; i++)
            db->Put("l0-" + std::to_string(i), value);
        db->Flush();
        // memtable tier
        for (int i = 0; i < 10000; i++)
            db->Put("mem-" + std::to_string(i), value);

        std::mt19937 rng(7);
        auto benchReads = [&](const char *tier, const char *prefix, int keys) {
            const int R = 20000;
            auto t0 = Clock::now();
            size_t hits = 0;
            for (int i = 0; i < R; i++) {
                auto v = db->Get(prefix + std::to_string(rng() % keys));
                hits += v.has_value();
            }
            auto t1 = Clock::now();
            double s = Secs(t0, t1);
            printf("  \"get_%s\": {\"n\": %d, \"hits\": %zu, \"ops_per_sec\": %.0f, \"avg_us\": %.2f},\n",
                   tier, R, hits, R / s, s / R * 1e6);
            fprintf(stderr, "get %s: %.0f ops/s (%.2f us avg)\n", tier, R / s, s / R * 1e6);
        };
        benchReads("memtable", "mem-", 10000);
        benchReads("l0", "l0-", N);
        benchReads("l1", "l1-", N);
    }

    // --- recovery time: reopen a db with a fat WAL ---
    {
        fs::remove_all("build/bench-recover");
        {
            Options o;
            o.sync_writes = false;
            o.memtable_bytes = 1 << 30; // never flush: everything in the WAL
            auto db = DB::Open("build/bench-recover", o);
            for (int i = 0; i < N; i++)
                db->Put("key" + std::to_string(i), value);
        }
        auto t0 = Clock::now();
        auto db = DB::Open("build/bench-recover");
        auto t1 = Clock::now();
        double s = Secs(t0, t1);
        printf("  \"recovery\": {\"wal_records\": %d, \"seconds\": %.3f, \"records_per_sec\": %.0f}\n",
               N, s, N / s);
        fprintf(stderr, "recovery: %d WAL records in %.3f s\n", N, s);
    }

    printf("}\n");
    return 0;
}
