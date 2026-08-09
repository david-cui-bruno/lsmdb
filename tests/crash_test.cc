// Crash-durability harness: the writer child applies a DETERMINISTIC op
// stream and acks each op over a pipe after it is durable; the parent
// kill -9s it at a random moment, reopens the DB, and verifies the final
// state matches ops 0..S (S = last acked op), allowing AT MOST the single
// in-flight op S+1 to also be present (child dies between apply and ack).
//
// An acked write missing or reverted = durability violation. This kills
// mid-anything, including mid-flush and mid-compaction (tiny memtable).
//
// Usage: crash_test <rounds> [seed]
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <map>
#include <random>
#include <signal.h>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

#include "lsmdb.h"

namespace fs = std::filesystem;
using lsmdb::DB;
using lsmdb::Options;

struct Op {
    bool del;
    std::string key, value;
};

// Deterministic op for a given seed and sequence number. Child and parent
// generate the identical stream.
static Op OpFor(uint64_t seed, uint64_t seq) {
    std::mt19937_64 rng(seed * 1000003 + seq);
    Op op;
    op.del = rng() % 20 == 0;
    op.key = (rng() % 3 == 0) ? "hot" + std::to_string(rng() % 50)
                              : "key" + std::to_string(seq);
    if (!op.del) {
        op.value = "v" + std::to_string(seq) + ":";
        size_t len = 10 + (seq * 2654435761u) % 500;
        op.value.resize(op.value.size() + len, (char)('a' + seq % 26));
    }
    return op;
}

// Child: apply ops in order, ack each durable op by seq.
[[noreturn]] static void Writer(const std::string &dir, int pipe_fd, uint64_t seed) {
    Options o;
    o.memtable_bytes = 16 << 10; // tiny: force frequent flush + compaction
    o.l0_compact_trigger = 3;
    auto db = DB::Open(dir, o);
    if (!db) _exit(2);
    char line[32];
    for (uint64_t seq = 0;; seq++) {
        const Op op = OpFor(seed, seq);
        const bool ok = op.del ? db->Delete(op.key) : db->Put(op.key, op.value);
        if (!ok) _exit(3);
        int n = snprintf(line, sizeof line, "%llu\n", (unsigned long long)seq);
        if (write(pipe_fd, line, n) != n) _exit(4);
    }
}

int main(int argc, char **argv) {
    const int rounds = argc > 1 ? atoi(argv[1]) : 20;
    const uint64_t seed0 = argc > 2 ? strtoull(argv[2], nullptr, 10) : 42;
    int failures = 0;
    uint64_t total_acked = 0;

    for (int round = 0; round < rounds; round++) {
        const uint64_t seed = seed0 + round;
        const std::string dir = "build/crash-" + std::to_string(round);
        fs::remove_all(dir);

        int pfd[2];
        if (pipe(pfd) != 0) return 2;
        pid_t pid = fork();
        if (pid == 0) {
            close(pfd[0]);
            Writer(dir, pfd[1], seed);
        }
        close(pfd[1]);

        std::mt19937_64 rng(seed * 7 + 1);
        const int run_ms = 30 + (int)(rng() % 400);
        usleep(run_ms * 1000);
        kill(pid, SIGKILL);
        waitpid(pid, nullptr, 0);

        // Highest acked seq (pipe lines are in order; last may be torn).
        FILE *f = fdopen(pfd[0], "r");
        long long last_acked = -1;
        unsigned long long seq;
        while (fscanf(f, "%llu", &seq) == 1) last_acked = (long long)seq;
        fclose(f);
        total_acked += (uint64_t)(last_acked + 1);

        // World A: ops 0..last_acked. World B: A + op last_acked+1.
        std::map<std::string, std::optional<std::string>> world;
        for (long long s = 0; s <= last_acked; s++) {
            const Op op = OpFor(seed, (uint64_t)s);
            world[op.key] = op.del ? std::nullopt : std::optional(op.value);
        }
        const Op inflight = OpFor(seed, (uint64_t)(last_acked + 1));

        auto db = DB::Open(dir);
        if (!db) {
            fprintf(stderr, "round %d: RECOVERY FAILED (acked=%lld)\n", round, last_acked + 1);
            failures++;
            continue;
        }
        uint64_t bad = 0;
        for (const auto &[k, want] : world) {
            auto got = db->Get(k);
            bool ok = (want.has_value() == got.has_value()) &&
                      (!want || *want == *got);
            if (!ok && k == inflight.key) {
                // the in-flight op may have landed
                if (inflight.del) ok = !got.has_value();
                else ok = got.has_value() && *got == inflight.value;
            }
            if (!ok) {
                if (bad < 3)
                    fprintf(stderr, "round %d: key %s %s\n", round, k.c_str(),
                            got ? "has wrong value" : "missing (acked write lost)");
                bad++;
            }
        }
        // the in-flight op may create a brand-new key not in world A
        if (!world.count(inflight.key)) {
            auto got = db->Get(inflight.key);
            if (got.has_value() && (inflight.del || *got != inflight.value)) {
                fprintf(stderr, "round %d: unexpected value on in-flight key %s\n",
                        round, inflight.key.c_str());
                bad++;
            }
        }
        if (bad) {
            fprintf(stderr, "round %d: %llu violations across %zu keys\n",
                    round, (unsigned long long)bad, world.size());
            failures++;
        } else {
            printf("round %2d: killed after %3d ms, %6lld acked ops, %5zu keys verified OK\n",
                   round, run_ms, last_acked + 1, world.size());
        }
        fs::remove_all(dir);
    }

    printf("\n%d/%d rounds clean, %llu acked ops total\n",
           rounds - failures, rounds, (unsigned long long)total_acked);
    return failures ? 1 : 0;
}
