# lsmdb build/test
CXX := clang++
CXXFLAGS := -std=c++20 -Wall -Wextra -O2 -Isrc

.PHONY: all test bench clean

all: build/test_lsmdb build/crash_test build/bench

build:
	mkdir -p build results

build/test_lsmdb: tests/test_lsmdb.cc src/lsmdb.h | build
	$(CXX) $(CXXFLAGS) -o $@ tests/test_lsmdb.cc

build/crash_test: tests/crash_test.cc src/lsmdb.h | build
	$(CXX) $(CXXFLAGS) -o $@ tests/crash_test.cc

build/bench: bench/bench.cc src/lsmdb.h | build
	$(CXX) $(CXXFLAGS) -o $@ bench/bench.cc

test: build/test_lsmdb build/crash_test
	./build/test_lsmdb
	./build/crash_test 30 42 | tee results/crash.log

bench: build/bench
	./build/bench > results/bench.json
	@cat results/bench.json

clean:
	rm -rf build
