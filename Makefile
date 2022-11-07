CXXFLAGS = -O3 -mcx16 -march=native -std=c++17 -Wall -Wextra -fopencilk -DCILK -lsqlite3

all: sssp
.PHONY: gen run clean

sssp:	src/sssp.cc src/sssp.h src/dijkstra.hpp src/graph.hpp src/metrics/metrics.hpp src/metrics/metrics.cpp
	$(CC) $(CXXFLAGS) src/sssp.cc src/metrics/metrics.cpp -o sssp

clean:
	rm sssp

