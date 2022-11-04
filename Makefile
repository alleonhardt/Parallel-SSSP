CXXFLAGS = -O3 -mcx16 -march=native -std=c++17 -Wall -Wextra -fopencilk -DCILK

all: sssp
.PHONY: gen run clean

sssp:	sssp.cc sssp.h dijkstra.hpp graph.hpp
	$(CC) $(CXXFLAGS) sssp.cc -o sssp

clean:
	rm sssp

