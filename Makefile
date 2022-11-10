CXXFLAGS = -O3 -mcx16 -march=native -std=c++17 -Wall -Wextra -fopencilk -DCILK -lsqlite3 -I/usr/include/postgresql -lpq -lssl -lcrypto

all: sssp
.PHONY: gen run clean

sssp:	src/sssp.cc src/sssp.h src/dijkstra.hpp src/graph.hpp src/metrics/metrics.hpp src/metrics/metrics.cpp src/backend/backend_interface.hpp src/backend/sqlite3_backend.cpp src/backend/sqlite3_backend.hpp src/backend/postgres_backend.cpp src/backend/postgres_backend.hpp
	$(CC) $(CXXFLAGS) src/sssp.cc src/metrics/metrics.cpp src/backend/sqlite3_backend.cpp src/backend/postgres_backend.cpp -o sssp

clean:
	rm sssp

