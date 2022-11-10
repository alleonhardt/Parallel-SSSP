#ifndef __SQLITE3_BACKEND_HPP__
#define __SQLITE3_BACKEND_HPP__

#include "backend_interface.hpp"
#include "sqlite3.h"
#include <mutex>

class Sqlite3Backend : public BackendInterface {
    public:
        void dump(SSSPMetrics *metrics, unsigned long long sourceNode) final;

        Sqlite3Backend(std::string filename, std::string graphAdj, std::string algorithm, int parameter);
        ~Sqlite3Backend();

    private:
        // Protect functions which mutate the state from race conditions
        std::mutex _access_guard;

        // The database to store the records
        sqlite3 *_database;

        // The graphId in the sqlite database
        int _sqliteGraphId;

        std::string _algorithm;
        int _algorithmParameter;
};

#endif