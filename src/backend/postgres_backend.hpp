#ifndef __POSTGRES_BACKEND_HPP__
#define __POSTGRES_BACKEND_HPP__

#include "backend_interface.hpp"
#include <string>
#include <libpq-fe.h>

class PostgresBackend : public BackendInterface {
    public:
        void dump(SSSPMetrics *metrics, unsigned long long sourceNode) final;

        PostgresBackend(std::string user, std::string password, std::string host, int port, std::string datbase, std::string graphAdj, std::string algorithm, int parameter);
        ~PostgresBackend();

        void exit_err(const char *step, PGresult *res);

    private:
        // Protect functions which mutate the state from race conditions
        std::mutex _access_guard;

        // The graphId in the sqlite database
        std::string _postgresGraphId;


        // Postgres connection
        PGconn *_database;

        std::string _algorithm;
        int _algorithmParameter;
};

#endif