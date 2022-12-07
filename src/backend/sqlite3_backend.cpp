#include "sqlite3_backend.hpp"
#include <iostream>
#include <cilk/cilk.h>
#include <cilk/cilk_api.h>
#include <openssl/sha.h>
#include "../metrics/stats.hpp"


int dummy_reader(void *, int, char **, char **)
{
    return 0;
}

Sqlite3Backend::Sqlite3Backend(std::string filename, std::string graphAdj, std::string algorithm, int parameter) : _algorithm(algorithm), _algorithmParameter(parameter) {
     _sqliteGraphId = -1;

    // Initialize the database
    _database = nullptr;
    auto result = sqlite3_open(filename.c_str(), &_database);
    if (result)
    {
        std::cerr << "Could not open database " << filename << " for writing the metrics" << std::endl;
        std::exit(-1);
    }

    printf("Metrics database %s is opened\n", filename.c_str());
    const char *create_sssp_source_sql = "CREATE TABLE IF NOT EXISTS SSSPExecution ("
                                            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                                            "graph_id INTEGER,"
                                            "algorithm TEXT,"
                                            "algorithmParameter INTEGER,"
                                            "source_node INTEGER,"
                                            "processors INTEGER,"
                                            "reinserts_total INTEGER,"
                                            "reinserts_mean REAL,"
                                            "reinserts_median INTEGER,"
                                            "reinserts_quartile1 INTEGER,"
                                            "reinserts_quartile3 INTEGER,"
                                            "edge_relaxations_total INTEGER,"
                                            "edge_relaxations_mean REAL,"
                                            "edge_relaxations_median INTEGER,"
                                            "edge_relaxations_quartile1 INTEGER,"
                                            "edge_relaxations_quartile3 INTEGER,"
                                            "FOREIGN KEY(graph_id) REFERENCES Graph(id));";
    const char *create_sssp_source_step_sql = "CREATE TABLE IF NOT EXISTS SSSPExecutionStep ("
                                                "sssp_source_id INTEGER,"
                                                "total_vertices INTEGER,"
                                                "step INTEGER,"
                                                "FOREIGN KEY(sssp_source_id) REFERENCES SSSPExecution(id));";

    char *errorMsg = 0;

    result = sqlite3_exec(_database, create_sssp_source_sql, dummy_reader, 0, &errorMsg);
    if (result != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errorMsg << std::endl;
        sqlite3_free(errorMsg);
    }

    result = sqlite3_exec(_database, create_sssp_source_step_sql, dummy_reader, 0, &errorMsg);
    if (result != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errorMsg << std::endl;
        sqlite3_free(errorMsg);
    }

    unsigned char hash[SHA_DIGEST_LENGTH];

    SHA1((const unsigned char*)graphAdj.c_str(), graphAdj.length(), hash);

    char buf[SHA_DIGEST_LENGTH*2+1];
    for (int i=0; i<SHA_DIGEST_LENGTH; i++) {
        sprintf(buf+i*2, "%02x", hash[i]);
    }
    buf[SHA_DIGEST_LENGTH*2]=0;


    std::string statement = "SELECT id FROM Graph WHERE hash_digest=?;";
    sqlite3_stmt *find_graph_stmt = nullptr;
    result = sqlite3_prepare_v2(
        _database,          // the handle to your (opened and ready) database
        statement.c_str(),  // the sql statement, utf-8 encoded
        statement.length(), // max length of sql statement
        &find_graph_stmt,   // this is an "out" parameter, the compiled statement goes here
        nullptr);           // pointer to the tail end of sql statement (when there are
    result = sqlite3_bind_text(
        find_graph_stmt,   // previously compiled prepared statement object
        1,                 // parameter index, 1-based
        buf,  // the data
        SHA_DIGEST_LENGTH*2, // length of data
        SQLITE_STATIC);    // this parameter is a little tricky - it's a pointer to the callback

    auto stepResult = sqlite3_step(find_graph_stmt);
    if (stepResult == SQLITE_ROW)
    {
        _sqliteGraphId = sqlite3_column_int(find_graph_stmt, 0);
    }
    else
    {
        std::cerr<<"Could not find graph!"<<std::endl;
        std::exit(-1);
    }
    sqlite3_finalize(find_graph_stmt);
}

void Sqlite3Backend::dump(SSSPMetrics *metrics, unsigned long long sourceNode) {
    auto lck = std::unique_lock<std::mutex>(_access_guard);
    sqlite3_stmt *insert_point = nullptr;
    char *errorMsg = nullptr;
    auto result = sqlite3_exec(_database, "BEGIN TRANSACTION;", nullptr, nullptr, &errorMsg);
    if (result != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errorMsg << std::endl;
        sqlite3_free(errorMsg);
        std::exit(-1);
    }
    std::string insertPoint = "INSERT INTO SSSPExecutionStep (sssp_source_id,total_vertices,step) VALUES (?,?,?)";
    
    int step = 0;
    std::vector<unsigned long> inserts_vec;
    for(auto &entry : metrics->getInsertionsPerNode()) {
        inserts_vec.push_back(entry.second);
    }

    VectorStats<unsigned long> stats_insert(&inserts_vec, 0);


    std::vector<unsigned long> relaxations_vec;

    for(auto &entry : metrics->getEdgeRelaxationsPerNode()) {
        relaxations_vec.push_back(entry.second);
    }

    VectorStats<unsigned long> stats_relaxations(&relaxations_vec, 0);


    std::string insertSSSP = "INSERT INTO SSSPExecution (graph_id,source_node, algorithm, algorithmParameter,processors,"
                                "reinserts_total,"
                                "reinserts_mean,"
                                "reinserts_median,"
                                "reinserts_quartile1,"
                                "reinserts_quartile3,"
                                "edge_relaxations_total,"
                                "edge_relaxations_mean,"
                                "edge_relaxations_median,"
                                "edge_relaxations_quartile1,"
                                "edge_relaxations_quartile3"
                                ") VALUES (" + std::to_string(_sqliteGraphId) + "," + std::to_string(sourceNode) + ",\"" + _algorithm + "\"," + std::to_string(_algorithmParameter) + ","+std::to_string(__cilkrts_get_nworkers())+ ","
                                    +std::to_string(stats_insert.sum())
                                    +","
                                    +std::to_string(stats_insert.mean())
                                    +","
                                    +std::to_string(stats_insert.median())
                                    +","
                                    +std::to_string(stats_insert.first_quartile())
                                    +","
                                    +std::to_string(stats_insert.third_quartile())
                                    +","
                                    +std::to_string(stats_relaxations.sum())
                                    +","
                                    +std::to_string(stats_relaxations.mean())
                                    +","
                                    +std::to_string(stats_relaxations.median())
                                    +","
                                    +std::to_string(stats_relaxations.first_quartile())
                                    +","
                                    +std::to_string(stats_relaxations.third_quartile())
                                    +");";

    long long id = -1;
    result = sqlite3_exec(_database, insertSSSP.c_str(), dummy_reader, &id, &errorMsg);

    if (result != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errorMsg << std::endl;
        sqlite3_free(errorMsg);
        std::exit(-1);
    }
    id = sqlite3_last_insert_rowid(_database);
    result = sqlite3_prepare_v2(
        _database,            // the handle to your (opened and ready) database
        insertPoint.c_str(),  // the sql statement, utf-8 encoded
        insertPoint.length(), // max length of sql statement
        &insert_point,        // this is an "out" parameter, the compiled statement goes here
        nullptr);             // pointer to the tail end of sql statement (when there are
    result = sqlite3_bind_int(
        insert_point, // previously compiled prepared statement object
        1,            // parameter index, 1-based
        id);
    for (auto &map : metrics->getRoundMetrics())
    {
        result = sqlite3_bind_int(
            insert_point, // previously compiled prepared statement object
            2,            // parameter index, 1-based
            map.size());
        result = sqlite3_bind_int(
            insert_point, // previously compiled prepared statement object
            3,            // parameter index, 1-based
            step);
        sqlite3_step(insert_point);
        sqlite3_reset(insert_point);
        step++;
    }
    sqlite3_finalize(insert_point);
    result = sqlite3_exec(_database, "END TRANSACTION;", nullptr, nullptr, &errorMsg);
    if (result != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errorMsg << std::endl;
        sqlite3_free(errorMsg);
        std::exit(-1);
    }
}

Sqlite3Backend::~Sqlite3Backend() {
    // Close the open connection to the sqlite database
    if (_database)
    {
        sqlite3_close(_database);
        _database = nullptr;
    }
}