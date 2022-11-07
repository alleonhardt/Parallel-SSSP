#include "metrics.hpp"
#include <iostream>

int read_graph(void *, int argc, char **argv, char **azColName)
{
    /*SSSPMetrics *mets = (SSSPMetrics*)data;
    mets->_sqliteGraphId = argv[0]? std::stoi(argv[0]): -1;
   return 0;*/
    int i;
    for (i = 0; i < argc; i++)
    {
        printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
}

int dummy_reader(void *, int, char **, char **)
{
    return 0;
}

SSSPMetrics::SSSPMetrics(std::string filename, std::string graphAdj, std::string algorithm, int parameter) : _dump_filename(filename), _algorithm(algorithm), _algorithmParameter(parameter)
{
    // At least one round is needed
    _insertions_by_id.push_back({});

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
    const char *create_graph_table_sql = "CREATE TABLE IF NOT EXISTS Graph (id INTEGER PRIMARY KEY AUTOINCREMENT, adj_description TEXT, graph_generator_config_json TEXT, graph_generator_name TEXT,num_nodes INTEGER, num_edges INTEGER);";
    const char *create_sssp_source_sql = "CREATE TABLE IF NOT EXISTS SSSPExecution (id INTEGER PRIMARY KEY AUTOINCREMENT, graph_id INTEGER, algorithm TEXT, algorithmParameter INTEGER, source_node INTEGER, FOREIGN KEY(graph_id) REFERENCES Graph(id));";
    const char *create_sssp_source_step_sql = "CREATE TABLE IF NOT EXISTS SSSPExecutionStep (sssp_source_id INTEGER, total_vertices INTEGER, step INTEGER, FOREIGN KEY(sssp_source_id) REFERENCES SSSPExecution(id));";

    char *errorMsg = 0;
    result = sqlite3_exec(_database, create_graph_table_sql, dummy_reader, 0, &errorMsg);
    if (result != SQLITE_OK)
    {
        std::cerr << "SQL error: " << errorMsg << std::endl;
        sqlite3_free(errorMsg);
        std::exit(-1);
    }

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

    std::string findGraph = "SELECT id FROM Graph WHERE adj_description=?;";
    sqlite3_stmt *find_graph_stmt = nullptr;
    result = sqlite3_prepare_v2(
        _database,          // the handle to your (opened and ready) database
        findGraph.c_str(),  // the sql statement, utf-8 encoded
        findGraph.length(), // max length of sql statement
        &find_graph_stmt,   // this is an "out" parameter, the compiled statement goes here
        nullptr);           // pointer to the tail end of sql statement (when there are
    result = sqlite3_bind_text(
        find_graph_stmt,   // previously compiled prepared statement object
        1,                 // parameter index, 1-based
        graphAdj.c_str(),  // the data
        graphAdj.length(), // length of data
        SQLITE_STATIC);    // this parameter is a little tricky - it's a pointer to the callback

    auto stepResult = sqlite3_step(find_graph_stmt);
    if (stepResult == SQLITE_ROW)
    {
        _sqliteGraphId = sqlite3_column_int(find_graph_stmt, 1);
    }
    else
    {
        // Graph does not exist yet, therefore insert it
        if (_sqliteGraphId == -1)
        {
            // TODO: Change this to prepared statement
            std::string insertGraph = "INSERT INTO Graph (adj_description) VALUES (\"" + graphAdj + "\");";
            result = sqlite3_exec(_database, insertGraph.c_str(), read_graph, this, &errorMsg);
            if (result != SQLITE_OK)
            {
                std::cerr << "SQL error: " << errorMsg << std::endl;
                sqlite3_free(errorMsg);
                std::exit(-1);
            }
            _sqliteGraphId = sqlite3_last_insert_rowid(_database);
        }
    }
    sqlite3_finalize(find_graph_stmt);
}

void SSSPMetrics::incAlgorithmStep()
{
    _currentStep += 1;
    _insertions_by_id.push_back({});
}

SSSPMetrics::~SSSPMetrics()
{
    // Close the open connection to the sqlite database
    if (_database)
    {
        sqlite3_close(_database);
        _database = nullptr;
    }
}

int SSSPMetrics::getCurrentTotalCount()
{
    auto lck = std::unique_lock<std::mutex>(_access_guard);
    return _insertions_by_id.back().size();
}

void SSSPMetrics::log_node_add(unsigned long long nodeId)
{
    auto lck = std::unique_lock<std::mutex>(_access_guard);
    _insertions_by_id.back().insert(nodeId);
}

void SSSPMetrics::reset_round(unsigned long long sourceNode)
{
    dump(sourceNode);
    reset_round();
}

void SSSPMetrics::reset_round()
{
    auto lck = std::unique_lock<std::mutex>(_access_guard);
    _insertions_by_id.clear();
    _insertions_by_id.push_back({});
    _currentStep = 0;
}

void SSSPMetrics::dump(unsigned long long sourceNode)
{
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
    std::string insertSSSP = "INSERT INTO SSSPExecution (graph_id,source_node, algorithm, algorithmParameter) VALUES (" + std::to_string(_sqliteGraphId) + "," + std::to_string(sourceNode) + ",\"" + _algorithm + "\"," + std::to_string(_algorithmParameter) + ");";

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
    for (auto &map : _insertions_by_id)
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
