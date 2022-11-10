#include "postgres_backend.hpp"

void PostgresBackend::dump(SSSPMetrics *metrics, unsigned long long sourceNode) {
    auto lck = std::unique_lock<std::mutex>(_access_guard);

    std::string insertSSSP = std::string("INSERT INTO SSSPExecution (graph_id,source_node, algorithm, algorithmParameter) VALUES (") + _postgresGraphId + "," + std::to_string(sourceNode) + ",'" + _algorithm + "'," + std::to_string(_algorithmParameter) + ") RETURNING id;";

    PGresult *res = PQexec(_database, insertSSSP.c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        exit_err("Could not insert data into SSSPExecution",res);
    }

    int tupleCount = PQntuples(res);
    std::string id;
    if(tupleCount==1) {
        id = PQgetvalue(res, 0, 0);
    }
    else {
        exit_err("Could not retrieve id after inserting data into SSSPExecution",res);
    }

    const char* insertPoint = "INSERT INTO SSSPExecutionStep (sssp_source_id,total_vertices,step) VALUES ($1,$2,$3)";
    int step = 0;



    for (auto &map : metrics->getRoundMetrics())
    {
        std::string metrics_size = std::to_string(map.size());
        std::string step_str = std::to_string(step);
        const char *paramValues[3];
        paramValues[0] = id.c_str();
        paramValues[1] = metrics_size.c_str();
        paramValues[2] = step_str.c_str();

        res = PQexecParams(_database, insertPoint, 3, NULL, paramValues, 
            NULL, NULL, 0);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            exit_err("Could not insert data into SSSPExecutionStep",res);
        }
        else {
            PQclear(res);
        }
        step+=1;
    }
}

void PostgresBackend::exit_err(const char *step, PGresult *result) {
    if(_database) {
        fprintf(stderr, "%s. Error: %s\n", step, PQerrorMessage(_database));    

        if(result) PQclear(result);
        PQfinish(_database);
        _database = nullptr;
    }
    std::exit(-1);
}

PostgresBackend::PostgresBackend(std::string user, std::string password, std::string host, int port, std::string datbase, std::string graphAdj, std::string algorithm, int parameter) : _algorithm(algorithm), _algorithmParameter(parameter) {
    int lib_ver = PQlibVersion();

    printf("Version of postgres library: %d\n", lib_ver);
    _database = nullptr;

    std::string login = "user="+user+" password="+password+" host="+host+" port="+std::to_string(port)+" dbname="+datbase;
    _database = PQconnectdb(login.c_str());
    if (PQstatus(_database) == CONNECTION_BAD) {
        exit_err("Connection to database failed",nullptr);
    }
    const char *create_graph_table_sql = "CREATE TABLE IF NOT EXISTS Graph (id SERIAL PRIMARY KEY, adj_description TEXT, graph_generator_config_json TEXT, graph_generator_name TEXT, experiment_name TEXT, num_nodes INTEGER, num_edges INTEGER, FOREIGN KEY(experiment_name) REFERENCES Experiment(name));";
    const char *create_sssp_source_sql = "CREATE TABLE IF NOT EXISTS SSSPExecution (id SERIAL PRIMARY KEY, graph_id INTEGER, algorithm TEXT, algorithmParameter INTEGER, source_node INTEGER, FOREIGN KEY(graph_id) REFERENCES Graph(id));";
    const char *create_sssp_source_step_sql = "CREATE TABLE IF NOT EXISTS SSSPExecutionStep (sssp_source_id INTEGER, total_vertices INTEGER, step INTEGER, FOREIGN KEY(sssp_source_id) REFERENCES SSSPExecution(id));";


    PGresult *res = PQexec(_database, create_graph_table_sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        exit_err("Connection to database failed",res);
    }
    PQclear(res);

    res = PQexec(_database, create_sssp_source_sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        exit_err("Connection to database failed",res);
    }
    PQclear(res);

    res = PQexec(_database, create_sssp_source_step_sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        exit_err("Connection to database failed",res);
    }
    PQclear(res);

    const char *statement = "SELECT id FROM Graph WHERE adj_description=$1;";
    const char *paramValues[1];
    paramValues[0] = graphAdj.c_str();
    res = PQexecParams(_database, statement, 1, NULL, paramValues, 
        NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        exit_err("Could not read graph",res);
    }

    int results = PQntuples(res);
    // Graph does not exist yet, insert it ourselves
    if(results == 0) {
        exit_err("Does not support inserting the graph yet!",res);
    }
    else {
        _postgresGraphId = PQgetvalue(res, 0, 0);
        printf("Got graph id %s\n",_postgresGraphId.c_str());
    }
    PQclear(res);
}


PostgresBackend::~PostgresBackend() {
    if(_database) {
        PQfinish(_database);
        _database = nullptr;
    }
}