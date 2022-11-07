#include <mutex>
#include <set>
#include <vector>
#include <sqlite3.h>

/*
This class logs advanced metrics from the execution of the SSSP algorithm. Since it is written to be correct but not fast, it will slow down
the execution speed of the programm considerably
*/
class SSSPMetrics {
    private:
        // long long to enable graphs with more than 2^32-1 nodes
        // Use vector to record multiple rounds at once
        std::vector<std::set<unsigned long long>> _insertions_by_id;
        
        // Protect functions which mutate the state from race conditions
        std::mutex _access_guard;

        // Log the results to this file
        std::string _dump_filename;

        // The database to store the records
        sqlite3 *_database;

        // The graphId in the sqlite database
        int _sqliteGraphId;

        // The current step in the algorithm
        int _currentStep;

        std::string _algorithm;
        int _algorithmParameter;

        // Log the data for every distinct round
        void dump(unsigned long long sourceNode);
    
    public:
        SSSPMetrics(std::string filename, std::string graphAdj, std::string algorithm, int parameter);
        void log_node_add(unsigned long long nodeId);

        // Used to reset the Metrics after each round
        void reset_round(unsigned long long sourceNode);
        void reset_round();

        // Increase the current algorithm step
        void incAlgorithmStep();

        int getCurrentTotalCount();

        // Use RAII
        ~SSSPMetrics();
    
        friend int read_graph(void *data, int argc, char **argv, char **azColName);
};