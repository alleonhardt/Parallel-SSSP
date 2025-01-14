#ifndef __METRICS_HPP__
#define __METRICS_HPP__

#include <mutex>
#include <set>
#include <vector>
#include <map>

class BackendInterface;

/*
This class logs advanced metrics from the execution of the SSSP algorithm. Since it is written to be correct but not fast, it will slow down
the execution speed of the programm considerably
*/
class SSSPMetrics {
    private:
        // long long to enable graphs with more than 2^32-1 nodes
        // Use vector to record multiple rounds at once
        std::vector<std::set<unsigned long long>> _insertions_by_id;
        std::map<unsigned long long, unsigned long> _insertions_per_node;
        std::map<unsigned long long, unsigned long> _edge_relaxations_per_node;
        
        // Protect functions which mutate the state from race conditions
        std::mutex _access_guard;

        // The current step in the algorithm
        int _currentStep;

        BackendInterface *_backend;

        // Log the data for every distinct round
        void dump(unsigned long long sourceNode);
    
    public:
        SSSPMetrics(BackendInterface *interface);
        void log_node_add(unsigned long long nodeId);
        void log_node_relax(unsigned long long nodeId, unsigned long neighborhood_size);

        // Used to reset the Metrics after each round
        void reset_round(unsigned long long sourceNode);
        void reset_round();

        // Increase the current algorithm step
        void incAlgorithmStep();

        int getCurrentTotalCount();

        // Use RAII
        ~SSSPMetrics();

        std::vector<std::set<unsigned long long>> &getRoundMetrics();
        std::map<unsigned long long, unsigned long> &getInsertionsPerNode();
        std::map<unsigned long long, unsigned long> &getEdgeRelaxationsPerNode();
};

#endif