#include "metrics.hpp"
#include "../backend/backend_interface.hpp"
#include <iostream>

SSSPMetrics::SSSPMetrics(BackendInterface *interface) : _backend(interface)
{
    // At least one round is needed
    _insertions_by_id.push_back({});
}

void SSSPMetrics::incAlgorithmStep()
{
    _currentStep += 1;
    _insertions_by_id.push_back({});
}

SSSPMetrics::~SSSPMetrics()
{
    if(_backend) {
        delete _backend;
        _backend = nullptr;
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
    _insertions_per_node[nodeId]+=1;
}

void SSSPMetrics::log_node_relax(unsigned long long nodeId, unsigned long neighborhood_size) {
    auto lck = std::unique_lock<std::mutex>(_access_guard);
    _edge_relaxations_per_node[nodeId]+=neighborhood_size;
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
    _insertions_per_node.clear();
    _edge_relaxations_per_node.clear();
    _currentStep = 0;
}

void SSSPMetrics::dump(unsigned long long sourceNode)
{
    _backend->dump(this,sourceNode);
}

std::vector<std::set<unsigned long long>> &SSSPMetrics::getRoundMetrics() {
    return _insertions_by_id;
}

std::map<unsigned long long, unsigned long> &SSSPMetrics::getInsertionsPerNode() {
    return _insertions_per_node;
}

std::map<unsigned long long, unsigned long> &SSSPMetrics::getEdgeRelaxationsPerNode() {
    return _edge_relaxations_per_node;
}
