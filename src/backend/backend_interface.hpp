#ifndef __BACKEND_INTERFACE_HPP__
#define __BACKEND_INTERFACE_HPP__
#include "../metrics/metrics.hpp"

class BackendInterface {
    public:
        virtual void dump(SSSPMetrics *metrics, unsigned long long sourceNode) = 0;
};

#endif