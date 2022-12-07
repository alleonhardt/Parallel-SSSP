#ifndef _SSSP_STATS_HPP_
#define _SSSP_STATS_HPP_
#include <vector>
#include <numeric>
#include <cmath>
#include <algorithm>

template<typename T>class VectorStats {
    private:
        T _sum;
        double _mean;
        T _median;
        double _stddev;
        T _first_quartile;
        T _third_quartile;
    public:
        VectorStats(std::vector<T> *vec, T def_value) {
            _mean = 0.0;
            _stddev = 0.0;
            _median = def_value;
            _first_quartile = def_value;
            _third_quartile = def_value;


            _sum = std::accumulate(vec->begin(),vec->end(),def_value);
            if(vec->size() > 0) {
                _mean = _sum/(T)vec->size();
                _median = (*vec)[vec->size()/2];
                
                std::vector<T> diff(vec->size());
                double copy_mean = _mean;
                std::transform(vec->begin(), vec->end(), diff.begin(), [copy_mean](double x) { return x - copy_mean; });
                double sq_sum = std::inner_product(diff.begin(), diff.end(), diff.begin(), 0.0);
                _stddev = std::sqrt(sq_sum / vec->size());
                
                // Divide by 4 = 0.25
                int first = vec->size()>>2;

                // Divide by 4 + Divide by 2
                int third = first + (vec->size()>>1);
                std::sort(vec->begin(),vec->end());
                _first_quartile = (*vec)[first];
                _third_quartile = (*vec)[third];
            }
        }

        double mean() {
            return _mean;
        }

        T median() {
            return _median;
        }

        double stddev() {
            return _stddev;
        }

        T first_quartile() {
            return _first_quartile;
        }

        T third_quartile() {
            return _third_quartile;
        }

        T sum() {
            return _sum;
        }

};



#endif