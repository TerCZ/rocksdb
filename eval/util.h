//
// Created by tang on 18-9-20.
//

#include <vector>
#include <string>
#include <fstream>

#ifndef ROCKSDB_UTIL_H
#define ROCKSDB_UTIL_H


struct Config {
    std::string db_path;
    std::string out_path;
    int ops_per_sample_period;
    int record_num;
    int workload_size;
    int key_size;
    int value_size;
    int iter_num;
};


struct Stat {
    double compaction_time, non_compaction_time;
    int compaction_ops, non_compaction_ops;
    long long final_db_size;
};


struct AggregatedStat {
    double compaction_time_mean, compaction_time_std;
    double non_compaction_time_mean, non_compaction_time_std;
    double compaction_throughput_mean, compaction_throughput_std;
    double non_compaction_throughput_mean, non_compaction_throughput_std;
};


void parse_config(int argc, char **argv, std::vector<Config> &configs);
void output_header(std::ofstream &out);
void output_entry(std::ofstream &out, const Stat &stat, const Config &config);
void clear_folder(std::string path);
long long get_folder_size(std::string path);

#endif //ROCKSDB_UTIL_H
