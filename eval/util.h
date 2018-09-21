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
  int initial_db_size;

  int key_space, workload_size, key_size, value_size;
  double write_ratio;

  int ops_per_sample_period, iter_num;
  std::string out_path;

  enum Workload { GenericPoint, YCSB_A, YCSB_B, YCSB_C };
  Workload workload;
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
