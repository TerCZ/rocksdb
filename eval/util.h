//
// Created by tang on 18-9-20.
//

#include <vector>
#include <string>
#include <fstream>

#ifndef ROCKSDB_UTIL_H
#define ROCKSDB_UTIL_H

class Config {
 public:
  enum Workload { GenericPoint, YCSB_A, YCSB_B, YCSB_C };

  Config(
      std::string db_path, int initial_db_size, int key_space, int workload_size, int key_size, int value_size,
      double write_ratio, int ops_per_sample_period, int latency_sample_per_sample_period, int iter_num,
      std::string out_path, Workload workload);

  const std::string db_path;
  const int initial_db_size;

  const int key_space, workload_size, key_size, value_size;
  const double write_ratio;

  const int ops_per_sample_period, latency_sample_per_sample_period, iter_num;
  const std::string out_path;

  const Workload workload;
};

class Stat {
 public:
  double compaction_time, non_compaction_time;
  int compaction_ops, non_compaction_ops;
  long long final_db_size;

  std::vector<double> compaction_latencies, non_compaction_latencies;
  double compaction_latency_mean, compaction_latency_95, compaction_latency_99;
  double non_compaction_latency_mean, non_compaction_latency_95, non_compaction_latency_99;

  void finish_latency_stat();
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
