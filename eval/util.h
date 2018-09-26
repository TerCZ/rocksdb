//
// Created by tang on 18-9-20.
//

#include <vector>
#include <string>
#include <fstream>

#include "workload.h"

#ifndef ROCKSDB_UTIL_H
#define ROCKSDB_UTIL_H

class Config {
 public:
  Config(
      std::string result_path,
      std::string db_path,
      int initial_db_size,
      int key_space,
      int key_size,
      int value_size,
      double write_ratio,
      Workload::WriteType write_type,
      int client_thread_n,
      int benchmark_duration,
      int seconds_between_manager_pulling,
      int ops_between_latency_sample,
      int iter_num);

  const std::string result_path;

  const std::string db_path;
  const int initial_db_size;
  const int key_space;
  const int key_size;
  const int value_size;

  const double write_ratio;
  const Workload::WriteType write_type;
  const int client_thread_n;
  const int benchmark_duration;

  const int seconds_between_manager_pulling;
  const int ops_between_latency_sample;
  const int iter_num;
};

class StatEntry {
 public:
  StatEntry(double time,
            double throughput,
            double latency_mean,
            double latency_95,
            double latency_99,
            int compaction_job_n,
            int flush_job_n);

  const double time;
  const double throughput;
  const double latency_mean;
  const double latency_95;
  const double latency_99;
  const int compaction_job_n;
  const int flush_job_n;
};

void process_latencies(std::vector<double> latencies, double &latency_mean, double &latency_95, double &latency_99);
std::vector<Config> parse_config(int argc, char **argv);
std::string to_result_filename(const Config &config);
void output_header(std::ostream &out);
void output_entry(std::ostream &out, const Config &config, const StatEntry &entry);
void clear_folder(std::string path);
long long get_folder_size(std::string path);

#endif //ROCKSDB_UTIL_H
