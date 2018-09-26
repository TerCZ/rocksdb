//
// Created by tang on 18-9-20.
//

#include "util.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace std;

Config::Config(
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
    int iter_num)
    : result_path(result_path),
      db_path(db_path),
      initial_db_size(initial_db_size),
      key_space(key_space),
      key_size(key_size),
      value_size(value_size),
      write_ratio(write_ratio),
      write_type(write_type),
      client_thread_n(client_thread_n),
      benchmark_duration(benchmark_duration),
      seconds_between_manager_pulling(seconds_between_manager_pulling),
      ops_between_latency_sample(ops_between_latency_sample),
      iter_num(iter_num) {}

StatEntry::StatEntry(double time,
                     double throughput,
                     double latency_mean,
                     double latency_95,
                     double latency_99,
                     int compaction_job_n,
                     int flush_job_n)
    : time(time),
      throughput(throughput),
      latency_mean(latency_mean),
      latency_95(latency_95),
      latency_99(latency_99),
      compaction_job_n(compaction_job_n),
      flush_job_n(flush_job_n) {}

template<class ValT>
ValT average(vector<ValT> vals) {
  ValT init = 0;
  return accumulate(vals.begin(), vals.end(), init) / vals.size();
}

/*
 * process_latencies
 * Requirement:
 *  latencies.size() > 0
 */
void process_latencies(vector<double> latencies, double &latency_mean, double &latency_95, double &latency_99) {
  sort(latencies.begin(), latencies.end());
  latency_mean = average(latencies);
  latency_95 = latencies[latencies.size() * 95 / 100];
  latency_99 = latencies[latencies.size() * 99 / 100];
}

template<class ValT>
void parse_into_vector(stringstream &line_stream, vector<ValT> &vec) {
  while (!line_stream.eof()) {  // there can be multiple db_path
    ValT option;
    line_stream >> option;
    vec.push_back(option);
  }
}

vector<Config> parse_config(int argc, char **argv) {
  assert(argc == 2);

  vector<Config> configs;

  ifstream infile(argv[1]);
  assert(infile.is_open());

  // extract lines from config file to string next
  string line_str;

  // container for configurations
  string result_path;
  vector<string> db_paths;
  vector<int> initial_db_sizes, key_spaces, key_sizes, value_sizes, client_thread_ns, benchmark_durations,
      seconds_between_manager_pullings, ops_between_latency_samples, iter_nums;
  vector<double> write_ratios;
  vector<Workload::WriteType> write_types;

  // read them all
  while (!getline(infile, line_str).eof()) {
    stringstream line_stream(line_str);
    string start;
    line_stream >> start;

    if (start == "result_path") {
      line_stream >> result_path;
    } else if (start == "db_path") {
      parse_into_vector(line_stream, db_paths);
    } else if (start == "initial_db_size") {
      parse_into_vector(line_stream, initial_db_sizes);
    } else if (start == "key_space") {
      parse_into_vector(line_stream, key_spaces);
    } else if (start == "key_size") {
      parse_into_vector(line_stream, key_sizes);
    } else if (start == "value_size") {
      parse_into_vector(line_stream, value_sizes);
    } else if (start == "write_ratio") {
      parse_into_vector(line_stream, write_ratios);
    } else if (start == "write_type") {
      parse_into_vector(line_stream, write_types);
    } else if (start == "client_thread_n") {
      parse_into_vector(line_stream, client_thread_ns);
    } else if (start == "benchmark_duration") {
      parse_into_vector(line_stream, benchmark_durations);
    } else if (start == "seconds_between_manager_pulling") {
      parse_into_vector(line_stream, seconds_between_manager_pullings);
    } else if (start == "ops_between_latency_sample") {
      parse_into_vector(line_stream, ops_between_latency_samples);
    } else if (start == "iter_num") {
      parse_into_vector(line_stream, iter_nums);
    } else {
      cerr << "wrong config: " << start << endl;
      assert(0);  // invalid config name
    }
  }

  // construct grid search configs
  for (const auto db_path: db_paths)
    for (const auto initial_db_size: initial_db_sizes)
      for (const auto key_space: key_spaces)
        for (const auto key_size: key_sizes)
          for (const auto value_size: value_sizes)
            for (const auto write_ratio: write_ratios)
              for (const auto write_type: write_types)
                for (const auto client_thread_n: client_thread_ns)
                  for (const auto benchmark_duration: benchmark_durations)
                    for (const auto seconds_between_manager_pulling: seconds_between_manager_pullings)
                      for (const auto ops_between_latency_sample: ops_between_latency_samples)
                        for (const auto iter_num: iter_nums)
                          configs.push_back(
                              Config(result_path,
                                     db_path,
                                     initial_db_size,
                                     key_space,
                                     key_size,
                                     value_size,
                                     write_ratio,
                                     write_type,
                                     client_thread_n,
                                     benchmark_duration,
                                     seconds_between_manager_pulling,
                                     ops_between_latency_sample,
                                     iter_num));

  return configs;
}

void output_header(ostream &out) {
  out << "thread id\t"
         "db path\t"
         "initial db size\t"
         "key space\t"
         "key size\t"
         "value size\t"
         "write ratio\t"
         "write type\t"
         "client thread num\t"
         "benchmark duration\t"
         "ops between latency sample\t"
         "compaction thread num\t"
         "flush thread num\t"
         "time (s)\t"
         "throughput (ops)\t"
         "mean latency (s)\t"
         "95 latency (s)\t"
         "99 latency (s)" << endl;
}

void output_entry(ostream &out,
                  const Config &config,
                  const StatEntry &entry) {
  out << this_thread::get_id() << '\t'
      << config.db_path << '\t'
      << config.initial_db_size << '\t'
      << config.key_space << '\t'
      << config.key_size << '\t'
      << config.value_size << '\t'
      << config.write_ratio << '\t'
      << config.write_type << '\t'
      << config.client_thread_n << '\t'
      << config.benchmark_duration << '\t'
      << config.ops_between_latency_sample << '\t'
      << entry.compaction_job_n << '\t'
      << entry.flush_job_n << '\t'
      << entry.time << '\t'
      << entry.throughput << '\t'
      << entry.latency_mean << '\t'
      << entry.latency_95 << '\t'
      << entry.latency_99 << endl;
}

void clear_folder(std::string path) {
  string cmd = "rm -rf ";
  cmd += path;
  assert(system(cmd.c_str()) == 0);
}

long long get_folder_size(std::string path) {
  // command to be executed
  std::string cmd("du -sb ");
  cmd.append(path);
  cmd.append(" | cut -f1 2>&1");

  // execute above command and get the output
  FILE *stream = popen(cmd.c_str(), "r");
  if (stream) {
    const int max_size = 256;
    char readbuf[max_size];
    if (fgets(readbuf, max_size, stream) != NULL) {
      return atoll(readbuf);
    }
    pclose(stream);
  }

  // return error val
  return -1;
}