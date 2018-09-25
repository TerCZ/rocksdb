//
// Created by tang on 18-9-20.
//

#include "util.h"

#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <cmath>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include <numeric>

using namespace std;

Config::Config(std::string db_path,
               int initial_db_size,
               int key_space,
               int workload_size,
               int key_size,
               int value_size,
               double write_ratio,
               int ops_per_sample_period,
               int latency_sample_per_sample_period,
               int iter_num,
               std::string out_path,
               Config::Workload workload)
    : db_path(db_path),
      initial_db_size(initial_db_size),
      key_space(key_space),
      workload_size(workload_size),
      key_size(key_size),
      value_size(value_size),
      write_ratio(write_ratio),
      ops_per_sample_period(ops_per_sample_period),
      latency_sample_per_sample_period(latency_sample_per_sample_period),
      iter_num(iter_num),
      out_path(out_path),
      workload(workload) {}

template<class ValT>
ValT average(vector<ValT> vals) {
  ValT init = 0;
  return accumulate(vals.begin(), vals.end(), init) / vals.size();
}

void Stat::finish_latency_stat() {
  if (compaction_latencies.size() > 0) {
    sort(compaction_latencies.begin(), compaction_latencies.end());
    compaction_latency_mean = average(compaction_latencies);
    compaction_latency_95 = compaction_latencies[compaction_latencies.size() * 95 / 100];
    compaction_latency_99 = compaction_latencies[compaction_latencies.size() * 99 / 100];
  } else {
    compaction_latency_mean = 0;
    compaction_latency_95 = 0;
    compaction_latency_99 = 0;
  }

  if (non_compaction_latencies.size() > 0) {
    sort(non_compaction_latencies.begin(), non_compaction_latencies.end());
    non_compaction_latency_mean = average(non_compaction_latencies);
    non_compaction_latency_95 = non_compaction_latencies[non_compaction_latencies.size() * 95 / 100];
    non_compaction_latency_99 = non_compaction_latencies[non_compaction_latencies.size() * 99 / 100];
  } else {
    non_compaction_latency_mean = 0;
    non_compaction_latency_95 = 0;
    non_compaction_latency_99 = 0;
  }
}

void parse_config(int argc, char **argv, vector<Config> &configs) {
  assert(argc == 2);

  ifstream infile(argv[1]);
  assert(infile.is_open());

  // extract lines from config file to string next
  string line_str;

  // container for configurations
  string db_path, out_path;
  vector<int> ops_per_sample_periods, latency_sample_per_sample_periods, initial_db_sizes, workload_sizes, key_spaces,
      key_sizes, value_sizes, iter_nums;
  vector<double> write_ratios;
  vector<Config::Workload> workloads;

  // read them all
  while (!getline(infile, line_str).eof()) {
    stringstream line_stream(line_str);
    string start;
    line_stream >> start;

    if (start == "db_path") {
      line_stream >> db_path;
    } else if (start == "out_path") {
      line_stream >> out_path;
    } else if (start == "ops_per_sample_period") {
      while (!line_stream.eof()) {  // there can be multiple ops_per_sample_period
        int ops_per_sample_period;
        line_stream >> ops_per_sample_period;
        ops_per_sample_periods.push_back(ops_per_sample_period);
      }
    } else if (start == "latency_sample_per_sample_period") {
      while (!line_stream.eof()) {  // there can be multiple latency_sample_per_sample_period
        int latency_sample_per_sample_period;
        line_stream >> latency_sample_per_sample_period;
        latency_sample_per_sample_periods.push_back(latency_sample_per_sample_period);
      }
    } else if (start == "initial_db_size") {
      while (!line_stream.eof()) {  // there can be multiple initial_db_size
        int initial_db_size;
        line_stream >> initial_db_size;
        initial_db_sizes.push_back(initial_db_size);
      }
    } else if (start == "workload_size") {
      while (!line_stream.eof()) {  // there can be multiple workload_size
        int workload_size;
        line_stream >> workload_size;
        workload_sizes.push_back(workload_size);
      }
    } else if (start == "key_space") {
      while (!line_stream.eof()) {  // there can be multiple key_space
        int key_space;
        line_stream >> key_space;
        key_spaces.push_back(key_space);
      }
    } else if (start == "key_size") {
      while (!line_stream.eof()) {  // there can be multiple key_size
        int key_size;
        line_stream >> key_size;
        key_sizes.push_back(key_size);
      }
    } else if (start == "value_size") {
      while (!line_stream.eof()) {  // there can be multiple value_size
        int value_size;
        line_stream >> value_size;
        value_sizes.push_back(value_size);
      }
    } else if (start == "iter_num") {
      while (!line_stream.eof()) {  // there can be multiple iter_num
        int iter_num;
        line_stream >> iter_num;
        iter_nums.push_back(iter_num);
      }
    } else if (start == "workload") {
      while (!line_stream.eof()) {  // there can be multiple workload
        string workload;
        line_stream >> workload;
        if (workload == "YCSB_A") {
          workloads.push_back(Config::Workload::YCSB_A);
        } else if (workload == "YCSB_B") {
          workloads.push_back(Config::Workload::YCSB_B);
        } else if (workload == "YCSB_C") {
          workloads.push_back(Config::Workload::YCSB_C);
        } else if (workload == "GenericPoint") {
          workloads.push_back(Config::Workload::GenericPoint);
        }  else if (workload == "WorkloadD") {
          workloads.push_back(Config::Workload::GenericPoint);
        }  else if (workload == "WorkloadE") {
          workloads.push_back(Config::Workload::GenericPoint);
        }  else if (workload == "WorkloadF") {
          workloads.push_back(Config::Workload::GenericPoint);
        } else {
          assert(0);
        }
      }
    } else if (start == "write_ratio") {
      while (!line_stream.eof()) {  // there can be multiple write_ratio
        double write_ratio;
        line_stream >> write_ratio;
        write_ratios.push_back(write_ratio);
      }
    }
  }

  // error asserting
  assert((db_path != "" && out_path != ""));

  // construct grid search configs
  for (int ops_per_sample_period: ops_per_sample_periods) {
    for (int latency_sample_per_sample_period: latency_sample_per_sample_periods) {
      for (Config::Workload workload: workloads) {
        // only care about user specified write ratio when using GenericPoint workload
        vector<double> actual_ratios;

        if (workload == Config::Workload::GenericPoint) {
          actual_ratios = write_ratios;
        } else {
          double write_ratio;
          switch (workload) {
            case Config::Workload::YCSB_A:write_ratio = 0.5;
              break;
            case Config::Workload::YCSB_B:write_ratio = 0.05;
              break;
            case Config::Workload::YCSB_C:write_ratio = 0;
              break;
          }
          actual_ratios.push_back(write_ratio);
        }

        for (double write_ratio: actual_ratios) {
          for (int initial_db_size: initial_db_sizes) {
            for (int workload_size: workload_sizes) {
              for (int key_space: key_spaces) {
                for (int key_size: key_sizes) {
                  for (int value_size: value_sizes) {
                    for (int iter_num: iter_nums) {
                      Config config(
                          db_path, initial_db_size, key_space, workload_size, key_size, value_size, write_ratio,
                          ops_per_sample_period, latency_sample_per_sample_period, iter_num, out_path, workload);
                      configs.push_back(config);
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}

void output_header(ofstream &out) {
  out << "sample frequency (op), "
         "key space, "
         "initial db size (record), "
         "workload size (op), "
         "write ratio, "
         "key size (B), "
         "value size (B), "
         "compaction time (s), "
         "compaction throughput (ops), "
         "compaction latency mean (s), "
         "compaction latency 95% (s), "
         "compaction latency 99% (s), "
         "non compaction time (s), "
         "non compaction throughput (ops), "
         "non compaction latency mean (s), "
         "non compaction latency 95% (s), "
         "non compaction latency 99% (s), "
         "final DB size (MB)" << std::endl;
}

void output_entry(ofstream &out, const Stat &stat, const Config &config) {
  out //<< config.ops_per_sample_period << ", "
      //<< config.key_space << ", "
      //<< config.initial_db_size << ", "
     // << config.workload_size << ", "
      //<< config.write_ratio << ", "
      //<< config.key_size << ", "
      //<< config.value_size << ", "
      << stat.compaction_time << ", "
      //<< stat.compaction_ops / stat.compaction_time << ", "
      //<< stat.compaction_latency_mean << ", "
     // << stat.compaction_latency_95 << ", "
     // << stat.compaction_latency_99 << ", "
      //<< stat.non_compaction_time << ", "
      << stat.non_compaction_ops / stat.non_compaction_time << ", ";
      //<< stat.non_compaction_latency_mean << ", "
     // << stat.non_compaction_latency_95 << ", "
     // << stat.non_compaction_latency_99 << ", "
     // << stat.final_db_size << endl;
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
