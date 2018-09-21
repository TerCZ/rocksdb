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

using namespace std;

void parse_config(int argc, char **argv, vector<Config> &configs) {
  assert(argc == 2);

  ifstream infile(argv[1]);
  assert(infile.is_open());

  // extract lines from config file to string next
  string line_str;

  // container for configurations
  string db_path, out_path;
  vector<int> ops_per_sample_periods, initial_db_sizes, workload_sizes, key_sizes, value_sizes, iter_nums;
  vector<double> write_ratios;
  vector<Config::Workload> workloads;

  int key_space, workload_size, key_size, value_size;
  double write_ratio;

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
    for (Config::Workload workload: workloads) {
      if (workload != Config::Workload::GenericPoint) {
        for (double write_ratio: write_ratios) {
          for (int initial_db_size: initial_db_sizes) {
            for (int workload_size: workload_sizes) {
              for (int key_size: key_sizes) {
                for (int value_size: value_sizes) {
                  for (int iter_num: iter_nums) {
                    Config config;
                    config.db_path = db_path;
                    config.out_path = out_path;
                    config.ops_per_sample_period = ops_per_sample_period;
                    config.initial_db_size = initial_db_size;
                    config.workload_size = workload_size;
                    config.key_size = key_size;
                    config.value_size = value_size;
                    config.iter_num = iter_num;
                    config.workload = workload;
                    config.write_ratio = write_ratio;
                    configs.push_back(config);
                  }
                }
              }
            }
          }
        }
      } else {
        for (int initial_db_size: initial_db_sizes) {
          for (int workload_size: workload_sizes) {
            for (int key_size: key_sizes) {
              for (int value_size: value_sizes) {
                for (int iter_num: iter_nums) {
                  double write_ratio;
                  switch (workload) {
                    case Config::Workload::YCSB_A:
                      write_ratio = 0.5;
                      break;
                    case Config::Workload::YCSB_B:
                      write_ratio = 0.5;
                      break;
                    case Config::Workload::YCSB_C:
                      write_ratio = 0;
                      break;
                  }

                  Config config;
                  config.db_path = db_path;
                  config.out_path = out_path;
                  config.ops_per_sample_period = ops_per_sample_period;
                  config.initial_db_size = initial_db_size;
                  config.workload_size = workload_size;
                  config.key_size = key_size;
                  config.value_size = value_size;
                  config.iter_num = iter_num;
                  config.workload = workload;
                  config.write_ratio = write_ratio;
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

void output_header(ofstream &out) {
  out << "sample frequency (op)\t"
         "key space\t"
         "initial db size (record)\t"
         "workload size (op)\t"
         "write ratio\t"
         "key size (B)\t"
         "value size (B)\t"
         "compaction time (s)\t"
         "compaction throughput (ops)\t"
         "non compaction time (s)\t"
         "non compaction throughput (ops)\t"
         "final DB size (MB)" << std::endl;
}

void output_entry(ofstream &out, const Stat &stat, const Config &config) {
  out << config.ops_per_sample_period << '\t'
      << config.key_space << '\t'
      << config.initial_db_size << '\t'
      << config.workload_size << '\t'
      << config.write_ratio << '\t'
      << config.key_size << '\t'
      << config.value_size << '\t'
      << stat.compaction_time << '\t'
      << stat.compaction_ops / stat.compaction_time << '\t'
      << stat.non_compaction_time << '\t'
      << stat.non_compaction_ops / stat.non_compaction_time << '\t'
      << stat.final_db_size << endl;
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