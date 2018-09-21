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

using namespace std;

void parse_config(int argc, char **argv, vector<Config> &configs) {
  assert(argc == 2);

  ifstream infile(argv[1]);
  assert(infile.is_open());

  // extract lines from config file to string next
  string line_str;

  // container for configurations
  string db_path, out_path;
  vector<int> ops_per_sample_periods, record_nums, workload_sizes, key_sizes, value_sizes, iter_nums;

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
    } else if (start == "record_num") {
      while (!line_stream.eof()) {  // there can be multiple record_num
        int record_num;
        line_stream >> record_num;
        record_nums.push_back(record_num);
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
    }

  }

  // error asserting
  assert((db_path != "" && out_path != ""));

  // construct grid search configs
  for (int ops_per_sample_period: ops_per_sample_periods) {
    for (int record_num: record_nums) {
      for (int workload_size: workload_sizes) {
        for (int key_size: key_sizes) {
          for (int value_size: value_sizes) {
            for (int iter_num: iter_nums) {
              Config config;
              config.db_path = db_path;
              config.out_path = out_path;
              config.ops_per_sample_period = ops_per_sample_period;
              config.record_num = record_num;
              config.workload_size = workload_size;
              config.key_size = key_size;
              config.value_size = value_size;
              config.iter_num = iter_num;
              configs.push_back(config);
            }
          }
        }
      }
    }
  }
}

void output_header(ofstream &out) {
  out << "sample frequency (op)\t"
         "initial db size (record)\t"
         "workload size (op)\t"
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
      << config.record_num << '\t'
      << config.workload_size << '\t'
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