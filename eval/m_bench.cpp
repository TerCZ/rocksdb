//
// Created by 唐楚哲 on 2018/9/19.
//

#include <atomic>
#include <chrono>
#include <iostream>
#include <fstream>
#include <vector>
#include <sstream>
#include <cmath>
#include <memory>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

#include "workload.h"
#include "util.h"

using namespace std;

atomic<int> compaction_num(0);

unique_ptr<Workload> get_workload(rocksdb::DB *db, const Config &config) {
  switch (config.workload) {
    case Config::Workload::GenericPoint:
      return unique_ptr<Workload>(
          new GenericPointWorkload(
              db, config.key_space, config.initial_db_size, config.workload_size, config.write_ratio,
              config.key_size, config.value_size));
    case Config::Workload::YCSB_A:
      return unique_ptr<Workload>(
          new GenericPointWorkload(
              db, config.key_space, config.initial_db_size, config.workload_size, 0.5 /* write_ratio */,
              config.key_size, config.value_size));
    case Config::Workload::YCSB_B:
      return unique_ptr<Workload>(new GenericPointWorkload(
          db, config.key_space, config.initial_db_size, config.workload_size, 0.05 /* write_ratio */,
          config.key_size, config.value_size));
    case Config::Workload::YCSB_C:
      return unique_ptr<Workload>(new GenericPointWorkload(
          db, config.key_space, config.initial_db_size, config.workload_size, 0, /* write_ratio */
          config.key_size, config.value_size));
    default:assert(0);
  }
}

Stat run_config_once(const Config &config) {
  // clear existing data
  clear_folder(config.db_path);

  // init db
  rocksdb::DB *db;
  rocksdb::Options options;

  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  options.create_if_missing = true;

  rocksdb::Status s = rocksdb::DB::Open(options, config.db_path, &db);
  if (!s.ok()) {
    cout << s.ToString() << endl;
    exit(-2);
  }

  // init workload
  unique_ptr<Workload> workload = get_workload(db, config);
  workload->prepare_db();

  // statistics variables
  Stat stat;
  stat.compaction_time = 0, stat.non_compaction_time = 0;
  stat.compaction_ops = 0, stat.non_compaction_ops = 0;

  // run the workload
  while (!workload->is_finished()) {

    // check if this period should count as compaction period
    bool compaction_on_going = compaction_num > 0;

    // start timing
    auto start_time = chrono::steady_clock::now();

    // execute at most ops_per_sample_period operations
    for (int i = 0; i < config.ops_per_sample_period && !workload->is_finished(); ++i) {
      workload->proceed();
    }

    // stop timing
    auto end_time = chrono::steady_clock::now();

    // update statistics
    double duration =
        chrono::duration_cast<chrono::duration<double >>(end_time - start_time).count();
    if (compaction_on_going) {
      stat.compaction_time += duration;
      stat.compaction_ops += config.ops_per_sample_period;
    } else {
      stat.non_compaction_time += duration;
      stat.non_compaction_ops += config.ops_per_sample_period;
    }
  }

  stat.final_db_size = get_folder_size(config.db_path);

  // close db
  delete db;

  return stat;
}

void run_config(ofstream &outfile, const Config config) {
  // run several time to average the result
  vector<Stat> stats;
  for (int i = 0; i < config.iter_num; ++i) {
    cout << "run-" << i << "..." << endl;
    Stat stat = run_config_once(config);
    output_entry(outfile, stat, config);
  }
}

int main(int argc, char **argv) {

  // parse config
  vector<Config> configs;
  parse_config(argc, argv, configs);

  // open output file
  ofstream outfile(configs[0].out_path + "/result", ofstream::app);

  // output header
  output_header(outfile);

  // run each config
  for (const Config &config: configs) {
    run_config(outfile, config);
  }
}