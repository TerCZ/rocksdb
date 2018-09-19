//
// Created by 唐楚哲 on 2018/9/19.
//

#include <atomic>
#include <chrono>
#include <iostream>
#include <cstdlib>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

#include "workload.h"


std::atomic<int> compaction_num(0);


struct Config {
    std::string db_path;
    int ops_per_sample_period;
    int record_num;
    int workload_size;
    int key_size;
    int value_size;
};


Config parse_config(int argc, char **argv) {
    assert(argc == 7);

    Config config;
    config.db_path = std::string(argv[1]);
    config.ops_per_sample_period = std::atoi(argv[2]);
    config.record_num = std::atoi(argv[3]);
    config.workload_size = std::atoi(argv[4]);
    config.key_size = std::atoi(argv[5]);
    config.value_size = std::atoi(argv[6]);

    std::cout << "ops_per_sample_period: " << config.ops_per_sample_period << std::endl;
    std::cout << "record_num: " << config.record_num << std::endl;
    std::cout << "workload_size: " << config.workload_size << std::endl;
    std::cout << "key_size: " << config.key_size << std::endl;
    std::cout << "value_size: " << config.value_size << std::endl;

    return config;
}


int main(int argc, char **argv) {

    // parse config
    Config config = parse_config(argc, argv);

    // clear existing data
    std::string cmd = "rm -rf ";
    cmd += config.db_path;
    if (std::system(cmd.c_str()) != 0) {
        std::cout << "failed to clear existing directory" << std:endl;
        return -1;
    }

    // init db
    rocksdb::DB *db;
    rocksdb::Options options;

    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    options.create_if_missing = true;

    rocksdb::Status s = rocksdb::DB::Open(options, config.db_path, &db);
    if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
        return -2;
    }

    // init workload
    YCSB_A ycsb_a(db, config.record_num, config.workload_size, config.key_size, config.value_size);
    ycsb_a.prepare_db();

    // statistics variables
    double compaction_time = 0, non_compaction_time = 0;
    int compaction_ops = 0, non_compaction_ops = 0;

    // run the workload
    while (!ycsb_a.is_finished()) {

        // check if this period should count as compaction period
        bool compaction_on_going = compaction_num > 0;

        // start timing
        auto start_time = std::chrono::steady_clock::now();

        // execute at most ops_per_sample_period operations
        for (int i = 0; i < config.ops_per_sample_period && !ycsb_a.is_finished(); ++i) {
            ycsb_a.proceed();
        }

        // stop timing
        auto end_time = std::chrono::steady_clock::now();

        // update statistics
        double duration = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time).count();
        if (compaction_on_going) {
            compaction_time += duration;
            compaction_ops += config.ops_per_sample_period;
        } else {
            non_compaction_time += duration;
            non_compaction_ops += config.ops_per_sample_period;
        }
    }

    // close db
    delete db;

    // output
    std::cout << "compaction time: " << compaction_time << "s, throughput: " << compaction_ops / compaction_time
              << std::endl;
    std::cout << "non-compaction time: " << non_compaction_time << "s, throughput: "
              << non_compaction_ops / non_compaction_time << std::endl;
}