//
// Created by 唐楚哲 on 2018/9/19.
//

#include <chrono>
#include <random>
#include <iostream>
#include <string>
#include "workload.h"


YCSB_A::YCSB_A(rocksdb::DB *db, int record_num, int workload_size, int key_size, int value_size)
        : db(db),
          record_num(record_num),
          workload_size(workload_size),
          key_size(key_size),
          value_size(value_size),
          finished_num(0) {}

// populate db with uniform-distributed random value
void YCSB_A::prepare_db() {

    std::default_random_engine key_generator, value_generator;
    std::uniform_int_distribution<int> distribution;

    for (int i = 0; i < record_num; ++i) {
        // construct random key of length key_size
        std::string key = std::to_string(i);
        key.resize(key_size, 'x');

        // construct random value of length value_size
        int value_off = 0;
        std::string value;
        while (value_off < value_size) {
            int random_int = distribution(value_generator);
            int append_len = value_size - value_off > sizeof(random_int) ? sizeof(random_int) : value_size - value_off;
            value += std::string(reinterpret_cast<char *>(&random_int), append_len);
            value_off += append_len;
        }

        // insert to db
        rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, value);
        if (!s.ok()) {
            std::cout << s.ToString() << std::endl;
            exit(-1);
        }
    }
}

// proceed the workload with one more operation
void YCSB_A::proceed() {

    static std::default_random_engine generator;
    static std::uniform_int_distribution<int> uniform_int_distribution;
    static std::uniform_real_distribution<double> uniform_real_distribution(0, 1);

    double choice = uniform_real_distribution(generator);

    // 50% update, 50% read
    if (choice > 0.5) {  // update

        // construct an existing key
        int key_int = uniform_int_distribution(generator) % record_num;
        std::string key = std::to_string(key_int);
        key.resize(key_size, 'x');

        // construct new random value of length value_size
        int value_off = 0;
        std::string value;
        while (value_off < value_size) {
            int random_int = uniform_int_distribution(generator);
            int append_len = value_size - value_off > sizeof(random_int) ? sizeof(random_int) : value_size - value_off;
            value += std::string(reinterpret_cast<char *>(&random_int), append_len);
            value_off += append_len;
        }

        // update to db
        rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, value);
        if (!s.ok()) {
            std::cout << s.ToString() << std::endl;
            exit(-1);
        }

    } else {

        // construct an existing key
        int key_int = uniform_int_distribution(generator) % record_num;
        std::string key = std::to_string(key_int);
        key.resize(key_size, 'x');

        // get from db
        rocksdb::PinnableSlice value;
        rocksdb::Status s = db->Get(rocksdb::ReadOptions(), db->DefaultColumnFamily(), key, &value);
        if (!s.ok()) {
            std::cout << s.ToString() << std::endl;
            exit(-1);
        }
        value.Reset();
    }

    ++finished_num;
}

bool YCSB_A::is_finished() {
    return finished_num >= workload_size;
}
