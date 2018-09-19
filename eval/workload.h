//
// Created by 唐楚哲 on 2018/9/19.
//

#ifndef ROCKSDB_WORKLOAD_H
#define ROCKSDB_WORKLOAD_H


#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"


class YCSB_A {
public:
    YCSB_A(rocksdb::DB *db, int record_num, int workload_size, int key_size = 8, int value_size = 1024);

    void prepare_db();

    void proceed();

    bool is_finished();

private:
    rocksdb::DB *db;

    int record_num;
    int workload_size;

    int key_size;
    int value_size;

    int finished_num;
};

#endif //ROCKSDB_WORKLOAD_H