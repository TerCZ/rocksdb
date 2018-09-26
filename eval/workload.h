//
// Created by 唐楚哲 on 2018/9/19.
//

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#ifndef ROCKSDB_WORKLOAD_H
#define ROCKSDB_WORKLOAD_H

class Workload {
 public:
  enum WriteType { BlindWrite, ReadModifyWrite };

  virtual ~Workload() {};
  virtual void prepare_db() = 0;
  virtual void do_query() = 0;
};

class GenericPointWorkload : public Workload {
 public:
  GenericPointWorkload(
      rocksdb::DB *db,
      int initial_db_size,
      int key_space,
      int key_size,
      int value_size,
      double write_ratio,
      WriteType write_type);

  ~GenericPointWorkload() {}
  virtual void prepare_db() override final;
  virtual void do_query() override final;

 private:
  std::string from_key_id_to_str(int key_i);
  std::string random_value();

  rocksdb::DB *db;

  const int initial_db_size;
  const int key_space;
  const int key_size, value_size;

  const double write_ratio;
  WriteType write_type;
};

std::istream &operator>>(std::istream &is, Workload::WriteType &val);
std::ostream &operator<<(std::ostream &is, Workload::WriteType &val);

#endif //ROCKSDB_WORKLOAD_H