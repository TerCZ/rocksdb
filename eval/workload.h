//
// Created by 唐楚哲 on 2018/9/19.
//

#ifndef ROCKSDB_WORKLOAD_H
#define ROCKSDB_WORKLOAD_H

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

class Workload {
 public:
  virtual ~Workload() {};
  virtual void prepare_db() = 0;
  virtual void proceed() = 0;
  virtual bool is_finished() = 0;
};

class GenericPointWorkload : public Workload {
 public:
  GenericPointWorkload(
      rocksdb::DB *db, int key_space, int initial_db_size, int workload_size, double write_ratio,
      int key_size, int value_size);
  ~GenericPointWorkload() {}
  virtual void prepare_db() override final;
  virtual void proceed() override final;
  virtual bool is_finished() override final;

 private:
  std::string from_key_id_to_str(int key_i);
  std::string random_value();

  rocksdb::DB *db;
  const int initial_db_size;

  const int key_space, workload_size;
  const int key_size, value_size;
  const double write_ratio;

  int finished_num;
};



class WorkloadD : public Workload {
 public:
  WorkloadD(rocksdb::DB *db, int key_space, int initial_db_size,
                       int workload_size, double write_ratio, int key_size,
                       int value_size);
  ~WorkloadD() {}
  virtual void prepare_db() override final;
  virtual void proceed() override final;
  virtual bool is_finished() override final;

 private:
  std::string from_key_id_to_str(int key_i);
  std::string random_value();

  rocksdb::DB *db;
  const int initial_db_size;

  const int key_space, workload_size;
  const int key_size, value_size;
  const double write_ratio;

  int finished_num;
};
class WorkloadE : public Workload {
 public:
  WorkloadE(rocksdb::DB *db, int key_space, int initial_db_size,
            int workload_size, double write_ratio, int key_size,
            int value_size);
  ~WorkloadE() {}
  virtual void prepare_db() override final;
  virtual void proceed() override final;
  virtual bool is_finished() override final;

 private:
  std::string from_key_id_to_str(int key_i);
  std::string random_value();

  rocksdb::DB *db;
  const int initial_db_size;

  const int key_space, workload_size;
  const int key_size, value_size;
  const double write_ratio;

  int finished_num;
  
};

class WorkloadF : public Workload {
 public:
  WorkloadF(rocksdb::DB *db, int key_space, int initial_db_size,
            int workload_size, double write_ratio, int key_size,
            int value_size);
  ~WorkloadF() {}
  virtual void prepare_db() override final;
  virtual void proceed() override final;
  virtual bool is_finished() override final;

 private:
  std::string from_key_id_to_str(int key_i);
  std::string random_value();

  rocksdb::DB *db;
  const int initial_db_size;

  const int key_space, workload_size;
  const int key_size, value_size;
  const double write_ratio;

  int finished_num;
  
};

#endif //ROCKSDB_WORKLOAD_H
