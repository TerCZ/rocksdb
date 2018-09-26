//
// Created by 唐楚哲 on 2018/9/19.
//

#include <chrono>
#include <random>
#include <iostream>
#include <string>
#include "workload.h"

using namespace std;

GenericPointWorkload::GenericPointWorkload(
    rocksdb::DB *db,
    int initial_db_size,
    int key_space,
    int key_size,
    int value_size,
    double write_ratio,
    WriteType write_type)
    : db(db),
      initial_db_size(initial_db_size),
      key_space(key_space),
      key_size(key_size),
      value_size(value_size),
      write_ratio(write_ratio),
      write_type(write_type) {
  assert(key_space > 0);
  assert(initial_db_size >= 0);
  assert(write_ratio >= 0 && write_ratio <= 1);
  assert(key_size > 0 && value_size > 0);
}

void GenericPointWorkload::prepare_db() {
  default_random_engine random_engine;
  uniform_int_distribution<int> val_dist, key_dist(0, key_space);

  for (int i = 0; i < initial_db_size; ++i) {
    // construct random key and value
    int key_id = key_dist(random_engine);
    string key = from_key_id_to_str(key_id);
    string value = random_value();

    // insert to db
    rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, value);
    if (!s.ok()) {
      cerr << s.ToString() << endl;
      assert(0);
    }
  }
}

void GenericPointWorkload::do_query() {
  static default_random_engine random_engine;
  static uniform_int_distribution<int> key_dist(0, key_space);
  static uniform_real_distribution<double> choice_dist(0, 1);

  int key_id = key_dist(random_engine);
  string key = from_key_id_to_str(key_id);

  double choice = choice_dist(random_engine);
  if (choice < write_ratio) {  // write
    string value = random_value();

    if (write_type == BlindWrite) {
      rocksdb::Status s = db->Put(rocksdb::WriteOptions(), key, value);
      if (!s.ok()) {
        cerr << s.ToString() << endl;
        assert(0);
      }
    } else if (write_type == ReadModifyWrite) {
      // read first, even if the key does not exist
      rocksdb::PinnableSlice value;
      rocksdb::Status s = db->Get(rocksdb::ReadOptions(), db->DefaultColumnFamily(), key, &value);
      if (!s.ok() && !s.IsNotFound()) {
        cerr << s.ToString() << endl;
        assert(0);
      }
      value.Reset();

      // then write
      s = db->Put(rocksdb::WriteOptions(), key, value);
      if (!s.ok()) {
        cerr << s.ToString() << endl;
        assert(0);
      }
    } else {
      assert(0);
    }

  } else {
    // get from db
    rocksdb::PinnableSlice value;
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), db->DefaultColumnFamily(), key, &value);
    if (!s.ok() && !s.IsNotFound()) {
      cerr << s.ToString() << endl;
      assert(0);
    }
    value.Reset();
  }
}

string GenericPointWorkload::from_key_id_to_str(int key_i) {
  string key = to_string(key_i);
  key.resize(key_size, 'x');  // TODO: maybe hashing is better
  return key;
}

string GenericPointWorkload::random_value() {
  static default_random_engine random_engine;
  static uniform_int_distribution<int> uniform_dist;

  int value_off = 0;
  string value;
  while (value_off < value_size) {
    int random_int = uniform_dist(random_engine);
    int append_len = value_size - value_off > sizeof(random_int) ? sizeof(random_int) : value_size - value_off;
    value += string(reinterpret_cast<char *>(&random_int), append_len);
    value_off += append_len;
  }

  return value;
}

std::istream &operator>>(std::istream &is, Workload::WriteType &val) {
  string write_type_str;
  is >> write_type_str;

  if (write_type_str == "BlindWrite") {
    val = Workload::BlindWrite;
  } else if (write_type_str == "ReadModifyWrite") {
    val = Workload::ReadModifyWrite;
  } else {
    cerr << "wrong WriteType: " << write_type_str << endl;
  }

  return is;
}

std::ostream &operator<<(std::ostream &os, Workload::WriteType &val) {
  if (val == Workload::BlindWrite) {
    os << "BlindWrite";
  } else if (val == Workload::ReadModifyWrite) {
    os << "ReadModifyWrite";
  }

  return os;
}