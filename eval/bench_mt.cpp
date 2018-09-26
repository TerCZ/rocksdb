#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

#include "workload.h"
#include "util.h"

using namespace std;

atomic<int> compaction_num(0);
atomic<int> flush_num(0);

template<class Clock, class Duration = typename Clock::duration>
double steady_clock_duration_since(chrono::time_point<Clock, Duration> &start) {
  auto end = chrono::steady_clock::now();
  double duration = chrono::duration_cast<chrono::duration<double >>(end - start).count();
  return duration;
}

/*
 * client_work
 */
void client_work(shared_ptr<Workload> workload,
                 const Config config,
                 atomic<int> &op_count,
                 vector<double> &latencies,
                 mutex &latency_mutex,
                 atomic<bool> &stop) {
  while (!stop) {
    // run ops_between_latency_sample ops
    for (int i = 0; i < config.ops_between_latency_sample; ++i) {
      workload->do_query();
      ++op_count;
    }

    // measure latency once
    auto query_start = chrono::steady_clock::now();
    workload->do_query();
    double duration = steady_clock_duration_since(query_start);
    ++op_count;

    // only record when lock can be held, in order not to influence performance
    if (latency_mutex.try_lock()) {
      latencies.push_back(duration);
      latency_mutex.unlock();
    }
  }
}

/*
 * manager_work
 */
void manager_work(const Config config,
                  vector<atomic<int>> &op_counts,
                  vector<vector<double>> &client_latencies,
                  vector<mutex> &latency_mutexes,
                  atomic<bool> &stop) {
  // start timing
  auto start = chrono::steady_clock::now();

  // open output file
  ofstream outfile(config.result_path + "/result", ofstream::app);

  // output header
  output_header(outfile);

  double duration = 0;
  while (duration < config.benchmark_duration) {
    this_thread::sleep_for(std::chrono::seconds(config.seconds_between_manager_pulling));
    double timedelta = steady_clock_duration_since(start) - duration;
    if (timedelta <= 0) {
      continue;
    }
    duration += timedelta;

    // get op_count and latencies from client threads
    int op_count = 0;
    double latency_mean, latency_95, latency_99;
    vector<double> latencies;

    for (int i = 0; i < config.client_thread_n; ++i) {
      // copy and clear client op_count
      op_count += op_counts[i];
      op_counts[i] = 0;

      // copy and clear client latencies
      latency_mutexes[i].lock();
      latencies.insert(latencies.end(), client_latencies[i].begin(), client_latencies[i].end());
      client_latencies[i].clear();
      latency_mutexes[i].unlock();
    }

    if (latencies.size() != 0) {
      process_latencies(latencies, latency_mean, latency_95, latency_99);
      StatEntry entry(duration, op_count / timedelta, latency_mean, latency_95, latency_99, compaction_num, flush_num);
      output_entry(outfile, config, entry);
    } else {
      StatEntry entry(duration, op_count / timedelta, 0, 0, 0, compaction_num, flush_num);
      output_entry(outfile, config, entry);
    }
  }

  stop = true;
}

/*
 * run_config
 */
void run_config(const Config &config) {
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
    cerr << s.ToString() << endl;
    assert(0);
  }

  // get workload and prepare db
  shared_ptr<Workload> workload(new GenericPointWorkload(db,
                                                         config.initial_db_size,
                                                         config.key_space,
                                                         config.key_size,
                                                         config.value_size,
                                                         config.write_ratio,
                                                         config.write_type));
  workload->prepare_db();

  // create client threads
  vector<thread> clients;
  vector<atomic<int>> op_counts(config.client_thread_n);  // uninitialized here, later init them in loop
  vector<vector<double>> client_latencies(config.client_thread_n);
  vector<mutex> latency_mutexes(config.client_thread_n);
  atomic<bool> stop(false);

  // TODO: don't know if these ref is a must
  for (int i = 0; i < config.client_thread_n; ++i) {
    atomic_init(&op_counts[i], 0);
    clients.push_back(
        thread(client_work,
               ref(workload),
               config,
               ref(op_counts[i]),
               ref(client_latencies[i]),
               ref(latency_mutexes[i]),
               ref(stop)));
  }
  thread manager(manager_work, config, ref(op_counts), ref(client_latencies), ref(latency_mutexes), ref(stop));

  // wait for them to finish
  manager.join();
  for (thread &t: clients) {
    t.join();
  }

  // close db
  delete db;
}

/*
 * main
 */
int main(int argc, char **argv) {
  // parse config
  vector<Config> configs = parse_config(argc, argv);
  assert(configs.size() > 0);

  // run each config
  for (const Config &config: configs) {
    for (int i = 0; i < config.iter_num; ++i) {
      run_config(config);
    }
  }
}