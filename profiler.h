//===----------------------------------------------------------------------===//
//                         Compaction
//
// profiler.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <mutex>
#include <atomic>
#include <chrono>
#include <algorithm>

namespace compaction {
using std::mutex;
using std::lock_guard;
using std::atomic;
using std::chrono::time_point;
using std::chrono::system_clock;

//! The profiler can be used to measure elapsed time
template<typename T>
class BaseProfiler {
 public:
  //! Starts the timer
  void Start() {
    finished = false;
    start = Tick();
  }
  //! Finishes timing
  void End() {
    end = Tick();
    finished = true;
  }

  //! Returns the elapsed time in seconds. If End() has been called, returns
  //! the total elapsed time. Otherwise returns how far along the timer is
  //! right now.
  double Elapsed() const {
    auto _end = finished ? end : Tick();
    return std::chrono::duration_cast<std::chrono::duration<double>>(_end - start).count();
  }

 private:
  time_point<T> Tick() const {
    return T::now();
  }
  time_point<T> start;
  time_point<T> end;
  bool finished = false;
};

using Profiler = BaseProfiler<system_clock>;

class BeeProfiler {
 public:
  const static bool kEnableProfiling = 1;

 public:
  static BeeProfiler &Get() {
    static BeeProfiler instance;
    return instance;
  }

  void InsertStatRecord(string name, double value) {
    InsertStatRecord(name, size_t(value * 1e9));
  }

  inline void InsertStatRecord(string name, size_t value) {
    if (kEnableProfiling) {
      lock_guard<mutex> lock(mtx);
      values_[name] += value;
      calling_times_[name] += 1;
    }
  }

  void InsertHTRecord(string name, size_t tuple_sz, size_t point_table_sz, size_t num_terms) {
    if (kEnableProfiling) {
      std::lock_guard<std::mutex> lock(mtx);
      if (ht_records_.count(name) == 0) {
        ht_records_[name] = HTInfo(tuple_sz, point_table_sz, num_terms);
      }
    }
  }

  void EndProfiling() {
    if (kEnableProfiling) {
      PrintResults();
      Clear();
    }
  }

  void PrintResults() const {
    lock_guard<mutex> lock(mtx);

    // -------------------------------- Print Timing Results --------------------------------
    std::vector<std::string> keys;
    for (const auto &pair : values_) {
      keys.push_back(pair.first);
    }
    if (!keys.empty()) {
      std::sort(keys.begin(), keys.end());
      std::cerr << "-------\n";
      for (const auto &key : keys) {
        if (key.find("TableScan") != std::string::npos && key.find("in_mem") == std::string::npos) {
          continue;
        }
        if (key.find("#Tuple") != std::string::npos) {
          continue;
        }
        double time = values_.at(key) / double(1e9);
        size_t calling_times = calling_times_.at(key);
        double avg = time / calling_times;

        std::cerr << "Total: " << time << " s\tCalls: " << calling_times << "\tAvg: " << avg << " s\t" << key
                  << '\n';
      }
      std::cerr << "-------\n";
      for (const auto &key : keys) {
        if (key.find("#Tuple") != std::string::npos) {
          size_t total_tuples = values_.at(key);
          size_t calling_times = calling_times_.at(key);
          double avg = total_tuples / double(calling_times);

          std::cerr << "Total: " << total_tuples << "\tCalls: " << calling_times << "\tAvg: " << avg << "\t"
                    << key << '\n';
        }
      }
    }

    // -------------------------------- Print HT Results --------------------------------
    std::vector<string> ht_keys;
    for (const auto &pair : ht_records_) {
      ht_keys.push_back(pair.first);
    }
    if (!ht_keys.empty()) {
      std::cerr << "-------\n";
      std::sort(ht_keys.begin(), ht_keys.end());
      for (const auto &key : ht_keys) {
        auto ht_info = ht_records_.at(key);

        std::cerr << "Tuples Size: " << (double) ht_info.tuple_size / (1 << 20) << " MB\t"
                  << "Point Size: " << (double) ht_info.point_table_size / (1 << 20) << " MB\t"
                  << "#Term: " << ht_info.num_terms << "\t" << key << '\n';
      }
    }
  }

  void Clear() {
    std::lock_guard<std::mutex> lock(mtx);

    values_.clear();
    calling_times_.clear();
    ht_records_.clear();
  }

 private:
  struct HTInfo {
    size_t tuple_size;
    size_t point_table_size;
    size_t num_terms;

    HTInfo(size_t ts = 0, size_t pts = 0, size_t nt = 0) : tuple_size(ts), point_table_size(pts), num_terms(nt) {
    }
  };

  unordered_map<string, size_t> values_;
  unordered_map<string, size_t> calling_times_;
  unordered_map<string, HTInfo> ht_records_;
  mutable std::mutex mtx;
};

class ZebraProfiler {
 public:
  const static bool kEnableProfiling = 0;

 public:
  static ZebraProfiler &Get() {
    static ZebraProfiler instance;
    return instance;
  }

  inline void InsertRecord(string name, size_t key, double value) {
    InsertRecord(std::move(name), key, size_t(value * 1e9));
  }

  inline void InsertRecord(string name, size_t key, size_t value) {
    if (kEnableProfiling) {
      assert(key <= kBlockSize);

      if (hists_.count(name) == 0) {
        lock_guard<std::mutex> lock(mutex_);
        hists_[name] = Histogram();
      }

      // insert record into the histogram of [name]
      auto &hist = hists_[name];
      hist.values_[key] += value;
      hist.cnt_[key] += 1;
    }
  }

  inline void PrintResults() const {
    if (kEnableProfiling) {
      for (auto &pair : hists_) {
        const auto &name = pair.first;
        const auto &hist = pair.second;

        std::cerr << "-------\n";
        std::cerr << name << '\n';
        for (size_t i = 1; i <= kBlockSize; ++i) {
          if (hist.cnt_[i] > 0) {
            std::cerr << i << ": " << hist.values_[i] / double(1e3) / hist.cnt_[i] << " us\t"
                      << hist.cnt_[i] << '\n';
          }
        }
      }
    }
  }

  // output to csv file, each hist has a file.
  inline void ToCSV() {
    if (kEnableProfiling) {
      std::string folder_name = "./zebra_log_0x" + std::to_string(RandomInteger());
      std::filesystem::create_directories(folder_name);
      for (auto &pair : hists_) {
        const auto &name = pair.first;
        const auto &hist = pair.second;

        // filter these filters
        if (name.find("FILTER") != string::npos) {
          continue;
        }

        string file_name = folder_name + "/" + name + ".csv";
        std::ofstream out(file_name);
        out << "key, value, cnt\n";
        for (size_t i = 1; i <= kBlockSize; ++i) {
          if (hist.cnt_[i] == 0) continue;
          out << i << "," << hist.values_[i] / double(1e3) / hist.cnt_[i] << "," << hist.cnt_[i] << "\n";
        }
        out.close();
      }
    }
  }

  inline void Clear() {
    hists_.clear();
  }

 private:
  ZebraProfiler() : gen_(rd()) {
  }

  inline size_t RandomInteger() {
    return integers(gen_);
  }

  struct Histogram {
    vector<atomic<size_t>> values_;  // in ns
    vector<atomic<size_t>> cnt_;

    Histogram() : values_(kBlockSize + 1), cnt_(kBlockSize + 1) {
    }
  };

  mutex mutex_;
  unordered_map<string, Histogram> hists_;

  // random
  std::random_device rd;
  std::mt19937 gen_;
  std::uniform_int_distribution<int> integers;
};
}  // namespace duckdb

