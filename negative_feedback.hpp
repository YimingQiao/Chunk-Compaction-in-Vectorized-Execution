//===----------------------------------------------------------------------===//
//                         DuckDB
//
// negative_feedback.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cmath>
#include <filesystem>
#include <random>

#include "base.h"

namespace compaction {

// I use UCB1 (https://cse442-17f.github.io/LinUCB/) to select the best
class MultiArmedBandit {
 public:
  MultiArmedBandit(size_t n_arms, const std::vector<double> &means)
      : kArms_(n_arms),
        est_rewards_(means),
        est_square_rewards_(n_arms, 0),
        n_select_(n_arms, 0),
        select_times_(0),
        stage_update_times_(0),
        stage_n_update_(n_arms, 0),
        n_start_sampling_(0) {
  }

  // Selects an arm based on the UCB1 algorithm
  inline size_t SelectArm() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (n_start_sampling_ < kArms_ * kStartSampling) {
      // initialize experimental means by pulling each arm once
      size_t arm = n_start_sampling_ % kArms_;

      n_start_sampling_++;
      select_times_++;
      n_select_[arm]++;
      return arm;
    }

    // select the arm with the highest estimated_mean + UCB value
    double max_value = -1;
    size_t max_arm = 0;
    for (size_t i = 0; i < kArms_; i++) {
      double value = est_rewards_[i] + UCBTuned(i);

      if (value > max_value) {
        max_value = value;
        max_arm = i;
      }
    }
    select_times_++;
    n_select_[max_arm]++;
    return max_arm;
  }

  // Updates the arm with the given weight
  inline void UpdateArm(size_t arm, double reward) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (select_times_ % kHeart == 0 && n_start_sampling_ >= kArms_ * kStartSampling) {
      history_.emplace_back(est_rewards_, n_select_);

      if (r_means_.empty()) r_means_ = est_rewards_;
      bool detected = est_rewards_[arm] > r_means_[arm] * 2 || est_rewards_[arm] < r_means_[arm] / 2;
      r_means_ = est_rewards_;
      if (detected) {
        n_start_sampling_ = 0;
        std::fill_n(est_rewards_.begin(), kArms_, 0);
        std::fill_n(est_square_rewards_.begin(), kArms_, 0);

        stage_update_times_ = 0;
        std::fill_n(stage_n_update_.begin(), kArms_, 0);
      }
    }

    // update rewards
    size_t update_factor = std::min(stage_n_update_[arm], size_t(15));
    double ratio = update_factor / (update_factor + 1.0);
    est_rewards_[arm] = est_rewards_[arm] * ratio + reward * (1 - ratio);
    est_square_rewards_[arm] = est_square_rewards_[arm] * ratio + reward * reward * (1 - ratio);
    stage_update_times_++;
    stage_n_update_[arm]++;
  }

  inline void Print(const std::vector<size_t> &values) {
    for (size_t i = 0; i < est_rewards_.size(); i++) {
      std::cerr << " [PARAMETERS] Estimated reward for arm " << values[i] << " is " << std::to_string(est_rewards_[i])
                << " - Sampling times is " << n_select_[i] << "\n";
    }
  }

  inline void Log2Csv(std::string addr) {
    std::ofstream file(addr);

    // Check if file is open
    if (!file.is_open()) {
      throw std::runtime_error("Unable to open file");
    }

    // Iterating over history to write each record
    for (size_t i = 0; i < history_.size(); ++i) {
      const auto &record = history_[i];
      file << i * kHeart << ", ";
      for (size_t j = 0; j < record.his_rewards_.size(); ++j)
        file << record.his_rewards_[j] << ", ";

      for (size_t j = 0; j < record.his_select_.size(); ++j)
        file << record.his_select_[j] << ", ";
      file << "\n";
    }

    file.close();
  }

 private:
  inline double UCBTuned(size_t arm) {
    double ucb_var = est_square_rewards_[arm] - est_rewards_[arm] * est_rewards_[arm] +
        sqrt(2 * log(stage_update_times_) / (stage_n_update_[arm] + kEpsilon));
    return sqrt(log(stage_update_times_) / (stage_n_update_[arm] + kEpsilon) * std::min(0.25, ucb_var));
  }

 private:
  // init
  size_t kArms_;
  double kEpsilon = 0.1;
  size_t kStartSampling = 4;

 private:
  // stats
  size_t select_times_;
  std::vector<size_t> n_select_;

 private:
  // UCB-tuned
  std::mutex mutex_;
  std::vector<double> est_rewards_;
  std::vector<double> est_square_rewards_;
  size_t stage_update_times_;
  std::vector<size_t> stage_n_update_;

  // restart
  size_t n_start_sampling_ = 0;
  std::vector<double> r_means_;

 private:
  // logging
  size_t kHeart = 256;
  struct Record {
    std::vector<double> his_rewards_;
    std::vector<size_t> his_select_;

    Record(const std::vector<double> &rewards, const std::vector<size_t> selects)
        : his_rewards_(rewards), his_select_(selects) {};
  };
  std::vector<Record> history_;
};

class CompactTuner {
 public:
  static CompactTuner &Get() {
    static CompactTuner instance;
    return instance;
  }

  inline void Initialize(size_t address, const std::vector<size_t> &arms = {0, 32, 64, 128, 256, 384, 512, 768, 1024}) {
    assert(package_index_.count(address) == 0);

    package_index_[address] = bandit_packages_.size();
    bandit_packages_.emplace_back(arms, std::vector<double>(arms.size(), 0));
  }

  // Selects an arm based on the UCB1 algorithm
  inline size_t SelectArm(idx_t id) {
    auto &bandit = bandit_packages_[id].bandit;
    auto &value = bandit_packages_[id].value;

    size_t ret = value[bandit->SelectArm()];
    return ret;
  }

  // Updates the arm with the given weight
  inline void UpdateArm(idx_t id, size_t arm, double reward) {
    auto &bandit = bandit_packages_[id].bandit;
    auto &value_index = bandit_packages_[id].value_index;

    if (value_index.count(arm) == 0) return;
    bandit->UpdateArm(value_index[arm], reward);
  }

  inline void Reset(bool enable_log = false) {
    if (!bandit_packages_.empty() && enable_log) {
      // output the parameters
      std::cerr << "-------\n";

      std::string folder_name = "./bandit_log_0x" + std::to_string(RandomInteger());
      std::filesystem::create_directories(folder_name);
      for (auto &pair : package_index_) {
        auto &addr = pair.first;
        auto &id = pair.second;

        auto &bandit = bandit_packages_[id].bandit;
        auto &value = bandit_packages_[id].value;

        std::string bandit_name = "0x" + std::to_string(addr) + "\tId-" + std::to_string(id);
        std::cerr << " [PARAMETERS] Compaction Address - " << bandit_name << "\n";
        bandit->Log2Csv("./" + folder_name + "/" + bandit_name + ".log");
        bandit->Print(value);
      }

      package_index_.clear();
      bandit_packages_.clear();
    }
  }

  inline int64_t GetId(size_t address) {
    if (package_index_.count(address) == 0) {
      // not found
      return -1;
    }

    return package_index_[address];
  }

  inline size_t GetBanditSize() {
    return bandit_packages_.size();
  }

 private:
  inline size_t RandomInteger() {
    return integers(gen_);
  }

  struct BanditPackage {
    std::unique_ptr<MultiArmedBandit> bandit;
    std::vector<size_t> value;
    std::unordered_map<size_t, idx_t> value_index;

    BanditPackage(const std::vector<size_t> &arms, const std::vector<double> &means) {
      bandit = std::make_unique<MultiArmedBandit>(arms.size(), means);
      value = arms;
      for (size_t i = 0; i < arms.size(); i++) {
        value_index[arms[i]] = i;
      }
    }
  };

  std::unordered_map<size_t, idx_t> package_index_;
  std::vector<BanditPackage> bandit_packages_;

  // random
  std::mt19937 gen_;
  std::uniform_int_distribution<int> integers;
};
}  // namespace duckdb
