#include "compactor.h"

namespace compaction {
void NaiveCompactor::Compact(unique_ptr<DataChunk> &chunk) {
  if (chunk->count_ == kBlockSize) return;

  profiler_.Start();

  // move
  if (chunk->count_ <= kBlockSize - cached_chunk_->count_) {
    cached_chunk_->Append(*chunk, chunk->count_);

    double time = profiler_.Elapsed();
    BeeProfiler::Get().InsertStatRecord(name_, time);
    ZebraProfiler::Get().InsertRecord(name_, chunk->count_, time);
    chunk->count_ = 0;
    return;
  }

  size_t n_move = kBlockSize - cached_chunk_->count_;
  cached_chunk_->Append(*chunk, n_move);
  temp_chunk_->Append(*chunk, chunk->count_ - n_move, n_move);

  profiler_.Start();
  // swap
  chunk.swap(cached_chunk_);
  cached_chunk_.swap(temp_chunk_);
  // temp_chunk_->Reset();
  temp_chunk_ = std::make_unique<DataChunk>(chunk->types_);

  double time = profiler_.Elapsed();
  BeeProfiler::Get().InsertStatRecord(name_, time);
  ZebraProfiler::Get().InsertRecord(name_, chunk->count_, time);
}

void BinaryCompactor::Compact(unique_ptr<DataChunk> &chunk) {
  if (chunk->count_ >= kCompactThreshold) return;

  // append
  profiler_.Start();
  cached_chunk_->Append(*chunk, chunk->count_);
  chunk->count_ = 0;
  if (cached_chunk_->count_ > kBlockSize - kCompactThreshold) {
    chunk = std::move(cached_chunk_);
    cached_chunk_ = std::make_unique<DataChunk>(chunk->types_);
  }
  double time = profiler_.Elapsed();
  BeeProfiler::Get().InsertStatRecord(name_, time);
  ZebraProfiler::Get().InsertRecord(name_, chunk->count_, time);
}

void DynamicCompactor::Compact(unique_ptr<DataChunk> &chunk) {
  if (chunk->count_ >= compact_threshold_) return;

  // Because the compaction threshold can be changed during execution, we must add this code.
  if (chunk->count_ > kBlockSize - cached_chunk_->count_) return;

  profiler_.Start();
  cached_chunk_->Append(*chunk, chunk->count_);
  chunk->count_ = 0;

  if (cached_chunk_->count_ > kBlockSize - compact_threshold_) {
    chunk = std::move(cached_chunk_);
    cached_chunk_ = std::make_unique<DataChunk>(chunk->types_);
  }
  double time = profiler_.Elapsed();
  BeeProfiler::Get().InsertStatRecord(name_, time);
  ZebraProfiler::Get().InsertRecord(name_, chunk->count_, time);
}
}
