#include "compactor.h"
#include "profiler.h"

namespace compaction {
void NaiveCompactor::Compact(unique_ptr<DataChunk> &chunk) {
  if (chunk->count_ == kBlockSize) return;

  const string name = "0x" + std::to_string(size_t(this));
  Profiler profiler;
  profiler.Start();
  {
    // move
    if (chunk->count_ <= kBlockSize - cached_chunk_->count_) {
      cached_chunk_->Append(*chunk, chunk->count_);

      double time = profiler.Elapsed();
      BeeProfiler::Get().InsertStatRecord("[Naive Compact - Append] " + name, time);
      ZebraProfiler::Get().InsertRecord("[Naive Compact - Append] " + name, chunk->count_, time);
      chunk->count_ = 0;
      return;
    }

    size_t n_move = kBlockSize - cached_chunk_->count_;
    cached_chunk_->Append(*chunk, n_move);
    temp_chunk_->Append(*chunk, chunk->count_ - n_move, n_move);
  }
  double time = profiler.Elapsed();
  BeeProfiler::Get().InsertStatRecord("[Naive Compact - Append] " + name, time);
  ZebraProfiler::Get().InsertRecord("[Naive Compact - Append] " + name, chunk->count_, time);

  profiler.Start();
  {
    // swap
    chunk = std::move(cached_chunk_);
    cached_chunk_ = std::move(temp_chunk_);
    temp_chunk_ = std::make_unique<DataChunk>(chunk->types_);
  }
  time = profiler.Elapsed();
  BeeProfiler::Get().InsertStatRecord("[Naive Compact - Fetch] " + name, time);
  ZebraProfiler::Get().InsertRecord("[Naive Compact - Fetch] " + name, chunk->count_, time);
}
}
