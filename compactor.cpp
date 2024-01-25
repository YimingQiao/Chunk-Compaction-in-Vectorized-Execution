#include "compactor.h"
#include "profiler.h"

namespace compaction {
void NaiveCompactor::Compact(unique_ptr<DataChunk> &chunk) {
  if (chunk->count_ == kBlockSize) return;

  Profiler profiler;
  profiler.Start();
  {
    // move
    if (chunk->count_ <= kBlockSize - cached_chunk_->count_) {
      cached_chunk_->Append(*chunk, chunk->count_);

      double time = profiler.Elapsed();
      BeeProfiler::Get().InsertStatRecord("[Naive Compact - Append] " + name_, time);
      ZebraProfiler::Get().InsertRecord("[Naive Compact - Append] " + name_, chunk->count_, time);
      chunk->Reset();
      return;
    }

    size_t n_move = kBlockSize - cached_chunk_->count_;
    cached_chunk_->Append(*chunk, n_move);
    temp_chunk_->Append(*chunk, chunk->count_ - n_move, n_move);
  }
  double time = profiler.Elapsed();
  BeeProfiler::Get().InsertStatRecord("[Naive Compact - Append] " + name_, time);
  ZebraProfiler::Get().InsertRecord("[Naive Compact - Append] " + name_, chunk->count_, time);

  // profiler.Start();
  {
    // swap
    chunk.swap(cached_chunk_);
    cached_chunk_.swap(temp_chunk_);
    temp_chunk_->Reset();
    // temp_chunk_ = std::make_unique<DataChunk>(chunk->types_);
  }
  time = profiler.Elapsed();
  BeeProfiler::Get().InsertStatRecord("[Naive Compact - Fetch] " + name_, time);
  ZebraProfiler::Get().InsertRecord("[Naive Compact - Fetch] " + name_, chunk->count_, time);
}
}
