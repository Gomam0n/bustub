//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  for(int i = 1; i <= (int)num_frames; i++){
    frames.insert(i);
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  bool succ = false;
  int t = this->current_timestamp_ + 1;
  int lastTime = this->current_timestamp_ + 1;
  for (auto p : this->evictable) {
    if (p.second == true) {
      frame_id_t cur = p.first;
      auto cur_times = this->times[cur];
      if (cur_times.size() == 0) continue;
      if (cur_times.size() < this->k_) {
        if (t != -1) {
          t = -1;
          lastTime = cur_times.back();
          *frame_id = cur;
          succ = true;
        } else {
          if (lastTime > cur_times.back()) {
            lastTime = cur_times.back();
            *frame_id = cur;
            succ = true;
          }
        }
      } else {
        int cur_time = cur_times[cur_times.size() - this->k_];
        if (cur_time < t) {
          t = cur_time;
          *frame_id = cur;
          succ = true;
        }
      }
    }
  }
  if (succ) {
    this->curr_size_--;
    this->evictable.erase(*frame_id);
    this->times.erase(*frame_id);
    this->frames.erase(*frame_id);
  }
  latch_.unlock();
  return succ;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id <= static_cast<int>(this->replacer_size_), "Frame id is invalid");
  latch_.lock();
  this->times[frame_id].push_back(this->current_timestamp_++);
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id <= static_cast<int>(this->replacer_size_), "Frame id is invalid");
  latch_.lock();
  if (this->evictable[frame_id] != set_evictable) {
    this->evictable[frame_id] = set_evictable;
    if (set_evictable == true) {
      this->curr_size_++;
    } else {
      this->curr_size_--;
    }
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if(frames.count(frame_id) == 0){
    return;
  }
  BUSTUB_ASSERT(frame_id <= static_cast<int>(this->replacer_size_), "Frame id is invalid");
  BUSTUB_ASSERT(this->evictable[frame_id] == true, "This frame is non-evivtable");
  latch_.lock();
  this->curr_size_--;
  this->evictable.erase(frame_id);
  this->times.erase(frame_id);
  frames.erase(frame_id);
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { 
  LOG_DEBUG("The size is %lu", curr_size_);
  return this->curr_size_; }

}  // namespace bustub
