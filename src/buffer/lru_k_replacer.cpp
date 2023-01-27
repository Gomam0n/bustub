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
  latch_.lock();
  // LOG_DEBUG("Total size is %d", static_cast<int>(num_frames));
  for (int i = 1; i <= static_cast<int>(num_frames); i++) {
    frames_.insert(i);
    evictable_[i] = false;
  }
  latch_.unlock();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  bool succ = false;
  int t = this->current_timestamp_ + 1;
  int last_time = this->current_timestamp_ + 1;  // use for LRU
  for (auto p : this->evictable_) {
    if (p.second) {
      frame_id_t cur = p.first;
      auto cur_times = this->times_[cur];
      if (cur_times.empty()) {
        continue;
      }
      if (cur_times.size() < this->k_) {  // less than k records, use LRU
        if (t != -1) {                    // -1 denotes inf
          t = -1;
          last_time = cur_times[0];
          *frame_id = cur;
          succ = true;
        } else {
          if (last_time > cur_times[0]) {
            last_time = cur_times[0];
            *frame_id = cur;
            succ = true;
          }
        }
      } else {  // more or equal to k records, use LRU-k
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
    // LOG_DEBUG("Evict %d successfully", static_cast<int>(*frame_id));
    this->curr_size_--;
    this->evictable_.erase(*frame_id);
    this->times_.erase(*frame_id);
    this->frames_.erase(*frame_id);
  }
  latch_.unlock();
  return succ;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id <= static_cast<int>(this->replacer_size_), "Frame id is invalid");
  latch_.lock();
  frames_.insert(frame_id);
  // LOG_DEBUG("Record access %d", static_cast<int>(frame_id));
  this->times_[frame_id].push_back(this->current_timestamp_++);
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id <= static_cast<int>(this->replacer_size_), "Frame id is invalid");
  latch_.lock();
  if (frames_.count(frame_id) == 0) {
    latch_.unlock();
    return;
  }
  // LOG_DEBUG("Set %d as %d", static_cast<int>(frame_id), set_evictable);
  if (this->evictable_[frame_id] != set_evictable) {
    this->evictable_[frame_id] = set_evictable;
    if (set_evictable) {
      this->curr_size_++;
    } else {
      this->curr_size_--;
    }
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (frames_.count(frame_id) == 0 || !this->evictable_[frame_id]) {
    latch_.unlock();
    return;
  }

  // LOG_DEBUG("Try remove %d", static_cast<int>(frame_id));
  BUSTUB_ASSERT(frame_id <= static_cast<int>(this->replacer_size_), "Frame id is invalid");

  this->curr_size_--;
  this->evictable_.erase(frame_id);
  this->times_.erase(frame_id);
  frames_.erase(frame_id);
  // LOG_DEBUG("Remove %d successfully", static_cast<int>(frame_id));
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  // LOG_DEBUG("The size is %d", static_cast<int>(curr_size_));
  return this->curr_size_;
}

}  // namespace bustub
