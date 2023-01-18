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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    bool succ = false;
    int t = this->current_timestamp_ + 1;
    int lastTime = this->current_timestamp_ + 1;
    for(auto p:this->evictable){
        if(p.second == true){
            frame_id_t cur = p.first;
            auto cur_times = this->times[cur];
            if(cur_times.size() == 0)
                continue;
            if(cur_times.size() < this->k_ ){
                if(t != -1){
                    t = -1;
                    lastTime = cur_times.back();
                    *frame_id = cur;
                    succ = true;
                }else{
                    if(lastTime > cur_times.back()){
                        lastTime = cur_times.back();
                        *frame_id = cur;
                        succ = true;
                    }
                }
            }else{
                int cur_time = cur_times[cur_times.size()-this->k_];
                if(cur_time < t){
                    t = cur_time;
                    *frame_id = cur;
                    succ = true;
                }
            }
        }
    }
    if(succ){
        this->curr_size_--;
        this->evictable.erase(*frame_id);
        this->times.erase(*frame_id);
    }
    return succ;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    BUSTUB_ASSERT(frame_id <= this->replacer_size_, "Frame id is invalid");
    this->times[frame_id].push_back(this->current_timestamp_++);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    BUSTUB_ASSERT(frame_id <= this->replacer_size_, "Frame id is invalid");
    if(this->evictable[frame_id] !=set_evictable){
        this->evictable[frame_id] = set_evictable;
        if(set_evictable == true){
            this->curr_size_++;
        }else{
            this->curr_size_--;
        }
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    BUSTUB_ASSERT(frame_id <= this->replacer_size_, "Frame id is invalid");
    BUSTUB_ASSERT(this->evictable[frame_id] == true, "This frame is non-evivtable");
    this->curr_size_--;
    this->evictable.erase(frame_id);
    this->times.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { 
    return this->curr_size_;
}

}  // namespace bustub
