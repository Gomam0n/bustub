//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  if (!free_list_.empty()) {  // there exists unused frame
    auto frame_id = *free_list_.begin();
    free_list_.erase(free_list_.begin());
    *page_id = AllocatePage();
    page_table_->Insert(*page_id, frame_id);

    RecordInReplacer(frame_id);

    pages_[frame_id].ResetMemory();

    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].pin_count_++;
    pages_exist.insert(*page_id);

    //LOG_DEBUG("New page:%d, frame:%d", *page_id, frame_id);
    return &pages_[frame_id];
  } else {  // there exists evictable frame
    frame_id_t frame_id;
    if (replacer_->Evict(&frame_id)) {
      auto old_page_id = pages_[frame_id].GetPageId();
      
      //LOG_DEBUG("page erase from page_exist %d", old_page_id);
      *page_id = AllocatePage();

      page_table_->Insert(*page_id, frame_id);

      RecordInReplacer(frame_id);

      if (pages_[frame_id].IsDirty()) {
        //LOG_DEBUG("Flush flush flush %d", old_page_id);
        FlushPgImp(old_page_id);
      }
      pages_exist.erase(old_page_id);
      pages_[frame_id].ResetMemory();
      pages_[frame_id].page_id_ = *page_id;
      pages_[frame_id].pin_count_++;
      pages_exist.insert(*page_id);

      LOG_DEBUG("New page:%d, frame:%d", *page_id, frame_id);
      return &pages_[frame_id];
    } else {
      return nullptr;
    }
  }
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  //LOG_DEBUG("Fetch page %d", page_id);
  frame_id_t frame_id;
  if (pages_exist.count(page_id) == 1) {  // if the page is in buffer pool
    //LOG_DEBUG("page exist %d", page_id);
    page_table_->Find(page_id, frame_id);
    RecordInReplacer(frame_id);
    return &pages_[frame_id];
  } else {
    if (!free_list_.empty() || replacer_->Evict(&frame_id)) { // the page is not in buffer pool but we can insert it into buffer pool
      if (!free_list_.empty()) {  // there exists unused frame
        frame_id = *free_list_.begin();
        free_list_.erase(free_list_.begin());
      } else {  // there exists evictable frame
        auto old_page_id = pages_[frame_id].GetPageId();
        if (pages_[frame_id].IsDirty()) {
          FlushPgImp(old_page_id);
        }
        pages_exist.erase(old_page_id);
        pages_[frame_id].ResetMemory();
        pages_[frame_id].page_id_ = page_id;
      }
      page_table_->Insert(page_id, frame_id);
      RecordInReplacer(frame_id);
      disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
      //LOG_DEBUG("data in page %d %s",page_id, pages_[frame_id].GetData());
      pages_[frame_id].pin_count_++;
      pages_exist.insert(page_id);
      return &pages_[frame_id];
    } else {
      return nullptr;
    }
  }
}

auto RecordInReplacer(frame_id_t frame_id) -> void{
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  latch_.lock();
  frame_id_t frame_id;
  page_table_->Find(page_id, frame_id);
  if (pages_exist.count(page_id) == 0 || frame_id < 0 || frame_id >= pool_size_ || pages_[frame_id].GetPinCount() ==0) {
    latch_.unlock();
    return false;
  }
  pages_[frame_id].is_dirty_ = is_dirty;
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].GetPinCount() ==0 == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  latch_.lock();
  frame_id_t frame_id;
  page_table_->Find(page_id, frame_id);
  if(pages_exist.count(page_id) == 0 || frame_id < 0 || frame_id >= pool_size_){
    latch_.unlock();
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  latch_.unlock();
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  latch_.lock();
  int n = pool_size_;
  for (int i = 0; i < n; i++) {
    if(pages_[i].IsDirty()){
      disk_manager_->WritePage(page_id, pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
  latch_.unlock();
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  latch_.lock();
  if (pages_exist.count(page_id) != 0) {
    frame_id_t frame_id;
    page_table_->Find(page_id, frame_id);
    if (pages_[frame_id].GetPinCount() != 0){
      latch_.unlock();
      return false;
    }
    // erase from pages_exist, add frame id to free list, remove from replacer and reset the memory.
    pages_exist.erase(page_id);
    free_list_.emplace_back(static_cast<int>(frame_id));

    pages_[frame_id].ResetMemory();
    replacer_->Remove(frame_id);

    DeallocatePage(page_id);
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
