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
  if (!free_list_.empty()) {
    auto frame_id = *free_list_.begin();
    free_list_.erase(free_list_.begin());
    *page_id = AllocatePage();
    page_table_->Insert(*page_id, frame_id);

    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);

    pages_[frame_id].ResetMemory();

    pages_[frame_id].page_id_ = *page_id;
    pins[*page_id]++;
    pages_exist.insert(*page_id);

    LOG_DEBUG("New page:%d, frame:%d", *page_id, frame_id);
    return &pages_[frame_id];
  } else {
    frame_id_t frame_id;
    if (replacer_->Evict(&frame_id)) {
      auto old_page_id = pages_[frame_id].GetPageId();
      
      LOG_DEBUG("page erase from page_exist %d", old_page_id);
      *page_id = AllocatePage();

      page_table_->Insert(*page_id, frame_id);

      replacer_->SetEvictable(frame_id, false);
      replacer_->RecordAccess(frame_id);

      if (is_dirty_page[old_page_id] == true || pages_[frame_id].IsDirty()) {
        LOG_DEBUG("Flush flush flush %d", old_page_id);
        FlushPgImp(old_page_id);
      }
      pages_exist.erase(old_page_id);
      pages_[frame_id].ResetMemory();
      pages_[frame_id].page_id_ = *page_id;
      pins[*page_id]++;
      pages_exist.insert(*page_id);

      LOG_DEBUG("New page:%d, frame:%d", *page_id, frame_id);
      return &pages_[frame_id];
    } else {
      return nullptr;
    }
  }
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  LOG_DEBUG("Fetch page %d", page_id);
  frame_id_t frame_id;
  if (pages_exist.count(page_id) == 1) {
    LOG_DEBUG("page exist %d", page_id);
    page_table_->Find(page_id, frame_id);
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);
    return &pages_[frame_id];
  } else {
    if (!free_list_.empty() || replacer_->Evict(&frame_id)) {
      if (!free_list_.empty()) {
        frame_id = *free_list_.begin();
        free_list_.erase(free_list_.begin());
      } else {
        auto old_page_id = pages_[frame_id].GetPageId();
        if (is_dirty_page[old_page_id] == true) {
          FlushPgImp(old_page_id);
        }
        pages_exist.erase(old_page_id);
        pages_[frame_id].ResetMemory();
        pages_[frame_id].page_id_ = page_id;
      }
      page_table_->Insert(page_id, frame_id);

      replacer_->SetEvictable(frame_id, false);
      replacer_->RecordAccess(frame_id);
      disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
      LOG_DEBUG("data in page %d %s",page_id, pages_[frame_id].GetData());
      pins[page_id]++;
      pages_exist.insert(page_id);
      return &pages_[frame_id];

    } else {
      return nullptr;
    }
  }
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  if (pages_exist.count(page_id) == 0 || pins[page_id] == 0) {
    return false;
  }
  is_dirty_page[page_id] = is_dirty;
  pins[page_id]--;
  if (pins[page_id] == 0) {
    frame_id_t frame_id;
    page_table_->Find(page_id, frame_id);
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  page_table_->Find(page_id, frame_id);
  if(pages_exist.count(page_id) == 0)
    return false;
  LOG_DEBUG("Flush page:%d, frame:%d", page_id, frame_id);
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  is_dirty_page[page_id] = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  int n = pool_size_;
  for (int i = 0; i < n; i++) {
    auto page_id = static_cast<page_id_t>(i) ;
    if (pages_exist.count(page_id) != 0) {
      frame_id_t frame_id;
      page_table_->Find(page_id, frame_id);
      disk_manager_->WritePage(page_id, pages_[page_id].GetData());
      is_dirty_page[page_id] = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  if (pages_exist.count(page_id) != 0) {
    if (pins[page_id] != 0) return false;
    frame_id_t frame_id;
    page_table_->Find(page_id, frame_id);
    pages_exist.erase(page_id);

    is_dirty_page.erase(page_id);
    free_list_.emplace_back(static_cast<int>(page_id));

    pages_[frame_id].ResetMemory();
    replacer_->Remove(frame_id);

    DeallocatePage(page_id);
  }
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
