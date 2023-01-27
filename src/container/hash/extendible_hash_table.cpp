//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/extendible_hash_table.h"
#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>
#include "common/logger.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  // LOG_DEBUG("Constructor, bucket size:%lu", bucket_size_);
  auto bucket = Bucket(bucket_size,
                       0);  // *************Use object instead of pointer can avoid memory leak. Why?******************
  this->dir_.emplace_back(std::make_shared<Bucket>(bucket));
}
template <typename K, typename V>
ExtendibleHashTable<K, V>::~ExtendibleHashTable() {
  for (auto &b : dir_) {
    b.reset();
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  latch_2_.lock();
  // std::cout << "Find" << key << " begin" << std::endl;
  int idx = IndexOf(key);
  bool success = this->dir_[idx]->Find(key, value);
  // std::cout << success << std::endl;
  latch_2_.unlock();
  return success;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  latch_2_.lock();
  // std::cout << "Remove " << key << " begin" << std::endl;
  int idx = IndexOf(key);
  bool success = this->dir_[idx]->Remove(key);
  // std::cout << success << std::endl;
  latch_2_.unlock();
  return success;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  latch_2_.lock();
  // std::cout << "Insert" << key << " begin" << std::endl;
  InsertImp(key, value);
  // std::cout << "Insert" << key << " end" << std::endl;
  // for (const auto &p : dir_) {
  // std::cout << p.use_count() << " ";
  // }
  // std::cout << std::endl;
  latch_2_.unlock();
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertImp(const K &key, const V &value) {
  int idx = IndexOf(key);  // find index of the key
  auto ori_bucket = this->dir_[idx];
  bool success = ori_bucket->Insert(key, value);  // try inserting into the bucket to the index
  // std::cout << "First try " << success << " index = " << idx << std::endl;
  if (!success) {
    int dep = ori_bucket->GetDepth();
    auto bucket = new Bucket(this->bucket_size_, dep + 1);
    ori_bucket->IncrementDepth();
    // if local depth == global depth_, we need double the size of dir
    // std::cout << "Local depth == " << dep << ". Global depth == " << global_depth_ << std::endl;
    if (dep == this->global_depth_) {
      // std::cout << "Double the size of dir." << std::endl;
      this->global_depth_++;
      int ori_size = this->dir_.size();
      for (int i = 0; i < ori_size; i++) {
        std::shared_ptr<Bucket> ptr(this->dir_[i]);
        this->dir_.emplace_back(ptr);
      }
    }
    // split the full bucket into two buckets
    int m1 = (1 << dep);
    int m2 = (1 << (dep + 1));
    auto items = ori_bucket->GetItems();
    for (auto &p : items) {
      auto k = IndexOf(p.first);
      if ((idx & m1) != (k & m1)) {
        // std::cout << "Remove " << k << " to new bucket" << std::endl;
        ori_bucket->Remove(p.first);
        bucket->Insert(p.first, p.second);
      }
    }
    int s = this->dir_.size();
    idx += m1;
    while (idx < s) {
      idx += m2;
    }
    idx %= s;
    this->dir_[idx].reset(bucket);
    while (idx + m2 < s) {
      this->dir_[idx + m2] = this->dir_[idx];
      idx += m2;
    }
    num_buckets_++;
    // std::cout << "Size of dir:" << dir_.size() << ". Size of different buckets:" << num_buckets_ << std::endl;
    InsertImp(key, value);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto it = this->list_.begin();
  while (it != this->list_.end()) {
    if ((*it).first == key) {
      value = (*it).second;
      return true;
    }
    ++it;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = this->list_.begin();
  while (it != this->list_.end()) {
    if ((*it).first == key) {
      this->list_.erase(it);
      return true;
    }
    ++it;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto it = this->list_.begin();
  while (it != this->list_.end()) {
    if ((*it).first == key) {
      (*it).second = value;
      return true;
    }
    ++it;
  }
  if (IsFull()) {
    return false;
  }
  this->list_.emplace_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
