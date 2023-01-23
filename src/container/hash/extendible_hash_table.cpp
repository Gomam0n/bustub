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

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  auto bucket = Bucket(bucket_size, 0);         // *************Use object instead of pointer can avoid memory leak. Why?******************
  this->dir_.emplace_back(std::make_shared<Bucket>(bucket));
}
template <typename K, typename V>
ExtendibleHashTable<K, V>::~ExtendibleHashTable(){
  for(auto &b:dir_){
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
  latch_.lock();
  int idx = IndexOf(key);
  bool success = this->dir_[idx]->Find(key, value);
  latch_.unlock();
  return success;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  latch_.lock();
  int idx = IndexOf(key);
  bool success = this->dir_[idx]->Remove(key);
  latch_.unlock();
  return success;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  latch_.lock();  
  int idx = IndexOf(key); // find index of the key
  auto ori_bucket = this->dir_[idx];  
  bool success = ori_bucket->Insert(key, value);  // try inserting into the bucket to the index
  if (!success) {
    int dep = ori_bucket->GetDepth(); 
    auto bucket = new Bucket(this->bucket_size_, dep + 1);
    ori_bucket->IncrementDepth();
    // if local depth == global depth_, we need double the size of dir
    if (dep == this->global_depth_) {
      this->global_depth_++;
      int ori_size = this->dir_.size();
      for (int i = 0; i < ori_size; i++) {
        std::shared_ptr<Bucket> ptr(this->dir_[i]);
        this->dir_.emplace_back(ptr);
      }
    }
    // split the full bucket into two buckets
    int m1 = (1 << dep), m2 = (1 << (dep + 1));
    auto items = ori_bucket->GetItems();
    for (auto &p : items) {
      auto k = IndexOf(p.first);
      if ((k & m1) != (k & m2)) {
        ori_bucket->Remove(p.first);
        bucket->Insert(p.first, p.second);
      }
    }
    int s = this->dir_.size();
    if (idx < int(s / 2)){
      idx += this->dir_.size() / 2;
    }
    this->dir_[idx].reset(bucket);
    latch_.unlock();
    Insert(key, value);
  }else{
    latch_.unlock();
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
