//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;

  void SetValueAt(int index, const ValueType &value);
  /*
   * @brief Helper function to find the last index that array_[index].key <= key
   */
  auto IndexLookup(const KeyType &key, const KeyComparator &comparator) const -> int;

  /*
   * @brief Helper function to the index corresponding to key
   */
  auto Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType;

  /*
   * @brief Populate new root page with old_value + new_key & new_value.
   */
  void PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);

  void InsertAt(int index, const KeyType &new_key, const ValueType &new_value);
  /*
   * @brief Insert new_key & new_value pair right after the pair with its value ==
   * old_value
   * @return:  new size after insertion
   */
  auto InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value) -> int;

  auto ValueIndex(const ValueType &value) const -> int;

  auto RemoveAndReturnOnlyChild() -> ValueType;
  void MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager);

  void CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager);
  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                        BufferPoolManager *buffer_pool_manager);
  void CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager);
  void MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                         BufferPoolManager *buffer_pool_manager);
  void CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager);

  void Remove(int index);

  void SetParentToMe(page_id_t page_id, BufferPoolManager *buffer_pool_manager);

  void MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager);

 private:
  // Flexible array member for page data.
  MappingType array_[2];
  IndexPageType page_type_ __attribute__((__unused__));
  lsn_t lsn_ __attribute__((__unused__));
  int size_ __attribute__((__unused__));
  int max_size_ __attribute__((__unused__));
  page_id_t parent_page_id_ __attribute__((__unused__));
  page_id_t page_id_ __attribute__((__unused__));
};
}  // namespace bustub
