#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if(IsEmpty()){
    return false;
  }
  Page* page_ptr = FindLeafPage(key, 0, 0, transaction);
  IN_TREE_LEAF_PAGE_TYPE* leaf_ptr = reinterpret_cast<IN_TREE_LEAF_PAGE_TYPE*>(page_ptr->GetData());
  RID temp_rid;
  bool ret = leaf_ptr->Lookup(key, &temp_rid, comparator_);
  buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);

  if (!ret){ return false; }
  result->push_back(temp_rid);
  return true;

  
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  if (IsEmpty()){
    StartNewTree(key, value);
    ret = true;
  }
  else{
    ret = InsertIntoLeaf(key, value, transaction);
  }
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t root_page_id;

  Page* root_page_ptr = buffer_pool_manager_->NewPage(root_page_id);

  root_page_id_ = root_page_id;
  UpdateRootPageId(1);

  IN_TREE_LEAF_PAGE_TYPE* leaf_ptr = reinterpret_cast<IN_TREE_LEAF_PAGE_TYPE*>(root_page_ptr->GetData());
  leaf_ptr->Init(root_page_id, INVALID_PAGE_ID, leaf_max_size_);
  leaf_ptr->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(root_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page* page_ptr = FindLeafPage(key, false, 1, transaction);
  if (page_ptr == nullptr){
    return false;
  }
  IN_TREE_LEAF_PAGE_TYPE* leaf_ptr = reinterpret_cast<IN_TREE_LEAF_PAGE_TYPE*>(page_ptr->GetData());

  int ori_size = leaf_ptr->GetSize();
  int size = leaf_ptr->Insert(key, value, comparator_);

  if(size == ori_size){
    buffer_pool_manager_->UnpinPage(new_leaf_ptr->GetPageId(), true);
    return false;
  }
  else if (size == leaf_ptr->GetMaxSize()){

    IN_TREE_LEAF_PAGE_TYPE* new_leaf_ptr = SplitPage<IN_TREE_LEAF_PAGE_TYPE>(leaf_ptr);

    leaf_ptr->MoveHalfTo(new_leaf_ptr);
    new_leaf_ptr->SetNextPageId(leaf_ptr->GetNextPageId());
    leaf_ptr->SetNextPageId(new_leaf_ptr->GetPageId());

    // After `MoveHalfTo` middle key is kept in array[0]
    InsertIntoParent(leaf_ptr, new_leaf_ptr->KeyAt(0), new_leaf_ptr, transaction);

    buffer_pool_manager_->UnpinPage(new_leaf_ptr->GetPageId(), true);
  }

  page_ptr->SetDirty(true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, int mode, Transaction* transaction) {
  // std::shared_ptr<std::deque<Page *>> deque_ptr = nullptr;
  if (IsEmpty()){
    return nullptr;
  }

  page_id_t page_id = root_page_id_;
  page_id_t last_page_id = INVALID_PAGE_ID;
  Page *page_ptr = nullptr;
  Page *last_page_ptr = nullptr;
  BPlusTreePage* tree_ptr;

  while (true){
    page_ptr = buffer_pool_manager_->FetchPage(page_id);
    assert(page_ptr != nullptr);
    tree_ptr = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());

    if(last_page_ptr != nullptr){
      buffer_pool_manager_->UnpinPage(last_page_id, false);
    }
    bool is_leaf = tree_ptr->IsLeafPage();
    if (is_leaf){
      break;
    }
    last_page_id = page_id;
    last_page_ptr = page_ptr;

    IN_TREE_INTERNAL_PAGE_TYPE* internal_ptr = reinterpret_cast<IN_TREE_INTERNAL_PAGE_TYPE*>(tree_ptr);
    page_id = internal_ptr->Lookup(key, comparator_);
  }

  return page_ptr;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::SplitPage(N *node) {
  page_id_t new_page_id;
  Page* new_page_ptr = buffer_pool_manager_->NewPage(new_page_id);

  N* type_n_page_ptr = reinterpret_cast<N*>(new_page_ptr->GetData());
  if (node->IsLeafPage()){
    type_n_page_ptr->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);
  }
  else{
    type_n_page_ptr->Init(new_page_id, node->GetParentPageId(), internal_max_size_);
  }

  return type_n_page_ptr;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node, Transaction *transaction) {
  if (old_node->IsRootPage()){  // Which means root has been split and need to construct a new root
    page_id_t new_root_page_id;
    Page* new_root_page_ptr;
    new_root_page_ptr = buffer_pool_manager_->NewPage(new_root_page_id);
    IN_TREE_INTERNAL_PAGE_TYPE *new_root_ptr = reinterpret_cast<IN_TREE_INTERNAL_PAGE_TYPE*>(new_root_page_ptr->GetData());

    new_root_ptr->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);

    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    new_root_ptr->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    SetRootPageId(new_root_page_id);
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    return;
  }

  page_id_t parent_page_id = old_node->GetParentPageId();
  Page *parent_page_ptr = buffer_pool_manager_->FetchPage(parent_page_id);
  IN_TREE_INTERNAL_PAGE_TYPE *parent_ptr = reinterpret_cast<IN_TREE_INTERNAL_PAGE_TYPE*>(parent_page_ptr->GetData());

  int new_size = parent_ptr->InsertNodeAfter(old_node->GetPageId(),
                                             key,
                                             new_node->GetPageId());

  if (new_size == parent_ptr->GetMaxSize()){
    IN_TREE_INTERNAL_PAGE_TYPE* new_parent_ptr = SplitPage<IN_TREE_INTERNAL_PAGE_TYPE>(parent_ptr);

    parent_ptr->MoveHalfTo(new_parent_ptr, buffer_pool_manager_);
    InsertIntoParent(parent_ptr,
                     new_parent_ptr->KeyAt(0),
                     new_parent_ptr,
                     transaction);

    buffer_pool_manager_->UnpinPage(new_parent_ptr->GetPageId(), true);
  }

  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}



/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()){
    return;
  }

  Page* leaf_page_ptr = FindLeafPage(key, false, 2, transaction);
  page_id_t leaf_page_id = leaf_page_ptr->GetPageId();
  IN_TREE_LEAF_PAGE_TYPE* leaf_ptr = reinterpret_cast<IN_TREE_LEAF_PAGE_TYPE*>(leaf_page_ptr->GetData());

  int index = leaf_ptr->KeyIndex(key, comparator_);
  if(leaf_ptr->KetAt(index) != key){  
    return;
  }
  leaf_ptr->RemoveAt(index);
  bool ret = false;
  if (leaf_ptr->GetSize() < leaf_ptr->GetMinSize()){
    ret = CoalesceOrRedistribute<IN_TREE_LEAF_PAGE_TYPE>(leaf_ptr, transaction);
  }

  if(!ret)
    leaf_page_ptr->SetDirty(true);
  }

}


/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()){
    return AdjustRoot(node);
  }

  page_id_t parent_page_id;
  page_id_t prev_page_id = INVALID_PAGE_ID;
  page_id_t next_page_id = INVALID_PAGE_ID;
  Page *parent_page_ptr;
  Page *prev_page_ptr;
  Page *next_page_ptr;
  IN_TREE_INTERNAL_PAGE_TYPE* parent_ptr;
  N *prev_node;
  N *next_node;

  parent_page_id = node->GetParentPageId();
  parent_page_ptr = buffer_pool_manager_->FetchPage(parent_page_id);
  parent_ptr = reinterpret_cast<IN_TREE_INTERNAL_PAGE_TYPE*>(parent_page_ptr->GetData());

  int node_index = parent_ptr->ValueIndex(node->GetPageId());
  if (node_index > 0){
    prev_page_id = parent_ptr->ValueAt(node_index - 1);
    prev_page_ptr = buffer_pool_manager_->FetchPage(prev_page_id);
    prev_node = reinterpret_cast<N*>(prev_page_ptr->GetData());

    if (prev_node->GetSize() > prev_node->GetMinSize()){
      Redistribute(prev_node, node, 1);

      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      buffer_pool_manager_->UnpinPage(prev_page_id, true);
      return false;
    }
  }

  if (node_index != parent_ptr->GetSize() - 1){
    next_page_id = parent_ptr->ValueAt(node_index + 1);
    next_page_ptr = buffer_pool_manager_->FetchPage(next_page_id);
    next_node = reinterpret_cast<N*>(next_page_ptr->GetData());
    if (next_node->GetSize() > next_node->GetMinSize()){
      Redistribute(next_node, node, 0);

      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      if (node_index > 0){
        buffer_pool_manager_->UnpinPage(prev_page_id, false);
      }
      buffer_pool_manager_->UnpinPage(next_page_id, true);

      return false;
    }
  }

  bool ret = false;
  if (prev_page_id != INVALID_PAGE_ID){
    ret = Coalesce(&prev_node, &node, &parent_ptr, node_index, transaction);

    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    buffer_pool_manager_->UnpinPage(prev_page_id, true);
    if (next_page_id != INVALID_PAGE_ID){
      buffer_pool_manager_->UnpinPage(next_page_id, false);
    }

    return true;
  }

  // prev_page_id == INVALID_PAGE_ID
  ret = Coalesce(&node, &next_node, &parent_ptr, node_index + 1, transaction);
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  buffer_pool_manager_->UnpinPage(next_page_id, true);
  
  return false;
}


/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->GetSize() > 1){
    return false;
  }

  page_id_t new_root_id;
  if (old_root_node->IsLeafPage()){
    if (old_root_node->GetSize() == 1){
      return false;
    }
    new_root_id = INVALID_PAGE_ID;
  }
  else{
    IN_TREE_INTERNAL_PAGE_TYPE* old_root_internal_node = reinterpret_cast<IN_TREE_INTERNAL_PAGE_TYPE*>(old_root_node);
    new_root_id = old_root_internal_node->RemoveAndReturnOnlyChild(); // check nullptr here???

    Page* new_root_page_ptr = buffer_pool_manager_->FetchPage(new_root_id);
    IN_TREE_INTERNAL_PAGE_TYPE* new_root_ptr = reinterpret_cast<IN_TREE_INTERNAL_PAGE_TYPE*>(new_root_page_ptr->GetData());
    new_root_ptr->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root_id, true);
  }

  root_page_id_ = new_root_id;
  UpdateRootPageId(0);

  return true;
}


/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  page_id_t parent_page_id = node->GetParentPageId();
  Page *parent_page_ptr = buffer_pool_manager_->FetchPage(parent_page_id);
  IN_TREE_INTERNAL_PAGE_TYPE* parent_ptr = reinterpret_cast<IN_TREE_INTERNAL_PAGE_TYPE*>(parent_page_ptr->GetData());

  if (node->IsLeafPage()){
    IN_TREE_LEAF_PAGE_TYPE* op_node =
      reinterpret_cast<IN_TREE_LEAF_PAGE_TYPE*>(node);
    IN_TREE_LEAF_PAGE_TYPE* op_neighbor_node =
      reinterpret_cast<IN_TREE_LEAF_PAGE_TYPE*>(neighbor_node);

    if (index == 0){
      op_neighbor_node->MoveFirstToEndOf(op_node);

      int node_index = parent_ptr->ValueIndex(op_neighbor_node->GetPageId());
      parent_ptr->SetKeyAt(node_index, op_neighbor_node->KeyAt(0));
    }
    else{
      op_neighbor_node->MoveLastToFrontOf(op_node);

      int node_index = parent_ptr->ValueIndex(op_node->GetPageId());
      parent_ptr->SetKeyAt(node_index, op_node->KeyAt(0));
    }
  }
  else{
    IN_TREE_INTERNAL_PAGE_TYPE* op_node =
      reinterpret_cast<IN_TREE_INTERNAL_PAGE_TYPE*>(node);
    IN_TREE_INTERNAL_PAGE_TYPE* op_neighbor_node =
      reinterpret_cast<IN_TREE_INTERNAL_PAGE_TYPE*>(neighbor_node);

    if (index == 0){
      int node_index = parent_ptr->ValueIndex(op_neighbor_node->GetPageId());
      KeyType middle_key = parent_ptr->KeyAt(node_index);
      KeyType next_middle_key = op_neighbor_node->KeyAt(1);

      op_neighbor_node->MoveFirstToEndOf(op_node, middle_key, buffer_pool_manager_);
      parent_ptr->SetKeyAt(node_index, next_middle_key);
    }
    else{
      int node_index = parent_ptr->ValueIndex(op_node->GetPageId());
      KeyType middle_key = parent_ptr->KeyAt(node_index);
      KeyType next_middle_key = op_neighbor_node->KeyAt(op_neighbor_node->GetSize() - 1);

      op_neighbor_node->MoveLastToFrontOf(op_node, middle_key, buffer_pool_manager_);
      parent_ptr->SetKeyAt(node_index, next_middle_key);
    }
  }

  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}


/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  if ((*node)->IsLeafPage()){
    IN_TREE_LEAF_PAGE_TYPE* op_node =
      reinterpret_cast<IN_TREE_LEAF_PAGE_TYPE*>(*node);
    IN_TREE_LEAF_PAGE_TYPE* op_neighbor_node =
      reinterpret_cast<IN_TREE_LEAF_PAGE_TYPE*>(*neighbor_node);

    op_node->MoveAllTo(op_neighbor_node);
  }
  else{
    IN_TREE_INTERNAL_PAGE_TYPE* op_node =
      reinterpret_cast<IN_TREE_INTERNAL_PAGE_TYPE*>(*node);
    IN_TREE_INTERNAL_PAGE_TYPE* op_neighbor_node =
      reinterpret_cast<IN_TREE_INTERNAL_PAGE_TYPE*>(*neighbor_node);

    KeyType middle_key = (*parent)->KeyAt(index);
    op_node->MoveAllTo(op_neighbor_node, middle_key, buffer_pool_manager_);
  }

  (*parent)->Remove(index);
  if ((*parent)->GetSize() < (*parent)->GetMinSize()){
    return CoalesceOrRedistribute(*parent, transaction);
  }
  return false;
}


/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return 0; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
