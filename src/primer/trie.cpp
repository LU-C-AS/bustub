#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  //  throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (root_ == nullptr) {
    return nullptr;
  }
  auto ptr = root_;
  for (const char c : key) {
    auto it = ptr->children_.find(c);
    if (it != ptr->children_.end()) {
      ptr = it->second;
    } else {
      //      std::cout << 0 << key << std::endl;
      return nullptr;
    }
  }
  //  std::cout << 1 << key << std::endl;
  auto ptr1 = dynamic_cast<const TrieNodeWithValue<T> *>(ptr.get());
  if (ptr1 == nullptr) {
    // std::cout << key << std::endl;
    //     printf("null ptr1 %s\n", key);
    return nullptr;
  }

  // std::cout << ptr1->value_ << std::endl;
  return ptr1->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  //  throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  auto new_trie = std::make_shared<Trie>();
  //  auto new_root = new_trie->root_;
  if (root_ != nullptr) {
    new_trie->root_ = std::shared_ptr(root_->Clone());
  } else {
    new_trie->root_ = std::make_shared<const TrieNode>();
  }
  auto new_root = std::const_pointer_cast<TrieNode>(new_trie->root_);
  if (key.empty()) {
    auto p_val = std::make_shared<T>(std::move(value));
    //    auto p = const_cast<std::shared_ptr<const TrieNode> *>(&root_);
    new_trie->root_ = std::make_shared<TrieNodeWithValue<T>>(new_root->children_, p_val);
    //    auto pp = std::make_shared<TrieNodeWithValue<T>>(new_root->children_, p_val);
    //    *new_root_p = pp;
    return *new_trie;
  }
  auto ptr_ori = root_;     // may stop where the node isn't find
  auto ptr_new = new_root;  // point to the last node
  auto ij = key.begin();
  std::shared_ptr<TrieNode> new_node;
  // printf("%c %d %d\n", key[0], ptr_ori == nullptr, ptr_new == nullptr);

  for (; ij != key.end(); ij++) {
    if (ij == key.end() - 1) {
      auto p_val = std::make_shared<T>(std::move(value));
      std::shared_ptr<TrieNodeWithValue<T>> new_node1;
      if (ptr_ori != nullptr && ptr_ori->children_.find(*ij) != ptr_ori->children_.end()) {
        // std::cout << key << " ";
        ptr_ori = ptr_ori->children_.find(*ij)->second;
        new_node1 = std::make_shared<TrieNodeWithValue<T>>(ptr_ori->children_, p_val);
        ptr_new->children_.erase(*ij);
      } else {
        new_node1 = std::make_shared<TrieNodeWithValue<T>>(p_val);
      }
      ptr_new->children_.emplace(*ij, new_node1);
      // std::cout << new_node1->value_ << std::endl;
      break;
    }
    if (ptr_ori != nullptr && ptr_ori->children_.find(*ij) != ptr_ori->children_.end()) {
      ptr_ori = ptr_ori->children_.find(*ij)->second;

      new_node = std::shared_ptr(ptr_ori->Clone());

      ptr_new->children_.erase(*ij);
      ptr_new->children_.emplace(*ij, new_node);

    } else {
      // printf("1 ");
      new_node = std::make_shared<TrieNode>();
      ptr_new->children_.emplace(*ij, new_node);
    }

    ptr_new = std::const_pointer_cast<TrieNode>(new_node);
    //    std::cout << ptr_ori << std::endl;
  }

  return *new_trie;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  //  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  auto new_trie = std::make_shared<Trie>();
  new_trie->root_ = std::shared_ptr(root_->Clone());

  if (key.empty()) {
    new_trie->root_ = std::make_shared<TrieNode>(root_->children_);
    return *new_trie;
  }

  auto ptr_new = std::const_pointer_cast<TrieNode>(new_trie->root_);
  auto ptr_new_p = ptr_new;
  auto ptr_ori = root_;
  std::shared_ptr<TrieNode> new_node;
  auto ij = key.begin();
  auto ic = ij;
  auto ptr_new_del = ptr_new;
  for (; ij != key.end(); ij++) {
    //    auto ptr_new_n = std::const_pointer_cast<TrieNode>(ptr_new);
    auto it = ptr_ori->children_.find(*ij);
    if (it != ptr_ori->children_.end()) {
      ptr_ori = std::const_pointer_cast<TrieNode>(it->second);

      new_node = std::shared_ptr<TrieNode>(ptr_ori->Clone());
      //      std::cout << "new_node " << newNode->is_value_node_ << std::endl;

      ptr_new->children_.erase(*ij);
      ptr_new->children_.emplace(*ij, new_node);
    } else {
      std::cout << key << " " << *ij << std::endl;
      throw Exception("remove: not find the key!\n");
    }
    ptr_new_p = ptr_new;
    ptr_new = new_node;
    //    ptr_ori = new_node;

    if (ij == key.begin() || (ij != key.end() - 1 && (new_node->is_value_node_ || new_node->children_.size() > 1))) {
      ptr_new_del = new_node;
      ic = ij;
      //      std::cout << *ic << " ptrdel" << std::endl;
    }
  }

  // auto pp = ptr_new_del->children_.find(*(ic+1))->second;
  if (new_node->children_.empty()) {
    auto pp = std::const_pointer_cast<TrieNode>(ptr_new_del);
    //    std::cout << *(ic + 1) << std::endl;

    pp->children_.erase(*(ic + 1));
    //    std::cout << "erase:" << pp->children_.erase(*(ic + 1)) << " size:" << pp->children_.size() << std::endl;
    //    std::cout << root_->children_.find(*(key.begin()))->second << ' ' << ptr_new_del.get();
    if (ptr_new_del == new_trie->root_->children_.find(*(key.begin()))->second) {
      auto new_root = std::const_pointer_cast<TrieNode>(new_trie->root_);
      if (!ptr_new_del->is_value_node_) {
        //        std::cout << "ptr_\n";
        //        auto ppp = std::const_pointer_cast<TrieNode>(new_trie->root_);
        new_root->children_.erase(*(key.begin()));
      }
      if (new_root->children_.empty()) {
        new_trie->root_ = nullptr;
      }
    }
    // Get<int>(key);

    //  You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
    //  you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  } else {
    auto ppp = std::make_shared<TrieNode>(new_node->children_);
    // auto ppp = dynamic_cast<const TrieNode *>(new_node.get());
    ptr_new_p->children_.erase(*(key.end() - 1));
    ptr_new_p->children_.emplace(*(key.end() - 1), ppp);
  }

  return *new_trie;
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
