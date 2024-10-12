//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 public:
  LRUKNode() = default;
  LRUKNode(size_t k, frame_id_t fid) : k_{k}, fid_{fid} {}
  auto GetFid() const -> frame_id_t { return fid_; }
  auto UpdateTime(size_t time) -> void {
    // size?
    history_.emplace_back(time);
    if (history_.size() > k_) {
      //      printf("size k\n");
      history_.pop_front();
    }
  }
  auto GetHistory() const -> size_t {
    //    printf("%zu\n",history_.size());
    return history_.size();  //?
  }
  auto GetTime() -> size_t {
    if (history_.size() < k_) {
      return 0;
    }
    return history_.front();
  }
  void ClearHistory() { history_.clear(); }
  void SetEvict(bool t) { is_evictable_ = t; }
  auto GetEvict() -> bool { return is_evictable_; }

 private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

  std::list<size_t> history_;
  size_t k_;
  frame_id_t fid_;
  bool is_evictable_{false};  // false initially
};
class LRUNode {
 public:
  LRUNode() {
    // node = std::make_unique<LRUKNode>();
    next_ = prev_ = nullptr;
  }
  LRUNode(size_t k, frame_id_t fid) {
    node_ = LRUKNode(k, fid);
    next_ = prev_ = nullptr;
  }
  LRUNode *next_, *prev_;
  AccessType type_;
  //  bool enable_out_{true};
  LRUKNode node_;
  void Evict() {
    if (this->next_ != nullptr && this->prev_ != nullptr) {
      this->next_->prev_ = this->prev_;
      this->prev_->next_ = this->next_;
      this->next_ = nullptr;
      this->prev_ = nullptr;
    }
  };
};
// void LRUNode::evict(){
//   this->next_->prev_=this->prev;
//   this->prev_->next_=this->next;
//   this->next_= nullptr;
//   this->prev_= nullptr;
// }

class LRU {
  // 可以改成set实现
 public:
  LRU() {
    head_ = new LRUNode;
    head_->prev_ = head_->next_ = head_;
  }
  ~LRU() { delete head_; }
  auto SelectVictim() -> LRUNode * {
    if (head_->next_ == head_) {
      return nullptr;
    }

    LRUNode *p = head_->prev_;
    head_->prev_ = p->prev_;
    p->prev_->next_ = head_;
    p->next_ = nullptr;
    p->prev_ = nullptr;
    while (p->type_ == AccessType::Lookup || p->type_ == AccessType::Index) {
      p->next_ = head_->next_;
      head_->next_->prev_ = p;
      p->prev_ = head_;
      head_->next_ = p;

      p = head_->prev_;
      head_->prev_ = p->prev_;
      p->prev_->next_ = head_;
      p->next_ = nullptr;
      p->prev_ = nullptr;

      p->type_ = AccessType::Scan;
    }
    //    printf("vic %d\n",head_->prev_->node.get_fid());

    return p;
  };
  void MoveLRU(LRUNode *p) {
    if (head_->next_ == head_->prev_) {
      return;
    }
    //    if(p->next_&&p->prev_) {  //可能不在链表里
    p->next_->prev_ = p->prev_;
    p->prev_->next_ = p->next_;
    //    }
    auto tmp = head_->next_;
    while (p->node_.GetTime() < tmp->node_.GetTime()) {
      tmp = tmp->next_;
      if (tmp == head_) {
        break;
      }
    }
    p->prev_ = tmp->prev_;
    p->next_ = tmp;
    tmp->prev_->next_ = p;
    tmp->prev_ = p;

    //    printf("move ");
    //    for (auto x = head_->next_; x != head_; x = x->next_) {
    //      printf("%d:%zu ", x->node_.GetFid(), x->node_.GetTime());
    //
    //    }
  };
  void InsertLRU(LRUNode *p) {
    if (p->next_ != nullptr) {
      //            printf(" in %d next_ %d\n", p->node_.get_fid(),p->next->node.get_fid());
      p->next_->prev_ = p->prev_;
      p->prev_->next_ = p->next_;
    }
    if (head_->next_ == head_) {
      //      printf("%d\n",p->node_.GetFid());
      p->next_ = head_->next_;
      p->prev_ = head_;
      head_->next_->prev_ = p;
      head_->next_ = p;
      return;
      //      }
    }
    auto tmp = head_->next_;
    while (p->node_.GetTime() < tmp->node_.GetTime()) {
      tmp = tmp->next_;
      if (tmp == head_) {
        break;
      }
    }
    p->prev_ = tmp->prev_;
    p->next_ = tmp;
    tmp->prev_->next_ = p;
    tmp->prev_ = p;
    //    printf("insert ");
    //    for (auto x = head_->next_; x != head_; x = x->next_) {
    //      printf("%d:%zu ", x->node_.GetFid(), x->node_.GetTime());
    //    }
  };

 private:
  LRUNode *head_;
  //  int type_; //0 LRU, 1 LRU-K;
  //  std::mutex latch_;
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  //  ~LRUKReplacer() = default;
  ~LRUKReplacer();

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
   * based on LRU.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   * @param access_type type of access that was received. This parameter is only needed for
   * leaderboard tests.
   */
  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  // 只有evictable的frame才加入以下两条链表
  // 维护集合

  //  std::function<bool(LRUKNode &, LRUKNode &)> comp_ = [](LRUKNode &n1, LRUKNode &n2) {
  //    if (n1.GetTime() >= n2.GetTime()) {
  //      return true;
  //    }
  //    return false;
  //  };

  //  std::set<frame_id_t, std::function<bool(frame_id_t, frame_id_t)>> nk_l_;
  //  std::set<frame_id_t, std::function<bool(frame_id_t, frame_id_t)>> k_l_;

  std::unordered_set<frame_id_t> nk_nodes_;
  std::unordered_set<frame_id_t> k_nodes_;
  //  std::set<frame_id_t> pinned_node;
  //  std::unordered_map<frame_id_t, LRUNode*> node_store_;
  // 没到k次的链表
  LRU *nk_list_;
  //  // k次的链表
  LRU *k_list_;
  // 不可置换的frame的链表
  //   LRU *free_list;
  //   std::list<LRUKNode*> *nevict_list;
  LRUNode *nodes_;
  //  LRUKNode *nodes_;
  size_t current_timestamp_{0};
  [[maybe_unused]] size_t curr_size_{0};
  // 可置换的frame数量
  size_t replacer_size_{0};
  size_t num_frames_;
  [[maybe_unused]] size_t k_;
  std::mutex latch_;
};

}  // namespace bustub
