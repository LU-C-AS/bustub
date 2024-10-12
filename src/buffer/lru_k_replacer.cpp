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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : num_frames_(num_frames), k_(k) {
  nodes_ = new LRUNode[num_frames];
  nk_list_ = new LRU;
  k_list_ = new LRU;
  for (size_t i = 0; i < num_frames; i++) {
    nodes_[i].node_ = LRUKNode(k, static_cast<int>(i));
  }

  //  std::cout << "num " << num_frames << " k " << k << std::endl;
}

LRUKReplacer::~LRUKReplacer() {
  delete[] nodes_;
  delete nk_list_;
  delete k_list_;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> lk(latch_);

  auto p = nk_list_->SelectVictim();
  if (p != nullptr) {
    //    std::cout<<p->node.GetFid()<<" nk\n";
    *frame_id = p->node_.GetFid();
    //    std::cout << "evict nk " << *frame_id << std::endl;
    p->node_.ClearHistory();

    //    p->node_.SetEvict(false);

    replacer_size_--;
    nk_nodes_.erase(p->node_.GetFid());
    return true;
  }
  p = k_list_->SelectVictim();
  if (p != nullptr) {
    //    std::cout<<p->node.GetFid()<<" k\n";
    *frame_id = p->node_.GetFid();
    //    std::cout << "evict k " << *frame_id << std::endl;
    p->node_.ClearHistory();

    //    p->node_.SetEvict(false);

    replacer_size_--;
    k_nodes_.erase(p->node_.GetFid());
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (frame_id > static_cast<int>(num_frames_)) {
    throw Exception("recordAccess:invalid frame_id\n");
  }

  std::unique_lock<std::mutex> lk(latch_);

  //  std::cout << "record " << frame_id << std::endl;
  nodes_[frame_id].node_.UpdateTime(++current_timestamp_);
  nodes_[frame_id].type_ = access_type;
  if (nodes_[frame_id].node_.GetEvict()) {
    //    std::cout << "get evict " << frame_id << std::endl;
    if (nodes_[frame_id].node_.GetHistory() < k_) {
      //      std::cout << frame_id << " <k" << std::endl;
      if (nk_nodes_.find(frame_id) != nk_nodes_.end()) {
        //        std::cout << "rec nk move " << frame_id << std::endl;
        nk_list_->MoveLRU(nodes_ + frame_id);
      } else {
        //        std::cout << "rec nk insert" << frame_id << std::endl;
        nk_list_->InsertLRU(nodes_ + frame_id);
        nk_nodes_.insert(frame_id);
        replacer_size_++;
      }
    } else {
      //      std::cout<<frame_id<<" k"<<std::endl;
      if (k_nodes_.find(frame_id) != k_nodes_.end()) {
        //        std::cout << "rec k move " << frame_id << std::endl;
        k_list_->MoveLRU(nodes_ + frame_id);
      } else {
        //        std::cout << "rec k insert" << frame_id << std::endl;
        k_list_->InsertLRU(nodes_ + frame_id);
        if (nk_nodes_.find(frame_id) == nk_nodes_.end()) {
          //          std::cout << "rec k replace+\n";
          replacer_size_++;
        } else {
          //          std::cout<<"nk moveto\n";
          nk_nodes_.erase(frame_id);
        }
        k_nodes_.insert(frame_id);
      }
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  //  std::cout << "set " << frame_id << " " << set_evictable << std::endl;
  if (frame_id > static_cast<int>(num_frames_)) {
    throw Exception("setEvictable:invalid frame_id\n");
  }
  std::unique_lock<std::mutex> lk(latch_);

  if (set_evictable == nodes_[frame_id].node_.GetEvict()) {
    return;
  }
  // 改

  if (set_evictable && (nk_nodes_.find(frame_id) == nk_nodes_.end() && k_nodes_.find(frame_id) == k_nodes_.end())) {
    if (nodes_[frame_id].node_.GetHistory() < k_) {
      //      std::cout << "set true nk insert" << frame_id << std::endl;
      nk_list_->InsertLRU(nodes_ + frame_id);
      nk_nodes_.insert(frame_id);
    } else {
      //      std::cout << "set true k insert" << frame_id << std::endl;
      k_list_->InsertLRU(nodes_ + frame_id);
      k_nodes_.insert(frame_id);
    }
    replacer_size_++;
  } else if (!set_evictable &&
             (nk_nodes_.find(frame_id) != nk_nodes_.end() || k_nodes_.find(frame_id) != k_nodes_.end())) {
    //    std::cout << "set evict " << frame_id;
    nodes_[frame_id].Evict();
    replacer_size_--;
    nk_nodes_.erase(frame_id);
    k_nodes_.erase(frame_id);
  }
  nodes_[frame_id].node_.SetEvict(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (frame_id > static_cast<int>(num_frames_)) {
    throw Exception("setEvictable:invalid frame_id\n");
  }

  std::unique_lock<std::mutex> lk(latch_);
  //  latch_.lock();
  if (nk_nodes_.find(frame_id) == nk_nodes_.end() && k_nodes_.find(frame_id) == k_nodes_.end() &&
      nodes_[frame_id].node_.GetEvict()) {
    return;
  }
  if (!nodes_[frame_id].node_.GetEvict()) {
    throw Exception("frame not evictable\n");
  }
  //  std::cout << "remove " << frame_id;
  nodes_[frame_id].Evict();
  nodes_[frame_id].node_.SetEvict(true);
  replacer_size_--;
  nk_nodes_.erase(frame_id);
  k_nodes_.erase(frame_id);
  //  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return replacer_size_; }

}  // namespace bustub
