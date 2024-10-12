//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

const int THREAD_CNT = 2;
DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  //  throw NotImplementedException(
  //      "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the
  //      " "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  for (auto i = 0; i < THREAD_CNT; i++) {
    background_thread_.push_back(std::make_optional<std::thread>([&] { StartWorkerThread(); }));
  }
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  for (auto i = 0; i < THREAD_CNT; i++) {
    request_queue_.Put(std::nullopt);
  }
  for (auto i = 0; i < THREAD_CNT; i++) {
    if (background_thread_[i].has_value()) {
      background_thread_[i]->join();
    }
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  auto re = std::make_optional<DiskRequest>(std::move(r));
  request_queue_.Put(std::move(re));
}

void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto task = request_queue_.Get();
    if (task == std::nullopt) {
      break;
    }
    if (task->is_write_) {
      disk_manager_->WritePage(task->page_id_, task->data_);
    } else {
      disk_manager_->ReadPage(task->page_id_, task->data_);
    }
    task->callback_.set_value(true);
    // 存读写类型
    //    auto pid = task->page_id_;
    //    auto it = page_q_.find(task->page_id_);
    //    if (it == page_q_.end()) {
    //      //      auto l = std::make_unique<std::list<DiskRequest>>();
    //      //      l->emplace_back(std::move(task.value()));
    //
    //      //      locks_.insert({task->page_id_, std::make_unique<std::mutex>()});
    //      //      auto l = std::list<std::optional<DiskRequest>>{std::move(task)};
    //      auto c = std::make_unique<Channel<DiskRequest>>();
    //      c->Put(std::move(task.value()));
    //      page_q_.emplace(task->page_id_, std::move(c));
    //    } else {
    //      //      locks_[task->page_id_]->lock();
    //      (it->second)->Put(std::move(task.value()));
    //      //      locks_[task->page_id_]->unlock();
    //    }
    //    auto f = [=](auto pid) {
    //      DiskRequest task = page_q_[pid]->Get();
    //      //      GetTask(pid, &task);
    //      if (task.is_write_) {
    //        disk_manager_->WritePage(task.page_id_, task.data_);
    //      } else {
    //        disk_manager_->ReadPage(task.page_id_, task.data_);
    //      }
    //      task.callback_.set_value(true);
    //    };
    //    std::thread t(f, pid);
    //    t.detach();
  }
}

}  // namespace bustub
