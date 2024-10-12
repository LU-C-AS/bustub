//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_latch_ = new std::mutex[pool_size];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  //  std::atomic_init(&prefetch_done_, false);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

// void BufferPoolManager::PrefetchWorker() {
//   while (true) {
//     if (prefetch_done_ == true) {
//       break;
//     }
//     std::unique_lock<std::mutex> lk(q_lk_);
//     while (prefetch_tasks_.empty()) {
//       cv.wait(lk);
//     }
//     prefetch_tasks_.
//   }
// }

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  //  prefetch_done_.store(true);
  delete[] page_latch_;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock<std::mutex> l(latch_);
  frame_id_t f;
  if (!free_list_.empty()) {
    f = free_list_.back();
    //    std::cout << "newpage free " << f << std::endl;
    free_list_.pop_back();
    //    std::cout<<free_list_.size()<<std::endl;
    *page_id = AllocatePage();

    auto find = static_cast<unsigned>(f);

    page_latch_[find].lock();
    pages_[find].ResetMemory();
    pages_[find].page_id_ = *page_id;
    pages_[find].pin_count_ = 1;
    pages_[find].is_dirty_ = false;
    page_latch_[find].unlock();

    page_table_.insert({*page_id, f});
    replacer_->SetEvictable(f, false);
    replacer_->RecordAccess(f);
    return pages_ + f;
  }
  if (replacer_->Evict(&f)) {
    auto find = static_cast<unsigned>(f);
    //    std::cout << "newpage evict " << f << std::endl;
    page_table_.erase(pages_[find].page_id_);

    *page_id = AllocatePage();
    page_table_.insert({*page_id, f});
    replacer_->SetEvictable(f, false);
    replacer_->RecordAccess(f);

    page_latch_[find].lock();
    l.unlock();
    if (pages_[find].IsDirty()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, pages_[find].data_, pages_[find].page_id_, std::move(promise)});
      future.get();
    }
    //    pages_[find].ResetMemory();
    pages_[find].page_id_ = *page_id;
    pages_[find].pin_count_ = 1;
    pages_[find].is_dirty_ = false;

    page_latch_[find].unlock();

    //    l.lock();
    //    page_table_.insert({*page_id, f});
    //    replacer_->SetEvictable(f, false);
    //    replacer_->RecordAccess(f);
    //    l.unlock();

    return pages_ + f;
  }
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::mutex> l(latch_);

  Page *p;
  //  Page *ret = nullptr;
  //  for (int i = 0; i < round; i++) {
  auto f = page_table_.find(page_id);
  uint32_t find;
  // scan可以预取
  if (f == page_table_.end()) {
    //    std::cout << "fetch end" << page_id << std::endl;
    frame_id_t fid;
    if (!free_list_.empty()) {
      fid = free_list_.back();
      //      std::cout << "fetch free fid\n";
      free_list_.pop_back();
      //    std::cout<<free_list_.size()<<std::endl;

      //      pages_[static_cast<unsigned>(fid)].ResetMemory();
      find = static_cast<unsigned>(fid);

      page_table_.insert({page_id, fid});
      replacer_->SetEvictable(fid, false);
      replacer_->RecordAccess(fid, access_type);

      page_latch_[find].lock();
      l.unlock();

      pages_[find].page_id_ = page_id;
      pages_[find].pin_count_ = 1;
      pages_[find].is_dirty_ = false;
      //      page_latch_[find].lock();
      //      page_table_.insert({page_id, fid});
      //      replacer_->SetEvictable(fid, false);
      //      replacer_->RecordAccess(fid, access_type);
      p = pages_ + fid;
    } else if (replacer_->Evict(&fid)) {
      find = static_cast<unsigned>(fid);
      page_table_.erase(pages_[find].page_id_);

      page_table_.insert({page_id, fid});
      replacer_->SetEvictable(fid, false);
      replacer_->RecordAccess(fid, access_type);
      page_latch_[find].lock();
      l.unlock();

      //      std::cout << "fetch evict fid\n";
      if (pages_[find].IsDirty()) {
        auto promise = disk_scheduler_->CreatePromise();
        auto future = promise.get_future();
        disk_scheduler_->Schedule({true, pages_[find].data_, pages_[find].page_id_, std::move(promise)});
        future.get();
      }
      pages_[find].page_id_ = page_id;
      pages_[find].pin_count_ = 1;
      pages_[find].is_dirty_ = false;

      //      pages_[static_cast<unsigned>(fid)].ResetMemory();

      p = pages_ + fid;
    } else {
      return nullptr;
    }
    //    page_latch_[fid].lock();
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({false, p->data_, page_id, std::move(promise)});
    future.get();  // block
    page_latch_[find].unlock();

    //    l.lock();
    //    page_table_.insert({page_id, fid});
    //    replacer_->SetEvictable(fid, false);
    //    replacer_->RecordAccess(fid, access_type);
    //    l.unlock();

  } else {
    //    std::cout << "fetch in " << f->second << " " << pages_[f->second].is_dirty_ << std::endl;
    p = pages_ + f->second;

    page_latch_[f->second].lock();
    pages_[f->second].pin_count_++;
    page_latch_[f->second].unlock();
    replacer_->SetEvictable(f->second, false);
    replacer_->RecordAccess(f->second, access_type);
  }
  //  latch_.unlock();
  // pagetable, freelist需要latch_，pages_/replacer不需要
  //  std::cout << "fp " << p->page_id_ << " pin " << p->pin_count_ << std::endl;
  return p;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> l(latch_);
  //  latch_.lock();
  auto f = page_table_.find(page_id);
  if (f == page_table_.end()) {
    std::cout << "unpin false table " << page_id << std::endl;
    //    latch_.unlock();
    return false;
  }
  page_latch_[f->second].lock();
  if (pages_[f->second].pin_count_ == 0) {
    page_latch_[f->second].unlock();
    std::cout << "unpin false pincount " << page_id << std::endl;
    return false;
  }
  //  std::cout << "unpin " << f->second << " " << is_dirty << std::endl;
  //  std::cout << "unpin " << page_id << " pin " << pages_[f->second].pin_count_ << std::endl;
  pages_[f->second].pin_count_--;
  if (!pages_[f->second].is_dirty_ && is_dirty) {
    pages_[f->second].is_dirty_ = is_dirty;
  }
  if (pages_[f->second].pin_count_ == 0) {
    //    std::cout << page_id << " unpin\n";
    replacer_->SetEvictable(f->second, true);
  }
  page_latch_[f->second].unlock();
  //  latch_.unlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> l(latch_);
  auto f = page_table_.find(page_id);
  if (f == page_table_.end()) {
    return false;
  }
  page_latch_[f->second].lock();
  l.unlock();
  //  std::cout << "flush " << f->second << std::endl;
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();  // 不能直接promise.getfuture.get，因为下面被move了
  disk_scheduler_->Schedule({true, pages_[f->second].data_, page_id, std::move(promise)});
  future.get();
  pages_[f->second].is_dirty_ = false;
  page_latch_[f->second].unlock();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> l(latch_);
  // map 遍历
  //  latch_.lock();
  //  std::cout << "flush all\n";
  for (auto &it : page_table_) {
    page_latch_[it.second].lock();
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();  // 不能直接promise.getfuture.get，因为下面被move了
    disk_scheduler_->Schedule({true, pages_[it.second].data_, it.first, std::move(promise)});
    future.get();

    pages_[it.second].is_dirty_ = false;
    page_latch_[it.second].unlock();
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> l(latch_);
  auto f = page_table_.find(page_id);
  if (f == page_table_.end()) {
    return true;
  }
  page_latch_[f->second].lock();
  if (pages_[f->second].pin_count_ > 0) {
    page_latch_[f->second].unlock();
    //    latch_.unlock();
    return false;
  }
  auto fid = f->second;
  replacer_->Remove(f->second);
  pages_[fid].ResetMemory();
  pages_[fid].pin_count_ = 0;
  pages_[fid].page_id_ = INVALID_PAGE_ID;
  pages_[fid].is_dirty_ = false;
  free_list_.push_back(f->second);
  page_table_.erase(page_id);
  DeallocatePage(page_id);
  page_latch_[fid].unlock();
  //  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto p = FetchPage(page_id);
  //  std::cout << "fetch page " << p->page_id_ << std::endl;
  return {this, p};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto p = FetchPage(page_id);
  //  printf("page rlock attempt %d\n", p->page_id_);
  p->RLatch();
  //  printf("page rlock %d\n", p->page_id_);
  //  auto r_guard = ReadPageGuard(this, p);
  return {this, p};
}
auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto p = FetchPage(page_id);
  //  printf("page wlock attempt %d\n", p->page_id_);
  p->WLatch();
  //  printf("page wlock %d\n", p->page_id_);
  //  auto w_guard = WritePageGuard
  return {this, p};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto p = NewPage(page_id);
  //  std::cout << "new page " << p->page_id_ << std::endl;
  return {this, p};
}

}  // namespace bustub
