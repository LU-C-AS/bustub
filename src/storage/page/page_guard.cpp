#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

void BasicPageGuard::Reset() {
  this->page_ = nullptr;
  this->bpm_ = nullptr;
  this->is_dirty_ = false;
}

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  if (this != &that) {
    this->Drop();
    this->page_ = that.page_;
    this->bpm_ = that.bpm_;
    this->is_dirty_ = that.is_dirty_;
    that.Reset();
    //        that.bpm_ = nullptr;
    //    that.page_ = nullptr;
    //    std::cout << "basic move " << page_->GetPageId() << std::endl;
  }
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    //    std::cout << "page " << page_->GetPageId() << std::endl;
    Reset();
    //    bpm_ = nullptr;
    //    page_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    this->Drop();
    //    std::cout << "=" << std::endl;
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.Reset();
    //    that.bpm_ = nullptr;
    //    that.page_ = nullptr;
  }
  return *this;
}

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  //  bpm_->FetchPage(page_->GetPageId());
  page_->RLatch();
  auto rpg = ReadPageGuard(bpm_, page_);
  //  auto rpd = std::make_unique<ReadPageGuard>(bpm_, page_);
  //  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  //  std::cout << "upgradeRead " << page_->GetPageId() << std::endl;
  //  Drop();
  Reset();
  return rpg;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  //  bpm_->FetchPage(page_->GetPageId());
  page_->WLatch();
  auto wpg = WritePageGuard(bpm_, page_);
  //  auto rpd = std::make_unique<ReadPageGuard>(bpm_, page_);
  //  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  //  std::cout << "upgradeWrite " << page_->GetPageId() << std::endl;
  Reset();
  //  Drop();
  return wpg;
}

BasicPageGuard::~BasicPageGuard() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    //    std::cout << "~basic " << page_->GetPageId() << std::endl;
  }
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  if (this != &that) {
    this->Drop();
    guard_ = std::move(that.guard_);
    //    this->page_ = that.page_;
    //    this->bpm_ = that.bpm_;
    //    this->is_dirty_ = that.is_dirty_;
    //    that.Reset();
    //        that.bpm_ = nullptr;
    //    that.page_ = nullptr;
    //    std::cout << "basic move " << guard_.page_->GetPageId() << std::endl;
  }
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    this->Drop();
    this->guard_ = std::move(that.guard_);
    //    std::cout << "read move " << guard_.page_->GetPageId() << std::endl;
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
    //    printf("page runlock %d\n", guard_.page_->GetPageId());
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() {
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
    // 因为析构的时候会调用guard自己的析构函数，因此不用unpin
    //     guard_.bpm_->UnpinPage(guard_.PageId(), guard_.is_dirty_);
    //    std::cout << "~read " << guard_.page_->GetPageId() << std::endl;
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  if (this != &that) {
    this->Drop();
    guard_ = std::move(that.guard_);
    //    std::cout << "write move " << guard_.page_->GetPageId() << std::endl;
  }
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    this->Drop();
    guard_ = std::move(that.guard_);
    //    std::cout << "write move " << guard_.page_->GetPageId() << std::endl;
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
    //    printf("page wunlock %d\n", guard_.page_->GetPageId());
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() {
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
    //    guard_.bpm_->UnpinPage(guard_.PageId(), guard_.is_dirty_);
    //    std::cout << "~write " << guard_.page_->GetPageId() << std::endl;
  }
}  // NOLINT

}  // namespace bustub
