//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  max_depth_ = max_depth;
  global_depth_ = 0;
  for (uint64_t i = 0; i < HTABLE_DIRECTORY_ARRAY_SIZE; i++) {
    local_depths_[i] = 0;
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  auto ind = hash & ((1 << global_depth_) - 1);
  return ind;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  if (bucket_idx > Size()) {
    return INVALID_PAGE_ID;
  }
  return bucket_page_ids_[bucket_idx];
}

// 加一个每次set完要调整一下，如果有其余idx也应指向该页。
// 每次overflow插入时最多只需要新增一个bucket。e.g.：ld(2->3)，则00映射的bucket变为000和100，只多一个。
void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  bucket_page_ids_[bucket_idx] = bucket_page_id;
  auto size = Size();
  //  printf("set %d %d size %d\n", bucket_idx, bucket_page_id, size);
  auto step = 1 << local_depths_[bucket_idx];
  //  auto span = step * 2;
  // st为取原local_depth_位的LSB
  auto st = bucket_idx & ((1 << (step / 2)) - 1);
  for (uint32_t i = st; i < size; i += step) {
    if (bucket_page_ids_[i] == INVALID_PAGE_ID) {
      bucket_page_ids_[i] = bucket_page_ids_[bucket_idx];
    }
  }
  //  for (uint32_t k = 0; k < size; k++) {
  //    printf("%d: page %d d %d  ", k, bucket_page_ids_[k], local_depths_[k]);
  //  }
  //  printf("\n");
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  //  if (local_depths_[bucket_idx] == global_depth_) {
  //    return UINT32_MAX;
  //  }
  uint32_t ind;
  //  if (incr) {
  //    ind = bucket_idx ^ (1 << local_depths_[bucket_idx]);
  //  } else {
  ind = bucket_idx ^ (1 << local_depths_[bucket_idx]);
  //  }
  return ind;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  //  printf("incr global %d\n", global_depth_);
  if (global_depth_ == max_depth_) {
    //    throw Exception("global depth overflow\n");
    return;
  }
  global_depth_++;
  // rehash

  uint32_t size = 1 << global_depth_;
  for (uint32_t i = 0; i < size / 2; i++) {
    if (local_depths_[i] == global_depth_) {
      // 如果先incrLD，则那时dir还未扩张，它的原split image的ld需要设置
      auto span = 1 << (global_depth_ - 1);
      local_depths_[i + span] = global_depth_;
      continue;
    }

    auto step = 1 << local_depths_[i];  // 将增加后
    auto j = size / 2 + i;
    for (; j < size; j += step) {
      local_depths_[j] = local_depths_[i];

      bucket_page_ids_[j] = bucket_page_ids_[i];
    }
  }
  //  for (uint32_t k = 0; k < size; k++) {
  //    printf("%d: page %d ld %d  ", k, bucket_page_ids_[k], local_depths_[k]);
  //  }
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  //  printf("decr global %d\n", global_depth_);
  global_depth_--;
  auto ori_size = 1 << (global_depth_ + 1);
  for (auto i = 1 << global_depth_; i < ori_size; i++) {
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (uint64_t i = 0; i < Size(); i++) {
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  //  printf("set local %d %d\n", bucket_idx, local_depths_[bucket_idx]);
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  //  printf("incr local %d %d\n", bucket_idx, local_depths_[bucket_idx]);
  if (local_depths_[bucket_idx] == max_depth_) {
    return;
  }
  local_depths_[bucket_idx]++;
  auto size = Size();
  // step表示idx & 1<<原ld，这些idx是原先指向同一个页的，现在这些idx如果idx & 1<<原ld == 0，则说明应该指向INVALID
  auto step = 1 << (local_depths_[bucket_idx] - 1);
  // span表示idx & 1<<现ld，应当有这么多idx指向该页
  auto span = step * 2;
  // st为取原local_depth_位的LSB
  auto st = bucket_idx & ((1 << (step / 2)) - 1);
  auto p = bucket_page_ids_[bucket_idx];
  for (uint32_t i = st; i < size; i += step) {
    local_depths_[i] = local_depths_[bucket_idx];

    if ((i - st) % span == 0) {
      bucket_page_ids_[i] = p;
    } else {
      bucket_page_ids_[i] = INVALID_PAGE_ID;
    }
  }

  //  for (uint32_t k = 0; k < size; k++) {
  //    printf("%d: page %d ld %d  ", k, bucket_page_ids_[k], local_depths_[k]);
  //  }
  // 调整
}

// 只有该页为空才会调用
void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  //  printf("decr local %d %d\n", bucket_idx, local_depths_[bucket_idx]);
  //  throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  if (local_depths_[bucket_idx] == 0) {
    return;
  }
  local_depths_[bucket_idx]--;
}

}  // namespace bustub