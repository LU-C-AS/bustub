//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

const int INVALID_REC = -1;

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::SetInvalid(uint32_t i) {
  //  for (uint32_t i = 0; i < max_size_; i++) {
  if constexpr (std::is_integral_v<K>) {
    array_[i] = std::pair(INVALID_REC, INVALID_REC);
  } else {
    array_[i].first.SetFromInteger(INVALID_REC);
  }
  //  }
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsInvalid(uint32_t i) -> bool {
  if constexpr (std::is_integral_v<K>) {
    return array_[i].first == INVALID_REC;
  } else {
    int64_t k = INVALID_REC;
    return memcmp(array_[i].first.data_, &k, sizeof(k)) == 0;
  }
}
template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  //  throw NotImplementedException("ExtendibleHTableBucketPage not implemented");
  size_ = 0;
  max_size_ = max_size;
  for (uint32_t i = 0; i < max_size_; i++) {
    //    auto key = -1, value = -1;
    //    std::pair p(-1, -1);
    //    i = std::nullopt;
    SetInvalid(i);
    //    auto p = std::make_pair<K, V>();
    //    i = std::pair<K, V>();
    //    i.first = GenericKey(-1);
  }
  //  memset(array_, -1, sizeof(array_));  //
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  // 可以有序(小到大），然后二分查找
  //  uint32_t l = 0;
  //  uint32_t r = size_;
  //  while (l < r) {
  //    auto m = (l + r) / 2;
  //    auto res = cmp(array_[m].first, key);
  //    if (res == 0) {
  //      value = array_[m].second;
  //      return true;
  //    } else if (res > 0) {
  //      l = m + 1;
  //    } else {
  //      r = m;
  //    }
  //  }
  //  return false;
  for (uint32_t i = 0; i < max_size_; i++) {
    if (cmp(array_[i].first, key) == 0) {
      value = array_[i].second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  if (size_ == max_size_) {
    return false;
  }
  //  int32_t l = 0;
  //  int32_t r = size_;
  //  // 左闭右开
  //  while (l < r) {
  //    auto m = (l + r) / 2;
  //    auto res = cmp(array_[m].first, key);
  //    if (res == 0) {
  //      return false;
  //    } else if (res > 0) {
  //      l = m + 1;
  //    } else {
  //      r = m;
  //    }
  //  }
  //  std::cout << "l " << l << " size " << size_ << " " << max_size_ << std::endl;
  //  std::cout << key << std::endl;
  //  //  if (size_ != 0) {
  //  for (int i = size_ - 1; i >= l; i--) {
  //    std::cout << i << std::endl;
  //    array_[i + 1] = array_[i];
  //    //    if (i == 0) {
  //    //      break;
  //    //    }
  //    //        std::pair(array_[i].first, array_[i].second);
  //    //    }
  //  }
  //  array_[l] = std::pair<K, V>(key, value);
  //  //  array_[l].first = key;
  //  //  array_[l].second = value;
  //  size_++;
  //  return true;
  uint32_t ins = UINT32_MAX;
  for (uint32_t i = 0; i < max_size_; i++) {
    if (cmp(array_[i].first, key) == 0) {
      //      std::cout << key << std::endl;
      return false;
    }
    //    auto p = std::pair<K, V>();
    if (ins == UINT32_MAX && IsInvalid(i)) {
      ins = i;
    }
  }
  array_[ins] = std::pair(key, value);
  size_++;
  return true;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  //  uint32_t l = 0;
  //  uint32_t r = size_;
  //  while (l < r) {
  //    auto m = (l + r) / 2;
  //    auto res = cmp(array_[m].first, key);
  //    if (res == 0) {
  //      //      SetInvalid(m);
  //      for (auto i = m; i < size_ - 1; i++) {
  //        array_[i] = array_[i + 1];
  //      }
  //      size_--;
  //      return true;
  //    } else if (res > 0) {
  //      l = m + 1;
  //    } else {
  //      r = m;
  //    }
  //  }
  //  return false;
  for (uint32_t i = 0; i < max_size_; i++) {
    if (cmp(array_[i].first, key) == 0) {
      //      array_[i] = std::pair<K, V>();
      SetInvalid(i);
      size_--;
      return true;
    }
  }
  return false;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  //  throw NotImplementedException("ExtendibleHTableBucketPage not implemented");
  if (bucket_idx >= max_size_) {
    return;
  }
  SetInvalid(bucket_idx);
  size_--;
  //  array_[bucket_idx] = std::pair<K, V>();
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return size_ == max_size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return size_ == 0;
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub