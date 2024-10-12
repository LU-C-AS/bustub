//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  auto header_guard = bpm_->NewPageGuarded(&header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth);
  //  header_page_id_ = INVALID_PAGE_ID;
  //  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  int32_t dir_page_id;
  int32_t buck_page_id;
  auto hash = Hash(key);
  {
    auto header_guard = bpm_->FetchPageRead(header_page_id_);
    auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
    auto dir_idx = header_page->HashToDirectoryIndex(hash);
    dir_page_id = header_page->GetDirectoryPageId(dir_idx);
    if (dir_page_id == INVALID_PAGE_ID) {
      return false;
    }
  }
  {
    auto dir_guard = bpm_->FetchPageRead(dir_page_id);
    auto dir_page = dir_guard.template As<ExtendibleHTableDirectoryPage>();
    auto buck_idx = dir_page->HashToBucketIndex(hash);
    buck_page_id = dir_page->GetBucketPageId(buck_idx);
    if (buck_page_id == INVALID_PAGE_ID) {
      return false;
    }
  }
  {
    auto buck_guard = bpm_->FetchPageRead(buck_page_id);
    auto buck_page = buck_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
    V val;
    auto success = buck_page->Lookup(key, val, cmp_);

    if (!success) {
      return false;
    }

    result->emplace_back(val);
    return true;
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  //  std::cout << "insert" << key << std::endl;
  page_id_t dir_page_id;
  page_id_t buck_page_id;
  //  ExtendibleHTableHeaderPage *header_page;
  //  ExtendibleHTableDirectoryPage *dir_page;
  //  ExtendibleHTableBucketPage<K, V, KC> *buck_page;
  auto hash = Hash(key);
  //  std::cout << hash << std::endl;
  {
    WritePageGuard header_guard;

    header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
    //    printf("header page %d\n", header_page_id_);
    //    header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
    //    header_page->Init(header_max_depth_);
    auto dir_idx = header_page->HashToDirectoryIndex(hash);
    //    std::cout << "dir_idx " << dir_idx << std::endl;
    dir_page_id = header_page->GetDirectoryPageId(dir_idx);
    if (dir_page_id == INVALID_PAGE_ID) {
      //      std::cout << "create dir\n";
      auto dir_basic_guard = bpm_->NewPageGuarded(&dir_page_id);
      header_page->SetDirectoryPageId(dir_idx, dir_page_id);
      auto dir_basic_page = dir_basic_guard.AsMut<ExtendibleHTableDirectoryPage>();
      dir_basic_page->Init(directory_max_depth_);
    }
  }
  {
    auto dir_guard = bpm_->FetchPageWrite(dir_page_id);
    auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
    //    printf("gd %d\n", dir_page->GetGlobalDepth());
    auto buck_idx = dir_page->HashToBucketIndex(hash);
    buck_page_id = dir_page->GetBucketPageId(buck_idx);
    //    printf("insert buck_page %d\n", buck_page_id);
    if (buck_page_id == INVALID_PAGE_ID) {
      auto buck_basic_guard = bpm_->NewPageGuarded(&buck_page_id);
      //      printf("buck_idx %d ", buck_idx);
      dir_page->SetBucketPageId(buck_idx, buck_page_id);
      auto buck_basic_page = buck_basic_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      buck_basic_page->Init(bucket_max_size_);
    }

    //    auto buck_guard = bpm_->FetchPageWrite(buck_page_id);
    auto buck_guard = bpm_->FetchPageWrite(buck_page_id);
    //    printf("insert page %d\n", buck_page_id);
    auto buck_page = buck_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    if (!buck_page->IsFull()) {
      dir_guard.Drop();
      //      std::cout << "not full " << key << std::endl;
      return buck_page->Insert(key, value, cmp_);
    }
    while (buck_page->IsFull()) {
      //      std::cout << "full " << key << std::endl;
      auto ld = dir_page->GetLocalDepth(buck_idx);
      if (ld == directory_max_depth_) {
        //        std::cout << ld << " " << key << std::endl;
        return false;
      }
      //      printf("page %d\n", buck_page_id);
      page_id_t new_buck_page_id;
      auto new_buck_basic_guard = bpm_->NewPageGuarded(&new_buck_page_id);
      auto new_buck_guard = new_buck_basic_guard.UpgradeWrite();
      auto new_buck_page = new_buck_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      new_buck_page->Init(bucket_max_size_);

      auto new_buck_idx = dir_page->GetSplitImageIndex(buck_idx);
      // 插入一个新的bucket，并将原bucket中的rehash到两个新bucket中。
      dir_page->IncrLocalDepth(buck_idx);  // 是否要变 调整
      if (dir_page->GetLocalDepth(buck_idx) > dir_page->GetGlobalDepth()) {
        dir_page->IncrGlobalDepth();
      }
      //      auto new_buck_idx = dir_page->GetSplitImageIndex(buck_idx, true);

      dir_page->SetBucketPageId(new_buck_idx, new_buck_page_id);
      //      printf("new %d\n", new_buck_idx);
      // 调整原来哈希到一起的bucket
      auto old_buck_size = buck_page->Size();
      for (uint32_t i = 0; i < old_buck_size; i++) {
        auto entry = buck_page->EntryAt(i);
        auto kk = entry.first;
        auto hhash = Hash(kk);
        auto new_buckk_idx = dir_page->HashToBucketIndex(hhash);
        //        std::cout << "k " << kk << " hash " << hhash << std::endl;
        if (new_buckk_idx != buck_idx) {
          //          std::cout << "to new " << kk << std::endl;
          buck_page->RemoveAt(i);
          new_buck_page->Insert(entry.first, entry.second, cmp_);
        }
      }
      auto ins_buck_idx = dir_page->HashToBucketIndex(hash);

      if (ins_buck_idx == new_buck_idx) {
        //        std::cout << " buck page" << new_buck_idx << std::endl;
        buck_guard = std::move(new_buck_guard);
        buck_page = new_buck_page;
        buck_idx = new_buck_idx;
        buck_page_id = new_buck_page_id;
      }
    }
    dir_guard.Drop();
    return buck_page->Insert(key, value, cmp_);
    //    std::cout << "insert " << key << std::endl;
  }
  //  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  //  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  //  std::cout << "remove " << key << std::endl;

  page_id_t dir_page_id;
  page_id_t buck_page_id;
  auto hash = Hash(key);
  {
    ReadPageGuard header_guard;
    header_guard = bpm_->FetchPageRead(header_page_id_);
    auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
    auto dir_idx = header_page->HashToDirectoryIndex(hash);
    dir_page_id = header_page->GetDirectoryPageId(dir_idx);
    if (dir_page_id == INVALID_PAGE_ID) {
      return false;
    }
  }
  {
    auto dir_guard = bpm_->FetchPageWrite(dir_page_id);
    auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
    auto buck_idx = dir_page->HashToBucketIndex(hash);

    buck_page_id = dir_page->GetBucketPageId(buck_idx);
    //    std::cout << "remove page " << buck_page_id << "idx " << buck_idx << std::endl;
    if (buck_page_id == INVALID_PAGE_ID) {
      return false;
      //      throw Exception("remove:buck not found\n");
    }
    {
      auto buck_guard = bpm_->FetchPageWrite(buck_page_id);
      auto buck_page = buck_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      auto f = buck_page->Remove(key, cmp_);
      if (!f) {
        return false;
      }
      if (buck_page->IsEmpty()) {
        buck_guard.Drop();
        //        auto split_buck_idx = buck_idx ^ (1 << (dir_page->GetGlobalDepth() - 1));
        // buck_page在循环中不一定保持empty
        while (dir_page->GetLocalDepth(buck_idx) != 0) {
          auto old_split_idx = buck_idx ^ (1 << (dir_page->GetLocalDepth(buck_idx) - 1));
          auto old_split_page_id = dir_page->GetBucketPageId(old_split_idx);
          //          auto split_buck_idx = dir_page->GetSplitImageIndex(buck_idx);
          auto ld = dir_page->GetLocalDepth(buck_idx);
          auto gd = dir_page->GetGlobalDepth();
          if (ld == gd) {
            //            std::cout << "ld=gd " << ld << " " << buck_idx << " " << old_split_idx << std::endl;

            BUSTUB_ASSERT(dir_page->GetLocalDepth(buck_idx) == dir_page->GetLocalDepth(old_split_idx),
                          "remove ld=gd false\n");

            if (old_split_page_id != INVALID_PAGE_ID) {
              auto buck_rguard = bpm_->FetchPageRead(buck_page_id);
              auto buck_page0 = buck_rguard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
              auto old_split_rguard = bpm_->FetchPageRead(old_split_page_id);
              auto split_page = old_split_rguard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
              if (!buck_page0->IsEmpty() && !split_page->IsEmpty()) {
                break;
              }
              dir_page->DecrLocalDepth(buck_idx);
              dir_page->DecrLocalDepth(old_split_idx);
              if (split_page->IsEmpty()) {
                old_split_rguard.Drop();
                bpm_->DeletePage(old_split_page_id);
                dir_page->SetBucketPageId(old_split_idx, buck_page_id);

              } else {
                if (buck_page0->IsEmpty()) {
                  bpm_->DeletePage(buck_page_id);
                  dir_page->SetBucketPageId(buck_idx, old_split_page_id);
                } else {
                  if (dir_page->CanShrink()) {
                    dir_page->DecrGlobalDepth();
                  }
                  break;
                }
              }
            } else {
              dir_page->DecrLocalDepth(buck_idx);
              dir_page->DecrLocalDepth(old_split_idx);
              dir_page->SetBucketPageId(old_split_idx, buck_page_id);
            }
            buck_idx = buck_idx < old_split_idx ? buck_idx : old_split_idx;
            buck_page_id = dir_page->GetBucketPageId(buck_idx);

            if (dir_page->CanShrink()) {
              dir_page->DecrGlobalDepth();
            }
          } else {
            if (dir_page->GetLocalDepth(old_split_idx) != dir_page->GetLocalDepth(buck_idx)) {
              break;
            }
            bpm_->DeletePage(buck_page_id);
            BUSTUB_ASSERT(old_split_page_id != INVALID_PAGE_ID, "old split invalid");
            auto st = buck_idx < old_split_idx ? buck_idx : old_split_idx;
            auto step = 1 << (1 << dir_page->GetLocalDepth(st));
            for (uint32_t i = st; i < dir_page->Size(); i += step) {
              dir_page->SetBucketPageId(i, old_split_page_id);
              dir_page->DecrLocalDepth(i);
            }
            buck_idx = buck_idx < old_split_idx ? buck_idx : old_split_idx;

            auto old_split_rguard = bpm_->FetchPageRead(old_split_page_id);
            auto split_page = old_split_rguard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
            if (!split_page->IsEmpty()) {
              //              std::cout << "split !empty\n";
              break;
            }
            old_split_rguard.Drop();
          }
        }
      }
    }
  }

  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
