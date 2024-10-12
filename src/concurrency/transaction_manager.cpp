//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.

  txn_ref->read_ts_ = last_commit_ts_.load();
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
  //  std::cout << "verify " << txn->GetTransactionIdHumanReadable() << std::endl;
  auto readts = txn->read_ts_.load();
  auto wset = txn->write_set_;
  if (wset.empty()) {
    return true;
  }
  std::vector<Transaction *> cfl_txns;
  std::shared_lock<std::shared_mutex> tl(txn_map_mutex_);
  for (auto &txn_it : txn_map_) {
    if (txn_it.second->commit_ts_ > readts) {
      cfl_txns.emplace_back(txn_it.second.get());
      //      std::cout << "add " << txn_it.second->GetTransactionIdHumanReadable() << std::endl;
    }
  }
  tl.unlock();

  for (auto &txn_it : cfl_txns) {
    for (auto &rid_it : txn_it->write_set_) {
      auto cur_table = catalog_->GetTable(rid_it.first);
      auto schema = cur_table->schema_;

      for (auto &r_it : rid_it.second) {
        auto cur_tuple = cur_table->table_->GetTuple(r_it);
        auto link_v = GetVersionLink(r_it);
        auto link = link_v.value().prev_;
        auto temp_t = cur_tuple.second;

        //        std::cout << "first" << temp_t.ToString(&schema);
        auto preds = txn->scan_predicates_[cur_table->oid_];
        std::cout << txn->GetTransactionIdHumanReadable() << " " << preds.empty() << std::endl;
        for (auto &p : preds) {
          std::cout << p->ToString();
          if (auto value = p->Evaluate(&temp_t, schema); value.GetAs<bool>()) {
            return false;
          }
        }

        do {
          auto ul = GetUndoLog(link);
          if (ul.ts_ < txn->read_ts_) {
            break;
          }
          if (ul.is_deleted_) {
            link = ul.prev_version_;
            continue;
          }
          //      auto ul = txn->GetUndoLog(link.prev_.);
          std::vector<Value> vals;
          std::vector<Column> cols;
          for (uint32_t i = 0; i < ul.modified_fields_.size(); i++) {
            if (ul.modified_fields_[i]) {
              cols.emplace_back(schema.GetColumn(i));
            }
          }

          auto partial_sch = Schema(cols);
          //      auto t_pair = table_info->table_->GetTuple(rid);
          //        auto temp_t = cur_tuple;
          uint32_t col = 0;
          for (uint32_t i = 0; i < ul.modified_fields_.size(); i++) {
            if (ul.modified_fields_[i]) {
              vals.emplace_back(ul.tuple_.GetValue(&partial_sch, col++));
            } else {
              vals.emplace_back(temp_t.GetValue(&schema, i));
            }
          }
          temp_t = Tuple{vals, &schema};

          std::cout << temp_t.ToString(&schema);
          //          auto preds = txn->scan_predicates_[cur_table->oid_];
          std::cout << txn->GetTransactionIdHumanReadable() << " " << preds.empty() << std::endl;
          for (auto &p : preds) {
            std::cout << p->ToString();
            if (auto value = p->Evaluate(&temp_t, schema); value.GetAs<bool>()) {
              return false;
            }
          }

          link = ul.prev_version_;
        } while (link.IsValid());
      }
    }
  }
  return true;
}

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!

  auto ts = last_commit_ts_ + 1;
  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!

  auto set = txn->GetWriteSets();
  for (const auto &it : set) {
    auto table_oid = it.first;
    for (auto rid : it.second) {
      auto table_info = catalog_->GetTable(table_oid);
      auto ori_meta = table_info->table_->GetTupleMeta(rid);
      table_info->table_->UpdateTupleMeta({ts, ori_meta.is_deleted_}, rid);

      auto link = GetVersionLink(rid);
      auto new_ver = VersionUndoLink{link->prev_, false};
      UpdateVersionLink(rid, {new_ver}, nullptr);
    }
  }
  //  for (auto it = txn->undo_logs_.begin(); it != txn->undo_logs_.end(); it++) {
  //    if (it->ts_ == txn->GetTransactionTempTs()) {
  //      it->ts_ = ts;
  //    }
  //  }
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.

  last_commit_ts_++;
  txn->commit_ts_ = ts;
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!
  auto set = txn->GetWriteSets();
  for (const auto &it : set) {
    auto table_oid = it.first;
    auto table_info = catalog_->GetTable(table_oid);
    auto schema = table_info->schema_;
    for (auto rid : it.second) {
      //      auto w_guard = table_info->table_->AcquireTablePageWriteLock(rid);
      auto link_v = GetVersionLink(rid);
      auto link = link_v.value().prev_;
      if (!link.IsValid()) {
        // insert进来的
        table_info->table_->UpdateTupleMeta({0, true}, rid);

        auto new_ver = VersionUndoLink{link, false};
        UpdateVersionLink(rid, {new_ver}, nullptr);
        continue;
      }
      if (link.prev_txn_ != txn->GetTransactionId()) {
        throw ExecutionException("abort fail\n");
      }
      auto ul = txn->GetUndoLog(link.prev_log_idx_);
      std::vector<Value> vals;
      std::vector<Column> cols;
      for (uint32_t i = 0; i < ul.modified_fields_.size(); i++) {
        if (ul.modified_fields_[i]) {
          cols.emplace_back(schema.GetColumn(i));
        }
      }
      if (cols.empty()) {
        std::cout << "abort col empty\n";
        continue;
      }
      auto partial_sch = Schema(cols);
      auto t_pair = table_info->table_->GetTuple(rid);
      auto temp_t = t_pair.second;
      uint32_t col = 0;
      for (uint32_t i = 0; i < ul.modified_fields_.size(); i++) {
        if (ul.modified_fields_[i]) {
          vals.emplace_back(ul.tuple_.GetValue(&partial_sch, col++));
        } else {
          vals.emplace_back(temp_t.GetValue(&schema, i));
        }
      }
      temp_t = Tuple{vals, &schema};

      table_info->table_->UpdateTupleInPlace({ul.ts_, ul.is_deleted_}, temp_t, rid, nullptr);

      //      auto ori_meta = table_info->table_->GetTupleMeta(rid);
      //      table_info->table_->UpdateTupleMeta({ts, ori_meta.is_deleted_}, rid);
      //
      //      auto link = GetVersionLink(rid);
      auto new_ver = VersionUndoLink{link, false};
      UpdateVersionLink(rid, {new_ver}, nullptr);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // UNIMPLEMENTED("not implemented"); }

  auto watermark = running_txns_.GetWatermark();
  std::vector<Transaction *> txns;

  //  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  std::set<txn_id_t> del_txns;
  for (auto &t : txn_map_) {
    if (t.second->state_ == TransactionState::COMMITTED || t.second->state_ == TransactionState::ABORTED) {
      // 如果读到的版本早于watermark，说明不需要保留它的任何undolog
      if (t.second->GetReadTs() < watermark) {
        del_txns.insert(t.first);
      } else {
        bool flag{true};
        auto txn = t.second.get();
        for (uint32_t i = 0; i < t.second->GetUndoLogNum(); i++) {
          if (t.second->GetUndoLog(i).ts_ >= watermark) {
            flag = false;
            break;
          }
        }
        if (!flag) {
          continue;
        }
        //      flag = true;
        for (auto &tbl : txn->write_set_) {
          //        auto table = catalog_->GetTable(tbl.first);
          for (auto rid : tbl.second) {
            //          auto tuple = table->table_->GetTuple(rid);
            auto link_o = GetUndoLink(rid);
            if (!link_o.has_value()) {
              std::cout << "link_o " << std::endl;
            }
            auto link = link_o.value();
            while (link.prev_txn_ != t.first && link.IsValid()) {
              auto undolog = GetUndoLog(link);
              if (undolog.ts_ < watermark) {
                break;
              }
              link = undolog.prev_version_;
            }
            if (link.prev_txn_ == t.first) {
              flag = false;
              break;
            }
          }
          if (!flag) {
            break;
          }
        }

        if (flag) {
          del_txns.insert(t.first);
        }
      }
    }

    //    if (t.second->state_ == TransactionState::RUNNING && t.second->read_ts_ == watermark) {
    //      std::cout << "GC " << t.first << " ts " << watermark << std::endl;
    //      txns.emplace_back(t.second.get());
    //    }
  }
  for (auto txn : del_txns) {
    txn_map_.erase(txn);
  }
  // watermark访问不到的
  //  std::set<txn_id_t> access_txns;
  //  for (auto txn : txns) {
  //    for (auto w_it : txn->scan_predicates_) {
  //      auto table = catalog_->GetTable(w_it.first);
  //      std::cout << "table " << w_it.first << std::endl;
  //      auto iter = table->table_->MakeIterator();
  //      while (!iter.IsEnd()) {
  //        auto rid = iter.GetRID();
  //        auto link_o = GetUndoLink(rid);
  //        if (!link_o.has_value()) {
  //          std::cout << "!value\n";
  //          ++iter;
  //          continue;
  //        }
  //        auto link = link_o.value();
  //        while (link.IsValid()) {
  //          auto undolog = GetUndoLog(link);
  //          if (undolog.ts_ < txn->GetReadTs() && (txn_map_[link.prev_txn_]->state_ == TransactionState::COMMITTED
  //          ||
  //                                                 txn_map_[link.prev_txn_]->state_ == TransactionState::ABORTED))
  //                                                 {
  //            break;
  //          } else {
  //            std::cout << "insert " << link.prev_txn_ << std::endl;
  //            del_txns.erase(link.prev_txn_);
  //            access_txns.insert(link.prev_txn_);
  //          }
  //          link = undolog.prev_version_;
  //        }
  //        link = GetUndoLog(link).prev_version_;
  //        while (link.IsValid()) {
  //          std::cout << "insert " << link.prev_txn_ << std::endl;
  //          del_txns.insert(link.prev_txn_);
  //          link = GetUndoLog(link).prev_version_;
  //          //          if (txn_map_[link.prev_txn_]->state_ == TransactionState::COMMITTED) {
  //          //            break;
  //          //          }
  //          //          link = GetUndoLog(link).prev_version_;
  //        }
  //        ++iter;
  //      }
  //    }
  //  }
  //  std::set<txn_id_t> del_txns;
  //  for (auto txn_it : txn_map_) {
  //    if (access_txns.find(txn_it.first) == access_txns.end()) {
  //      del_txns.insert(txn_it.first);
  //      std::cout << "GC del " << txn_it.first << std::endl;
  //    }
  //  }
  //  for (auto txn : del_txns) {
  //    txn_map_.erase(txn);
  //  }
}

}  // namespace bustub
