#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  //  UNIMPLEMENTED("not implemented");
  if (base_meta.is_deleted_ && undo_logs.empty()) {
    //    std::cout << "base null\n";
    return std::nullopt;
  }
  if (undo_logs.empty()) {
    //    return std::nullopt;
    return std::make_optional<Tuple>(base_tuple);
  }
  auto temp_t = base_tuple;
  for (auto &ul : undo_logs) {
    //    uint32_t col = 0;
    if (ul.is_deleted_) {
      temp_t = {};
    } else {
      std::vector<Value> vals;
      std::vector<Column> cols;

      vals.reserve(schema->GetColumnCount());
      for (uint32_t i = 0; i < ul.modified_fields_.size(); i++) {
        if (ul.modified_fields_[i]) {
          cols.emplace_back(schema->GetColumn(i));
          //        vals.emplace_back(ul.tuple_.GetValue())
        }
      }
      if (cols.empty()) {
        continue;
      }
      auto partial_sch = Schema(cols);

      uint32_t col = 0;
      for (uint32_t i = 0; i < ul.modified_fields_.size(); i++) {
        if (ul.modified_fields_[i]) {
          vals.emplace_back(ul.tuple_.GetValue(&partial_sch, col++));
        } else {
          vals.emplace_back(temp_t.GetValue(schema, i));
        }
      }
      temp_t = Tuple(vals, schema);
    }
  }
  if (IsTupleContentEqual(temp_t, {})) {
    //    std::cout << "comp  null\n";
    return std::nullopt;
  }
  return std::make_optional<Tuple>(temp_t);
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  /*fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");
*/
  auto schema = table_info->schema_;

  //  auto itt = table_heap->MakeIterator();
  //  while (!itt.IsEnd()) {
  //    auto tuple = itt.GetTuple();
  //    auto rid = itt.GetRID();
  //    std::cout << rid.ToString() << " ";
  //    auto link_v = txn_mgr->GetVersionLink(rid);
  //    auto link = link_v.value().prev_;
  //    while (link.IsValid()) {
  //      std::cout << link.prev_txn_ << std::endl;
  //      auto cur_log = txn_mgr->GetUndoLog(link);
  //      std::vector<Column> cols;
  //      uint32_t i = 0;
  //      for (auto m : cur_log.modified_fields_) {
  //        if (m) {
  //          cols.emplace_back(schema.GetColumn(i++));
  //        }
  //      }
  //      auto log_sch = Schema(cols);
  //      std::cout << cur_log.is_deleted_ << cur_log.tuple_.ToString(&log_sch) << " " << cur_log.ts_ << " "
  //                << link.prev_txn_ << std::endl;
  //      link = cur_log.prev_version_;
  //    }
  //    ++itt;
  //  }

  std::unique_lock<std::shared_mutex> lck(txn_mgr->txn_map_mutex_);
  for (auto &it : txn_mgr->txn_map_) {
    std::cout << "txn_id " << it.first << std::endl;
    for (uint32_t i = 0; i < it.second->GetUndoLogNum(); i++) {
      //      std::vector<Column> cols;
      //
      //      for (auto m : it.second->GetUndoLog(i).modified_fields_) {
      //        if (m) {
      //          cols.emplace_back(schema.GetColumn(i));
      //        }
      //      }
      //      auto log_sch = Schema(cols);
      std::cout << "i " << i << " ts " << it.second->GetUndoLog(i).ts_ << std::endl;
      //                << it.second->GetUndoLog(i).tuple_.ToString(&log_sch) << std::endl;
      //      for (auto m : it.second->GetUndoLog(i).modified_fields_) {
      //        std::cout << m.operator bool() << " ";
      //      }
      //      std::cout << std::endl;
    }
  }

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

void ForUpdateAndDel(Transaction *txn, Tuple &log_tuple, std::vector<bool> &mod, timestamp_t log_ts, UndoLink *link) {
  // void ForUpdateAndDel(Transaction *txn, TransactionManager *txn_mgr, RID rid, Tuple &log_tuple, std::vector<bool>
  // &mod,
  //                      timestamp_t log_ts, bool &need_abort, UndoLink &link_ret) {
  bool has_log{false};
  uint32_t log_id;
  //  if (link_v.has_value()) {
  //    auto link_o = link_v.value();
  //    if (link_o.in_progress_) {
  //      need_abort = true;
  //      return;
  //    } else {
  //      link_ret = link_o.prev_;
  //      auto new_ver = VersionUndoLink{link_o.prev_, true};
  //      txn_mgr->UpdateVersionLink(rid, {new_ver}, nullptr);
  //    }
  //    link = link_o.prev_;
  //    //      auto  = link.prev_txn_;
  //    if (txn->GetTransactionId() == link.prev_txn_) {
  //      has_log = true;
  //      log_id = link.prev_log_idx_;
  //    }
  //  }
  if (txn->GetTransactionId() == link->prev_txn_) {
    has_log = true;
    log_id = link->prev_log_idx_;
  }

  UndoLog undolog;
  //  timestamp_t log_t = ts;
  if (has_log) {
    auto ori_log = txn->GetUndoLog(log_id);
    auto ts = ori_log.ts_;
    auto p_link = ori_log.prev_version_;

    undolog = UndoLog{false, mod, log_tuple, ts, p_link};
    //    }
    txn->ModifyUndoLog(log_id, undolog);
  } else {
    //        std::cout << link.IsValid() << " "
    //                  << "not has log\n";
    undolog = UndoLog{false, mod, log_tuple, log_ts, *link};

    txn->AppendUndoLog(undolog);
    log_id = txn->GetUndoLogNum() - 1;
  }
  //  std::cout << "DU " << undolog.ts_ << std::endl;

  link->prev_log_idx_ = log_id;
  link->prev_txn_ = txn->GetTransactionId();
}

void CheckVersion(TransactionManager *txn_mgr, Transaction *txn, const RID &rid, bool &need_abort, UndoLink &new_link,
                  TableInfo *table_info) {
  auto round = 10;
  while (round-- != 0) {
    auto t_meta = table_info->table_->GetTupleMeta(rid);
    if (t_meta.ts_ > txn->GetReadTs() && t_meta.ts_ != txn->GetTransactionTempTs()) {
      need_abort = true;
      return;
    }
    auto ori_link = txn_mgr->GetVersionLink(rid);
    //    if (!ori_link.has_value()) {
    //      auto gen_link = UndoLink{};
    //      auto new_ver = VersionUndoLink{gen_link, true};
    //      txn_mgr->UpdateVersionLink(rid, {new_ver}, nullptr);
    //    }
    //    if (ori_link->in_progress_) {
    //      need_abort = true;
    //      return;
    //    }
    auto new_ver = VersionUndoLink{new_link, true};
    auto ori_undo = ori_link.value();
    auto check = [ori_undo](std::optional<VersionUndoLink> ver) {
      auto link_v = ver.value();
      //    std::cout << link_v.prev_.prev_txn_ << " " << link.prev_txn_ << std::endl;
      if (link_v != ori_undo) {
        std::cout << "check link\n";
        return false;
      }
      return !link_v.in_progress_;
    };
    auto update = txn_mgr->UpdateVersionLink(rid, {new_ver}, check);
    //  std::cout << rid.ToString() << " " << need_abort << std::endl;
    if (!update) {
      if (round != 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      } else {
        need_abort = true;
        return;
      }
    } else {
      break;
    }
  }
  auto t_meta_2 = table_info->table_->GetTuple(rid).first;
  if (t_meta_2.ts_ > txn->GetReadTs() && t_meta_2.ts_ != txn->GetTransactionTempTs()) {
    need_abort = true;
    return;
  }
}

void GetExpr(std::vector<AbstractExpressionRef> &exprs, const AbstractExpressionRef &cur_e) {
  if (!cur_e->GetChildren().empty()) {
    for (auto &c : cur_e->GetChildren()) {
      GetExpr(exprs, c);
    }
  }
  exprs.emplace_back(cur_e);
}

void InsertTxn(Transaction *txn, TransactionManager *txn_mgr, std::vector<IndexInfo *> &inds, Tuple &child_tuple,
               const Schema &schema, TableInfo *table_info, bool &need_abort, bool is_update) {
  //  std::cout << "insert ";
  //  std::cout << "\t" << child_tuple.ToString(&schema) << std::endl;
  bool tuple_del{false};
  RID child_rid;
  for (auto ind : inds) {
    // 可能不对

    auto key_schema = ind->key_schema_;
    std::vector<uint32_t> col_ids;
    for (uint32_t i = 0; i < key_schema.GetColumnCount(); i++) {
      col_ids.emplace_back(schema.GetColIdx(key_schema.GetColumn(0).GetName()));
    }
    std::vector<RID> res;
    // index的锁
    ind->index_->ScanKey(child_tuple.KeyFromTuple(schema, key_schema, col_ids), &res, txn);
    if (res.size() == 1) {
      //        auto r_guard = table_info->table_->AcquireTablePageReadLock(res[0]);
      auto t_pair = table_info->table_->GetTuple(res[0]);
      if (!t_pair.first.is_deleted_ ||
          (t_pair.first.ts_ > txn->GetReadTs() && t_pair.first.ts_ != txn->GetTransactionTempTs())) {
        std::cout << "ins ind\n";
        need_abort = true;
        return;
      }
      tuple_del = true;
      child_rid = res[0];

      //        r_guard.Drop();
    }
  }

  auto ts = txn->GetTransactionTempTs();
  //    std::cout << "ts " << ts << std::endl;
  if (!tuple_del) {
    // 这个是否有必要

    auto opt_rid =
        table_info->table_->InsertTuple({ts, false}, child_tuple, nullptr, txn, table_info->oid_);  // 先不要锁
    child_rid = opt_rid.value();
    //    auto w_guard = table_info->table_->AcquireTablePageWriteLock(child_rid);
    UndoLink link{};
    VersionUndoLink new_ver = {link, true};
    //    auto lock = std::unique_lock<std::shared_mutex>(txn_mgr->version_info_mutex_, std::try_to_lock);
    txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
  } else {
    auto link_v = txn_mgr->GetVersionLink(child_rid);

    UndoLink link = link_v->prev_;

    auto t_pair = table_info->table_->GetTuple(child_rid);
    if (t_pair.first.ts_ == txn->GetTransactionTempTs()) {
      table_info->table_->UpdateTupleInPlace({ts, false}, child_tuple, child_rid, nullptr);

    } else {
      auto wset = txn->GetWriteSets();
      auto it = wset[table_info->oid_].find(child_rid);
      if (it == wset[table_info->oid_].end()) {
        CheckVersion(txn_mgr, txn, child_rid, need_abort, link, table_info);
        if (need_abort) {
          std::cout << "ind check\n";
          return;
        }
      }
      auto log_ts = t_pair.first.ts_;  // 不对
      std::vector<bool> mod;
      mod.insert(mod.end(), schema.GetColumnCount(), true);

      auto undolog = UndoLog{true, mod, t_pair.second, log_ts, link};
      auto undolink = txn->AppendUndoLog(undolog);
      //      auto undolink = UndoLink{txn->GetTransactionId(), logi};
      auto new_versionlink = VersionUndoLink{undolink, true};

      txn_mgr->UpdateVersionLink(child_rid, new_versionlink, nullptr);
      table_info->table_->UpdateTupleInPlace({ts, false}, child_tuple, child_rid, nullptr);

      //      }
    }
  }
  txn->AppendWriteSet(table_info->oid_, child_rid);
  // 更新index
  if (!tuple_del) {
    for (auto ind : inds) {
      // 可能不对
      auto key_schema = ind->key_schema_;
      std::vector<uint32_t> col_ids;
      for (uint32_t i = 0; i < key_schema.GetColumnCount(); i++) {
        col_ids.emplace_back(schema.GetColIdx(key_schema.GetColumn(0).GetName()));
      }

      bool is_suc = ind->index_->InsertEntry(child_tuple.KeyFromTuple(schema, key_schema, col_ids), child_rid, txn);
      //      std::cout << "index " << key_schema.GetColumn(0).GetName() << std::endl;
      if (!is_suc) {
        need_abort = true;
        return;
      }
    }
  }
}

void DelTxn(Transaction *txn, TransactionManager *txn_mgr, const RID &child_rid, const Tuple &child_tuple,
            const Schema &schema, TableInfo *table_info, bool &need_abort, bool is_update) {
  auto t_meta = table_info->table_->GetTuple(child_rid).first;
  if (t_meta.ts_ > txn->GetReadTs() && t_meta.ts_ != txn->GetTransactionTempTs()) {
    need_abort = true;
    return;
  }
  //  std::cout << "del";
  //  std::cout << "\t" << child_rid.ToString() << std::endl;

  auto link_v = txn_mgr->GetVersionLink(child_rid);
  auto link = link_v->prev_;

  //  link = link_o.prev_;
  // 要检查有没有自己的undolog
  if (t_meta.ts_ == txn->GetTransactionTempTs()) {
    // 如果之前insert的，被自己刚刚update，那么直接设置就可以，因为update的log可以恢复原状
    if (link.IsValid()) {
      table_info->table_->UpdateTupleMeta({t_meta.ts_, true}, child_rid);
    } else {
      // 自己刚insert的，直接设置0
      table_info->table_->UpdateTupleMeta({0, true}, child_rid);
    }

  } else {
    auto wset = txn->GetWriteSets();
    auto it = wset[table_info->oid_].find(child_rid);
    if (it == wset[table_info->oid_].end()) {
      CheckVersion(txn_mgr, txn, child_rid, need_abort, link, table_info);
      if (need_abort) {
        return;
      }
    }
    std::vector<bool> mod_fields;
    mod_fields.insert(mod_fields.end(), schema.GetColumnCount(), true);

    auto log_tuple = child_tuple;
    //    auto ori_link = link;
    ForUpdateAndDel(txn, log_tuple, mod_fields, t_meta.ts_, &link);

    auto new_ver = VersionUndoLink{link, true};
    txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
    //    std::cout << link.prev_txn_ << std::endl;
    table_info->table_->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, child_rid);
    txn->AppendWriteSet(table_info->oid_, child_rid);
    //    auto lock = std::unique_lock<std::shared_mutex>(txn_mgr->version_info_mutex_);
    //    if (!is_update) {

    //    }
  }
}

void MyAbort(Transaction *txn, TransactionManager *txn_mgr) {
  //  std::cout << "myabort " << txn->GetTransactionIdHumanReadable() << std::endl;

  auto w_set = txn->GetWriteSets();
  for (const auto &it : w_set) {
    //    auto table_oid = it.first;
    for (auto rid : it.second) {
      //      auto table_info = catalog_->GetTable(table_oid);
      //      auto ori_meta = table_info->table_->GetTupleMeta(rid);
      //      table_info->table_->UpdateTupleMeta({ts, ori_meta.is_deleted_}, rid);

      auto link = txn_mgr->GetVersionLink(rid);
      auto new_ver = VersionUndoLink{link->prev_, false};
      txn_mgr->UpdateVersionLink(rid, {new_ver}, nullptr);
    }
  }
  //  auto table_iter = tableInfo->table_->MakeEagerIterator();
  //  while (!table_iter.IsEnd()) {
  //    auto rid = table_iter.GetRID();
  //    auto link_n = txn_mgr->GetVersionLink(rid);
  //    if (link_n->prev_.prev_txn_ == txn->GetTransactionId()) {
  //      auto link_ori = txn->GetUndoLog(link_n->prev_.prev_log_idx_).prev_version_;
  //      auto new_ver = VersionUndoLink{link_ori};
  //      txn_mgr->UpdateVersionLink(rid, {new_ver}, nullptr);
  //    }
  //    ++table_iter;
  //  }
}

}  // namespace bustub
