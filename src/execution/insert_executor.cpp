//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // throw NotImplementedException("InsertExecutor is not implemented");
  child_executor_->Init();
  is_inserted_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_inserted_) {
    return false;
  }
  auto cata = exec_ctx_->GetCatalog();
  auto table_info = cata->GetTable(plan_->table_oid_);
  int num = 0;
  //  std::cout << "insert\n";
  while (true) {
    Tuple child_tuple{};
    RID child_rid;
    auto status = child_executor_->Next(&child_tuple, &child_rid);
    if (!status) {
      //      std::cout << "insert false1\n";
      break;
    }

    //    bool tuple_del{false};

    auto txn = exec_ctx_->GetTransaction();
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    auto table_schema = table_info->schema_;
    //    bool need_abort;
    //    auto link = txn_mgr->GetVersionLink(child_rid).value().prev_;
    //    CheckVersion(txn_mgr, txn, child_rid, need_abort, link, table_info);
    //    if (need_abort) {
    //      txn->SetTainted();
    //      throw ExecutionException("insert scan fail\n");
    //    }
    //
    //    auto inds = cata->GetTableIndexes(table_info->name_);
    //    for (auto ind : inds) {
    //      // 可能不对
    //      auto key_schema = ind->key_schema_;
    //      std::vector<uint32_t> col_ids;
    //      for (uint32_t i = 0; i < key_schema.GetColumnCount(); i++) {
    //        col_ids.emplace_back(table_schema.GetColIdx(key_schema.GetColumn(0).GetName()));
    //      }
    //      //      bool is_suc =
    //      //          ind->index_->InsertEntry(child_tuple.KeyFromTuple(table_schema, key_schema, col_ids),
    //      child_rid, txn); std::vector<RID> res;
    //      // index的锁
    //      ind->index_->ScanKey(child_tuple.KeyFromTuple(table_schema, key_schema, col_ids), &res, txn);
    //      if (res.size() == 1) {
    //        //        auto r_guard = table_info->table_->AcquireTablePageReadLock(res[0]);
    //        auto t_pair = table_info->table_->GetTuple(res[0]);
    //        if (!t_pair.first.is_deleted_ /*||
    //            (t_pair.first.ts_ > txn->GetReadTs() && t_pair.first.ts_ != txn->GetTransactionTempTs())*/) {
    //          txn->SetTainted();
    //          throw ExecutionException("insert scan fail\n");
    //        } else {
    //          tuple_del = true;
    //          child_rid = res[0];
    //        }
    //        //        r_guard.Drop();
    //      }
    //    }
    //
    //    auto txn_mgr = exec_ctx_->GetTransactionManager();
    //    //    auto txn = exec_ctx_->GetTransaction();
    //    auto ts = txn->GetTransactionTempTs();
    //    //    std::cout << "ts " << ts << std::endl;
    //    if (!tuple_del) {
    //      auto opt_rid =
    //          table_info->table_->InsertTuple({ts, false}, child_tuple, nullptr, txn, table_info->oid_);  //
    //          先不要锁
    //      child_rid = opt_rid.value();
    //      UndoLink link{};
    //      VersionUndoLink new_ver = {link, false};
    //      txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
    //    } else {
    //      //      auto link_v = txn_mgr->GetVersionLink(child_rid);
    //      //      UndoLink link;
    //      //      if (link_v.has_value()) {
    //      //        auto link_o = link_v.value();
    //      //        if (link_o.in_progress_) {
    //      //          //          w_guard.Drop();
    //      //          txn->SetTainted();
    //      //          throw ExecutionException("insert inprogress\n");
    //      //        } else {
    //      //          auto new_ver = VersionUndoLink{link_o.prev_, true};
    //      //          txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
    //      //        }
    //      //        link = link_o.prev_;
    //      //      } else {
    //      //        //        std::cout << "del link_v\n";
    //      //        throw ExecutionException("insert del link\n");
    //      //      }
    //      bool need_abort{false};
    //      bool is_con{false};
    //      UndoLink link;
    //      CheckVersion(txn_mgr, txn, child_rid, need_abort, is_con, link);
    //      if (need_abort) {
    //        txn->SetTainted();
    //        throw ExecutionException("seq inpro\n");
    //      }
    //      if (is_con) {
    //        //        ++(*next_iter_);
    //        continue;
    //      }
    //
    //      //      auto w_guard = table_info->table_->AcquireTablePageWriteLock(child_rid);
    //      auto t_pair = table_info->table_->GetTuple(child_rid);
    //      if (t_pair.first.ts_ == txn->GetTransactionTempTs()) {
    //        table_info->table_->UpdateTupleInPlace({ts, false}, child_tuple, child_rid, nullptr);
    //        auto new_ver = VersionUndoLink{link};
    //        txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
    //      } else {
    //        auto log_ts = t_pair.first.ts_;  // 不对
    //        std::vector<bool> mod;
    //        mod.insert(mod.end(), child_executor_->GetOutputSchema().GetColumnCount(), true);
    //        //        auto link_v = txn_mgr->GetVersionLink(child_rid);
    //        //        UndoLink link;
    //        //        if (link_v.has_value()) {
    //        //          auto link_o = link_v.value();
    //        //          if (link_o.in_progress_) {
    //        //            //          w_guard.Drop();
    //        //            txn->SetTainted();
    //        //            throw ExecutionException("insert inprogress\n");
    //        //          } else {
    //        //            auto new_ver = VersionUndoLink{link_o.prev_, true};
    //        //            txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
    //        //          }
    //        //          link = link_o.prev_;
    //        //        } else {
    //        //          std::cout << "del link_v\n";
    //        //        }
    //
    //        auto undolog = UndoLog{true, mod, t_pair.second, log_ts, link};
    //        txn->AppendUndoLog(undolog);
    //        auto undolink = UndoLink{txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum() - 1)};
    //        auto new_versionlink = VersionUndoLink{undolink};
    //        table_info->table_->UpdateTupleInPlace({ts, false}, child_tuple, child_rid, nullptr);
    //        txn_mgr->UpdateVersionLink(child_rid, new_versionlink, nullptr);
    //
    //        //      auto new_ver = VersionUndoLink{li, true};
    //        //      txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
    //      }
    //    }
    //    txn->AppendWriteSet(table_info->oid_, child_rid);

    auto inds = cata->GetTableIndexes(table_info->name_);
    bool need_abort{false};
    InsertTxn(txn, txn_mgr, inds, child_tuple, table_schema, table_info, need_abort);
    if (need_abort) {
      //      MyAbort(txn, txn_mgr);
      txn->SetTainted();
      throw ExecutionException("insert fail\n");
    }
    //    auto new_ver = VersionUndoLink{link, false};
    //    txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
    //    std::cout << "link " << b << std::endl;
    num++;
    // 更新index
    //    auto table_schema = table_info->schema_;

    //    auto inds = cata->GetTableIndexes(table_info->name_);
    //    if (!tuple_del) {
    //      for (auto ind : inds) {
    //        // 可能不对
    //        auto key_schema = ind->key_schema_;
    //        std::vector<uint32_t> col_ids;
    //        for (uint32_t i = 0; i < key_schema.GetColumnCount(); i++) {
    //          col_ids.emplace_back(table_schema.GetColIdx(key_schema.GetColumn(0).GetName()));
    //        }
    //
    //        bool is_suc =
    //            ind->index_->InsertEntry(child_tuple.KeyFromTuple(table_schema, key_schema, col_ids), child_rid, txn);
    //        //      std::cout << "index " << key_schema.GetColumn(0).GetName() << std::endl;
    //        if (!is_suc) {
    //          txn->SetTainted();
    //          throw ExecutionException("insert fail\n");
    //        }
    //      }
    //    }
  }
  //  if (num == 0) {
  //    return false;
  //  } else {
  //  std::vector<Column> p;
  //  p.push_back({"insert num", INTEGER});
  //  auto schema = Schema(p);

  std::vector<Value> q;
  q.emplace_back(Value{INTEGER, num});

  //  std::cout << num << std::endl;
  *tuple = Tuple{q, plan_->output_schema_.get()};

  is_inserted_ = true;
  //  std::cout << num << std::endl;
  return true;

  //  }
}

}  // namespace bustub
