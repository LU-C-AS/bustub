//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // throw NotImplementedException("DeleteExecutor is not implemented"); }
  child_executor_->Init();
  auto cata = exec_ctx_->GetCatalog();
  auto table_info = cata->GetTable(plan_->table_oid_);
  tuples_.clear();

  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  while (true) {
    Tuple child_tuple{};
    RID child_rid;
    auto status = child_executor_->Next(&child_tuple, &child_rid);
    if (!status) {
      break;
    }
    //    auto txn = exec_ctx_->GetTransaction();
    //    auto txn_mgr = exec_ctx_->GetTransactionManager();
    auto t_pair = table_info->table_->GetTuple(child_rid);
    auto t_meta = t_pair.first;
    if (t_meta.ts_ > txn->GetReadTs() && t_meta.ts_ != txn->GetTransactionTempTs()) {
      //      MyAbort(txn, txn_mgr);
      txn->SetTainted();
      throw ExecutionException("del: w-w conflict\n");
    }

    auto wset = txn->GetWriteSets();
    auto it = wset[table_info->oid_].find(child_rid);
    if (it == wset[table_info->oid_].end()) {
      bool need_abort{false};
      //      auto r = 20;
      //      while (r--) {
      auto link_v = txn_mgr->GetVersionLink(child_rid);
      auto link = link_v.value().prev_;
      CheckVersion(txn_mgr, txn, child_rid, need_abort, link, table_info);
      if (need_abort) {
        //        MyAbort(txn, txn_mgr);
        txn->SetTainted();
        throw ExecutionException("update: w-w conflict\n");
      }
    }
    tuples_.emplace_back(std::pair<Tuple, RID>{t_pair.second, child_rid});
    txn->AppendWriteSet(table_info->oid_, child_rid);
  }
  iter_ = tuples_.cbegin();
}
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_deleted_) {
    return false;
  }
  auto cata = exec_ctx_->GetCatalog();
  auto table_info = cata->GetTable(plan_->table_oid_);
  int num = 0;
  while (iter_ != tuples_.cend()) {
    Tuple child_tuple = iter_->first;
    RID child_rid = iter_->second;
    //    auto status = child_executor_->Next(&child_tuple, &child_rid);
    //    if (!status) {
    //      break;
    //    }
    //    auto value = plan_->filter_predicate_->Evaluate(&tuple_pair.second, GetOutputSchema());
    //    table_info->table_->InsertTuple({0, false}, child_tuple);  // 先不要锁
    auto txn = exec_ctx_->GetTransaction();
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    bool abort{false};

    auto t_pair = table_info->table_->GetTuple(child_rid);
    child_tuple = t_pair.second;
    DelTxn(txn, txn_mgr, child_rid, child_tuple, child_executor_->GetOutputSchema(), table_info, abort);
    if (abort) {
      // 中止需要自己把link清除干净
      //      MyAbort(txn, txn_mgr);
      txn->SetTainted();
      throw ExecutionException("delete: w-w conflict\n");
    }
    iter_++;
    //    auto t_meta = table_info->table_->GetTuple(child_rid).first;
    //    if (t_meta.ts_ > txn->GetReadTs() && t_meta.ts_ != txn->GetTransactionTempTs()) {
    //      txn->SetTainted();
    //      throw ExecutionException("delete: w-w conflict\n");
    //    }
    //    UndoLink link;
    //    auto link_v = txn_mgr->GetVersionLink(child_rid);
    //    //  std::cout << "seq " << link_v.has_value() << std::endl;
    //
    //    auto link_o = link_v.value();
    //    if (link_o.in_progress_) {
    //      txn->SetTainted();
    //      throw ExecutionException("del inpro\n");
    //    }
    //    auto ver = VersionUndoLink{link_o.prev_, true};
    //    txn_mgr->UpdateVersionLink(child_rid, {ver}, nullptr);
    //
    //    link = link_o.prev_;
    //    //    CheckVersion(txn_mgr, txn, child_rid, need_abort, is_con, link);
    //
    //    //    if (is_con) {
    //    //      continue;
    //    //    }
    //    //    auto link_o = txn_mgr->GetUndoLink(child_rid);
    //    //    UndoLink link{};
    //    //    bool has_log{false};
    //    //    uint32_t log_id;
    //    //    if (link_o.has_value()) {
    //    //      link = link_o.value();
    //    //      //      auto  = link.prev_txn_;
    //    //      if (txn->GetTransactionId() == link.prev_txn_) {
    //    //        has_log = true;
    //    //        log_id = link.prev_log_idx_;
    //    //      }
    //    //    }
    //    // 要检查有没有自己的undolog
    //    if (t_meta.ts_ == txn->GetTransactionTempTs()) {
    //      //      auto w_guard = table_info->table_->AcquireTablePageWriteLock(child_rid);
    //      //      auto link_v = txn_mgr->GetVersionLink(child_rid);
    //      //      if(link_v.has_value()) {
    //      //        auto link_o = link_v.value();
    //      // 如果之前insert的，被自己刚刚update，那么直接设置就可以，因为update的log可以恢复原状
    //      if (link.IsValid()) {
    //        table_info->table_->UpdateTupleMeta({t_meta.ts_, true}, child_rid);
    //      } else {
    //        // 自己刚insert的，直接设置0
    //        table_info->table_->UpdateTupleMeta({0, true}, child_rid);
    //      }
    //      auto new_ver = VersionUndoLink{link, false};
    //      txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
    //      //      }
    //      //      w_guard.Drop();
    //    } else {
    //      std::vector<bool> mod_fields;
    //
    //      mod_fields.insert(mod_fields.end(), child_executor_->GetOutputSchema().GetColumnCount(), true);
    //
    //      //      UndoLink link_o;
    //      ForUpdateAndDel(txn, txn_mgr, child_rid, child_tuple, mod_fields, t_meta.ts_, link);
    //      //      w_guard.Drop();
    //      //      if (new_abort) {
    //      //        txn->SetTainted();
    //      //        throw ExecutionException("del inprogress\n");
    //      //      }
    //      table_info->table_->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, child_rid);
    //      txn->AppendWriteSet(table_info->oid_, child_rid);
    //      auto new_ver = VersionUndoLink{link, false};
    //      txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
    //
    //      //      auto new_ver = VersionUndoLink{link_o, false};
    //      //      txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
    //    }
    num++;
    //    w_guard.Drop();
    // 更新index
    //    auto inds = cata->GetTableIndexes(table_info->name_);
    //    auto table_schema = table_info->schema_;
    //    for (auto ind : inds) {
    //      auto key_schema = ind->key_schema_;
    //      auto col_id = table_schema.GetColIdx(key_schema.GetColumn(0).GetName());
    //
    //      ind->index_->DeleteEntry(child_tuple.KeyFromTuple(table_schema, key_schema, {col_id}), child_rid, nullptr);
    //    }
  }

  //  if (num == 0) {
  //    return false;
  //  } else {
  //  std::vector<Column> p;
  //  p.push_back({"delete num", INTEGER});
  //  auto schema = Schema(p);

  std::vector<Value> q;
  q.emplace_back(Value{INTEGER, num});
  *tuple = Tuple{q, plan_->output_schema_.get()};

  is_deleted_ = true;
  return true;
  //  }
}

}  // namespace bustub
