//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  auto cata = exec_ctx_->GetCatalog();
  auto table_info = cata->GetTable(plan_->table_oid_);
  tuples_.clear();

  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  //  std::cout << "update init " << txn->GetTransactionIdHumanReadable() << std::endl;
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
    //    std::cout << "up " << t_pair.second.ToString(&child_executor_->GetOutputSchema()) << " " << t_meta.ts_ <<
    //    std::endl;
    if (t_meta.ts_ > txn->GetReadTs() && t_meta.ts_ != txn->GetTransactionTempTs()) {
      //      MyAbort(txn, txn_mgr);
      txn->SetTainted();
      throw ExecutionException("update: w-w conflict\n");
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
    //    }
    tuples_.emplace_back(std::pair<Tuple, RID>{t_pair.second, child_rid});
    txn->AppendWriteSet(table_info->oid_, child_rid);
  }

  // 测试不严谨的地方。如果是空集，一种可能是和其他事务有冲突，update失败，此时应该abort，
  // 另一种可能是没有事务冲突，单纯是没有满足条件的记录，此时return false即可。但两种都不会对表更新。
  //   if (tuples_.empty()) {
  //     txn->SetTainted();
  //     throw ExecutionException("update: w-w conflict\n");
  //   }
  iter_ = tuples_.cbegin();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuples_.cend()) {
    return false;
  }
  auto cata = exec_ctx_->GetCatalog();
  auto table_info = cata->GetTable(plan_->table_oid_);

  auto inds = cata->GetTableIndexes(table_info->name_);

  bool is_pm_update{false};
  IndexInfo *pm_ind = nullptr;
  for (auto &ind : inds) {
    if (ind->is_primary_key_) {
      pm_ind = ind;
    }
  }

  if (pm_ind != nullptr) {
    for (uint32_t j = 0; j < plan_->target_expressions_.size(); j++) {
      if (j == pm_ind->index_->GetMetadata()->GetKeyAttrs()[0]) {
        auto col_e = dynamic_cast<ColumnValueExpression *>(plan_->target_expressions_[j].get());
        if (col_e == nullptr) {
          is_pm_update = true;
          break;
        }
      }
    }
  }
  //  std::cout << is_pm_update << " pm\n";
  int num = 0;
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  if (is_pm_update) {
    while (iter_ != tuples_.cend()) {
      bool need_abort{false};

      auto t_pair = *iter_;
      auto up_tuple = t_pair.first;
      auto up_rid = t_pair.second;
      auto ori_pair = table_info->table_->GetTuple(up_rid);
      //      auto t_meta = ori_pair.first;
      //      BUSTUB_ASSERT(
      //          ori_pair.second.GetValue(&child_executor_->GetOutputSchema(), 1)
      //                  .CompareEquals(up_tuple.GetValue(&child_executor_->GetOutputSchema(), 1)) == CmpBool::CmpTrue,
      //          "update notemain\n");
      //      auto del_tuple = ori_pair.second;
      DelTxn(txn, txn_mgr, up_rid, up_tuple, table_info->schema_, table_info, need_abort, true);
      if (need_abort) {
        //        MyAbort(txn, txn_mgr);
        txn->SetTainted();
        throw ExecutionException("update inpro del\n");
      }
      iter_++;
    }
    iter_ = tuples_.cbegin();
    while (iter_ != tuples_.cend()) {
      auto t_pair = *iter_;
      auto up_tuple = t_pair.first;
      std::vector<Value> values{};
      //    values.reserve(GetOutputSchema().GetColumnCount());
      auto c_schema = child_executor_->GetOutputSchema();
      values.reserve(c_schema.GetColumnCount());
      for (auto &e : plan_->target_expressions_) {
        auto val = e->Evaluate(&up_tuple, c_schema);
        values.emplace_back(val);
      }

      auto new_tuple = Tuple{values, &c_schema};

      bool need_abort{false};
      //      auto inds = cata->GetTableIndexes(table_info->name_);
      InsertTxn(txn, txn_mgr, inds, new_tuple, table_info->schema_, table_info, need_abort, true);
      if (need_abort) {
        //        MyAbort(txn, txn_mgr);
        txn->SetTainted();
        throw ExecutionException("update inpro insert\n");
      }

      iter_++;
    }
    num = tuples_.size();
  } else {
    while (iter_ != tuples_.cend()) {
      auto t_pair = *iter_;
      auto up_tuple = t_pair.first;
      auto up_rid = t_pair.second;
      //    std::cout << "update " << up_rid.ToString();
      // update
      auto ori_pair = table_info->table_->GetTuple(up_rid);
      auto t_meta = ori_pair.first;
      BUSTUB_ASSERT(
          ori_pair.second.GetValue(&child_executor_->GetOutputSchema(), 1)
                  .CompareEquals(up_tuple.GetValue(&child_executor_->GetOutputSchema(), 1)) == CmpBool::CmpTrue,
          "update noteq\n");
      if (t_meta.ts_ > txn->GetReadTs() && t_meta.ts_ != txn->GetTransactionTempTs()) {
        //        MyAbort(txn, txn_mgr);
        txn->SetTainted();
        throw ExecutionException("update: w-w conflict\n");
      }
      //      bool need_abort{false};
      //      bool is_con{false};
      auto link_v = txn_mgr->GetVersionLink(up_rid);
      UndoLink link = link_v->prev_;
      if (t_meta.ts_ == txn->GetTransactionTempTs()) {
        //      std::cout << "multi\n";
        // 多重update要更新log，因为log只记载了更新的col

        bool has_log{false};
        uint32_t log_id;

        if (txn->GetTransactionId() == link.prev_txn_ && !txn->GetUndoLog(link.prev_log_idx_).is_deleted_) {
          has_log = true;
          log_id = link.prev_log_idx_;
        }
        if (has_log) {
          std::vector<Value> mod_values{};
          std::vector<Value> values{};
          std::vector<bool> mod;
          std::vector<Column> cols;
          //    values.reserve(GetOutputSchema().GetColumnCount());
          auto c_schema = child_executor_->GetOutputSchema();
          values.reserve(c_schema.GetColumnCount());
          mod.reserve(c_schema.GetColumnCount());

          auto ori_log = txn_mgr->GetUndoLog(link);
          auto ori_mod = ori_log.modified_fields_;

          auto ori_tuple = ReconstructTuple(&c_schema, up_tuple, t_meta, {ori_log});
          if (!ori_tuple.has_value()) {
            //            auto lock = std::unique_lock<std::shared_mutex>(txn_mgr->version_info_mutex_);
            std::cout << "update log_del\n";

            ++iter_;
            continue;
          }
          auto o_tuple = ori_tuple.value();
          //        std::cout << "multilog " << ori_tuple->ToString(&c_schema);

          //        uint32_t j = 0;
          for (uint32_t i = 0; i < plan_->target_expressions_.size(); i++) {
            //      std::cout << e->ToString() << std::endl;
            auto val = plan_->target_expressions_[i]->Evaluate(&up_tuple, c_schema);
            values.emplace_back(val);
            if (val.CompareEquals(o_tuple.GetValue(&c_schema, i)) == CmpBool::CmpFalse) {
              mod.emplace_back(true);
              mod_values.emplace_back(o_tuple.GetValue(&c_schema, i));
              cols.emplace_back(c_schema.GetColumn(i));
            } else {
              if (ori_mod[i]) {
                mod.emplace_back(true);
                mod_values.emplace_back(o_tuple.GetValue(&c_schema, i));
                cols.emplace_back(c_schema.GetColumn(i));
              } else {
                mod.emplace_back(false);
              }
            }
          }
          auto log_sch = Schema{cols};
          auto log_tuple = Tuple{mod_values, &log_sch};
          //        std::cout << "new log " << log_tuple.ToString(&log_sch) << std::endl;

          auto undolog = UndoLog{false, mod, log_tuple, ori_log.ts_, ori_log.prev_version_};
          txn->ModifyUndoLog(log_id, undolog);

          auto new_tuple = Tuple{values, &c_schema};
          //          if (t_meta.ts_ > txn->GetReadTs() && t_meta.ts_ != txn->GetTransactionTempTs()) {
          //                  MyAbort(txn, txn_mgr, table_info);
          //            txn->SetTainted();
          //            throw ExecutionException("update: w-w conflict\n");
          //          }
          table_info->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, new_tuple, up_rid, nullptr);

          // modify log不需要更新prev
        } else {
          // 自己刚insert进来的，不需要log
          std::vector<Value> values{};
          //    values.reserve(GetOutputSchema().GetColumnCount());
          auto c_schema = child_executor_->GetOutputSchema();
          values.reserve(c_schema.GetColumnCount());
          //        for (uint32_t i = 0; i < plan_->target_expressions_.size(); i++) {
          for (auto &e : plan_->target_expressions_) {
            //      std::cout << e->ToString() << std::endl;

            auto val = e->Evaluate(&up_tuple, c_schema);
            values.emplace_back(val);
          }

          auto new_tuple = Tuple{values, &c_schema};

          table_info->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, new_tuple, up_rid, nullptr);
        }

      } else {
        //        std::cout << "norm ";
        std::vector<Value> mod_values{};
        std::vector<Value> values{};
        std::vector<bool> mod;
        std::vector<Column> cols;
        //    values.reserve(GetOutputSchema().GetColumnCount());
        auto c_schema = child_executor_->GetOutputSchema();
        values.reserve(c_schema.GetColumnCount());
        mod.reserve(c_schema.GetColumnCount());
        for (uint32_t i = 0; i < plan_->target_expressions_.size(); i++) {
          //      std::cout << e->ToString() << std::endl;
          auto val = plan_->target_expressions_[i]->Evaluate(&up_tuple, c_schema);
          values.emplace_back(val);
          if (val.CompareEquals(up_tuple.GetValue(&c_schema, i)) == CmpBool::CmpFalse) {
            mod.emplace_back(true);
            mod_values.emplace_back(up_tuple.GetValue(&c_schema, i));
            cols.emplace_back(c_schema.GetColumn(i));
          } else {
            mod.emplace_back(false);
          }
        }
        // 多重update应该取原tuple的值
        auto log_t_sch = Schema(cols);
        auto log_tuple = Tuple{mod_values, &log_t_sch};
        //      std::cout << "1upda" << log_tuple.ToString(&log_t_sch);

        ForUpdateAndDel(txn, log_tuple, mod, t_meta.ts_, &link);

        auto new_tuple = Tuple{values, &c_schema};

        auto new_ver = VersionUndoLink{link, true};
        txn_mgr->UpdateVersionLink(up_rid, {new_ver}, nullptr);
        table_info->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, new_tuple, up_rid, nullptr);
        txn->AppendWriteSet(table_info->oid_, up_rid);

        std::cout << "norm ";
        num++;
      }

      auto new_t = table_info->table_->GetTuple(up_rid);
      //      std::cout << "\t update" << txn->GetTransactionIdHumanReadable() << " "
      //                << new_t.second.ToString(&table_info->schema_) << " "
      //                << iter_->first.ToString(&child_executor_->GetOutputSchema()) << std::endl;
      iter_++;
      // 更新index
      //    auto inds = cata->GetTableIndexes(table_info->name_);
      //    auto table_schema = table_info->schema_;
      //    for (auto ind : inds) {
      //      auto key_schema = ind->key_schema_;
      //      auto col_id = table_schema.GetColIdx(key_schema.GetColumn(0).GetName());
      //
      //      ind->index_->DeleteEntry(up_tuple.KeyFromTuple(table_schema, key_schema, {col_id}), child_rid, nullptr);
      //      ind->index_->InsertEntry(new_tuple.KeyFromTuple(table_schema, key_schema, {col_id}), new_rid, nullptr);
      //    }
    }
  }

  //
  //  if (is_updated_) {
  //    return false;
  //  }
  //  //  auto cata = exec_ctx_->GetCatalog();
  //  //  auto table_info = cata->GetTable(plan_->table_oid_);
  //  int num = 0;
  //  while (true) {
  //    Tuple child_tuple{};
  //    RID child_rid;
  //    auto status = child_executor_->Next(&child_tuple, &child_rid);
  //    if (!status) {
  //      break;
  //    }
  //
  //    //    std::cout << child_tuple.ToString(&GetOutputSchema());
  //    //    auto txn = exec_ctx_->GetTransaction();
  //    //    auto txn_mgr = exec_ctx_->GetTransactionManager();
  //    //    auto link_o = txn_mgr->GetUndoLink(child_rid);
  //    table_info->table_->UpdateTupleMeta({0, true}, child_rid);
  //
  //    std::vector<Value> values{};
  //    values.reserve(GetOutputSchema().GetColumnCount());
  //    for (auto &e : plan_->target_expressions_) {
  //      //      std::cout << e->ToString() << std::endl;
  //      values.push_back(e->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
  //    }
  //    auto new_tuple = Tuple(values, &child_executor_->GetOutputSchema());
  //
  //    auto opt_rid = table_info->table_->InsertTuple({0, false}, new_tuple);  // 先不要锁
  //    auto new_rid = opt_rid.value();
  //    num++;
  //    // 更新index
  //    auto inds = cata->GetTableIndexes(table_info->name_);
  //    auto table_schema = table_info->schema_;
  //    for (auto ind : inds) {
  //      auto key_schema = ind->key_schema_;
  //      auto col_id = table_schema.GetColIdx(key_schema.GetColumn(0).GetName());
  //
  //      ind->index_->DeleteEntry(child_tuple.KeyFromTuple(table_schema, key_schema, {col_id}), child_rid, nullptr);
  //      ind->index_->InsertEntry(new_tuple.KeyFromTuple(table_schema, key_schema, {col_id}), new_rid, nullptr);
  //    }
  //  }

  std::vector<Value> q;
  q.emplace_back(Value{INTEGER, num});
  *tuple = Tuple{q, plan_->output_schema_.get()};

  is_updated_ = true;
  return true;
  //  }
}
}  // namespace bustub
