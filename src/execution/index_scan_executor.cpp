//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  //  auto cata = exec_ctx_->GetCatalog();
  //  auto table_info = cata->GetTable(plan_->table_oid_);
  //  next_iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

void IndexScanExecutor::Init() { /*throw NotImplementedException("IndexScanExecutor is not implemented");*/
  is_scaned_ = false;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto txn = exec_ctx_->GetTransaction();

  if (plan_->filter_predicate_) {
    txn->AppendScanPredicate(table_info->oid_, plan_->filter_predicate_);
  }
  //  txn->AppendScanPredicate(table_info->oid_, std::shared_ptr<AbstractExpression>(plan_->pred_key_));
  //  auto preds = txn->GetScanPredicates();
  //  auto p_it = preds.find(table_info->oid_);
  //  if (p_it == preds.end()) {
  //    preds.insert({table_info->oid_, {plan_->filter_predicate_}});
  //  } else {
  //    preds[table_info->oid_].emplace_back(plan_->filter_predicate_);
  //  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_scaned_) {
    return false;
  }
  //  std::cout << "index scan\n";
  auto cata = exec_ctx_->GetCatalog();
  auto table_info = cata->GetTable(plan_->table_oid_);
  auto index_info = cata->GetIndex(plan_->index_oid_);

  auto htable = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());

  auto key_schema = index_info->key_schema_;
  Tuple temp;
  auto key_value = plan_->pred_key_->Evaluate(&temp, GetOutputSchema());
  //  std::cout << tuple->ToString(&GetOutputSchema()) << std::endl;
  //  std::cout << key_value.GetAs<int>() << std::endl;
  //  auto c = Column("key", INTEGER);
  //  auto s = Schema(std::vector<Column>{c});
  //  std::cout << "key\n";
  auto key = Tuple(std::vector<Value>{key_value}, &key_schema);
  //  std::cout << key.GetLength()
  //  std::cout << key.GetData() << std::endl;
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  std::vector<RID> rids;
  htable->ScanKey(key, &rids, txn);

  //  std::cout << "after scan " << rids[0] << std::endl;
  //  BUSTUB_ASSERT(rids.size() == 1, "rids size");

  is_scaned_ = true;
  if (rids.empty()) {
    //    std::cout << "empty\n";
    return false;
  }
  auto child_rid = rids[0];
  //  std::cout << child_rid.ToString() << std::endl;
  //  bool need_abort{false};
  //  bool is_con{false};
  //  UndoLink link;
  //  auto link_v = txn_mgr->GetVersionLink(child_rid);
  //  auto link_o = link_v.value();
  //  link = link_o.prev_;
  auto tuple_pair = table_info->table_->GetTuple(child_rid);
  auto base_tuple = tuple_pair.second;
  UndoLink link;
  auto link_v = txn_mgr->GetVersionLink(child_rid);
  auto link_o = link_v.value();
  link = link_o.prev_;
  //  std::cout << "ind " << tuple_pair.second.ToString(&GetOutputSchema()) << " " << tuple_pair.first.ts_ << " "
  //            << txn->GetTransactionIdHumanReadable() << std::endl;
  //  r_guard.Drop();

  auto cur_ts = tuple_pair.first.ts_;
  if (cur_ts > txn->GetReadTs() && (cur_ts != txn->GetTransactionTempTs())) {
    std::vector<UndoLog> uls;
    //      std::cout << "\nrid " << next_iter_->GetRID().ToString() << std::endl;

    auto txn_ts = txn->GetReadTs();
    // read和write好像没区别
    auto flag{false};
    auto local_link = link;
    while (local_link.IsValid()) {
      auto cur_log = txn_mgr->GetUndoLog(local_link);
      //      std::cout << "ind log " << cur_log.ts_ << std::endl;
      if (cur_log.ts_ < cur_ts) {
        uls.emplace_back(cur_log);
      } else {
        local_link = cur_log.prev_version_;
        continue;
      }
      local_link = cur_log.prev_version_;
      if (local_link.IsValid() && cur_log.ts_ > txn_mgr->GetWatermark()) {
        auto next_log = txn_mgr->GetUndoLog(local_link);
        if (cur_log.ts_ <= txn_ts) {
          if (next_log.ts_ < txn_ts) {
            flag = true;
            break;
          }
        }
      } else {
        if (cur_log.ts_ > txn_ts) {
          break;
        }
        flag = true;
        break;
      }
    }

    //      auto new_ver = VersionUndoLink{link_o.prev_, false};
    //      txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);

    //    std::cout << "flag " << flag << " " << txn->GetTransactionIdHumanReadable() << std::endl;
    if (flag) {
      auto res = ReconstructTuple(&GetOutputSchema(), base_tuple, tuple_pair.first, uls);
      if (res.has_value()) {
        base_tuple = res.value();
        //        std::cout << base_tuple.ToString(&GetOutputSchema()) << " " << txn->GetTransactionIdHumanReadable()
        //                  << std::endl;
      } else {
        return false;
      }

    } else {
      return false;
    }

  } else {
    if (tuple_pair.first.is_deleted_) {
      return false;
    }
  }
  if (!plan_->filter_predicate_) {
    *rid = rids[0];
    *tuple = base_tuple;
    //    link_v.in_progress_ = false;
    return true;
  }
  auto value = plan_->filter_predicate_->Evaluate(&base_tuple, GetOutputSchema());
  if (value.GetAs<bool>()) {
    *rid = rids[0];
    *tuple = base_tuple;
    //    link_v.in_progress_ = false;
    return true;
  }
  return false;
}

}  // namespace bustub
