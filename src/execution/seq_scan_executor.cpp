//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  //  auto cata = exec_ctx_->GetCatalog();
  //  auto table_info = cata->GetTable(plan_->table_oid_);
  //  next_iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

void SeqScanExecutor::Init() {
  auto cata = exec_ctx_->GetCatalog();
  auto table_info = cata->GetTable(plan_->table_oid_);
  next_iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
  // throw NotImplementedException("SeqScanExecutor is not implemented");

  auto txn = exec_ctx_->GetTransaction();
  txn->AppendScanPredicate(table_info->oid_, plan_->filter_predicate_);
  //  auto preds = txn->GetScanPredicates();
  //  auto p_it = preds.find(table_info->oid_);
  //  if (p_it == preds.end()) {
  //    preds.insert({table_info->oid_, {plan_->filter_predicate_}});
  //  } else {
  //    preds[table_info->oid_].emplace_back(plan_->filter_predicate_);
  //  }
  std::cout << txn->GetTransactionIdHumanReadable() << " " << txn->GetScanPredicates().empty() << std::endl;
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  //  if (next_iter_->IsEnd()) {
  //    return false;
  //  }
  auto txn = exec_ctx_->GetTransaction();
  auto txn_ts = txn->GetReadTs();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  //  while (true) {
  //    txn_mgr->version_info_mutex_.lock_shared();
  //    //      auto lock = std::shared_lock<std::shared_mutex>(txn_mgr->version_info_mutex_);
  //
  //    auto link_v = txn_mgr->GetVersionLink(child_rid);
  //    auto link_o = link_v.value();
  //    if (!link_o.in_progress_) {
  //      // break的时候带有读锁
  //      break;
  //    }
  //    txn_mgr->version_info_mutex_.unlock_shared();
  //  }
  //  std::cout << exec_ctx_->GetTransaction()->GetTransactionId() << std::endl;
  while (!next_iter_->IsEnd()) {
    auto tuple_pair = next_iter_->GetTuple();
    //    std::cout << tuple_pair.second.ToString(&GetOutputSchema()) << " " << tuple_pair.first.ts_ << std::endl;
    //    if (!tuple_pair.first.is_deleted_) {
    std::cout << " seqscan\n";
    Tuple base_tuple = tuple_pair.second;
    auto cur_ts = tuple_pair.first.ts_;

    //    auto cata = exec_ctx_->GetCatalog();
    //    auto table_info = cata->GetTable(plan_->table_oid_);
    //    std::cout << "cts " << cur_ts << " txn " << txn_ts << std::endl;

    auto child_rid = next_iter_->GetRID();
    //    bool need_abort{false};
    //    bool is_con{false};
    UndoLink link;

    //    while (true) {
    //      txn_mgr->version_info_mutex_.lock_shared();
    //      //      auto lock = std::shared_lock<std::shared_mutex>(txn_mgr->version_info_mutex_);
    //
    //    while (true) {
    auto link_v = txn_mgr->GetVersionLink(child_rid);
    auto link_o = link_v.value();
    link = link_o.prev_;
    //      if (!link_o.in_progress_) {
    //        break;
    //      }
    //    }

    //      if (!link_o.in_progress_) {
    //        // break的时候带有读锁
    //        break;
    //      }
    //      txn_mgr->version_info_mutex_.unlock_shared();
    //    }
    //    CheckVersion(txn_mgr, txn, child_rid, need_abort, is_con, link);
    //    if (need_abort) {
    //      txn->SetTainted();
    //      throw ExecutionException("seq inpro\n");
    //    }
    //    if (is_con) {
    //      ++(*next_iter_);
    //      continue;
    //    }

    if (cur_ts > txn_ts && (cur_ts != txn->GetTransactionTempTs())) {
      std::vector<UndoLog> uls;
      //      std::cout << "\nrid " << next_iter_->GetRID().ToString() << std::endl;

      // read和write好像没区别
      auto flag{false};
      auto local_link = link;
      while (local_link.IsValid()) {
        auto cur_log = txn_mgr->GetUndoLog(local_link);
        //        std::cout << "seq log " << cur_log.ts_ << " ";
        //        uls.emplace_back(cur_log);
        //        if (cur_log.ts_ <= txn_ts) {
        //          //          std::cout << "seq logtrue " << cur_log.ts_ << std::endl;
        //          flag = true;
        //          break;
        //        }
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

      bool con{false};
      if (flag) {
        auto res = ReconstructTuple(&GetOutputSchema(), base_tuple, tuple_pair.first, uls);
        if (res.has_value()) {
          base_tuple = res.value();
        } else {
          con = true;
        }
        //        else {
        //          // 可能中间被其他txn删过
        //          auto new_ver = VersionUndoLink{link, false};
        //          txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
        //          ++(*next_iter_);
        //          continue;
        //        }
      } else {
        con = true;
        //        auto new_ver = VersionUndoLink{link, false};
        //        txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
        //        ++(*next_iter_);
        //        continue;
      }

      //      txn_mgr->version_info_mutex_.unlock_shared();
      //      auto new_ver = VersionUndoLink{link, false};
      //      txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
      if (con) {
        ++(*next_iter_);
        continue;
      }
    } else {
      if (tuple_pair.first.is_deleted_) {
        //        std::cout << "se del " << txn->GetTransactionIdHumanReadable() << std::endl;
        ++(*next_iter_);
        //        txn_mgr->version_info_mutex_.unlock_shared();
        //        auto new_ver = VersionUndoLink{link, false};
        //        txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
        continue;
      }

      //      txn_mgr->version_info_mutex_.unlock_shared();
      //      auto new_ver = VersionUndoLink{link, false};
      //      txn_mgr->UpdateVersionLink(child_rid, {new_ver}, nullptr);
      //      std::cout << "se else " << cur_ts << " " << txn_ts << std::endl;
    }

    std::cout << base_tuple.ToString(&GetOutputSchema()) << " " << cur_ts << std::endl;
    //    std::cout << "seq 2" << base_tuple.ToString(&GetOutputSchema()) << std::endl;
    if (plan_->filter_predicate_) {
      auto value = plan_->filter_predicate_->Evaluate(&base_tuple, GetOutputSchema());
      if (value.GetAs<bool>()) {
        *rid = child_rid;
        *tuple = base_tuple;
        ++(*next_iter_);
        return true;
      }
      //      }
    } else {
      *rid = child_rid;
      *tuple = base_tuple;
      ++(*next_iter_);
      return true;
    }
    //    }
    ++(*next_iter_);
  }
  return false;
  //  auto cata = exec_ctx_->GetCatalog();
  //  auto table_info = cata->GetTable(plan_->table_oid_);
  //  auto iter = table_info->table_->MakeIterator();
  //  for (; !iter.IsEnd(); ++iter) {
  //    *rid = iter.GetRID();
  //    auto tuple_pair = iter.GetTuple();
  //    if (tuple_pair.first.is_deleted_ != true) {
  //      *tuple = tuple_pair.second;
  //    }
  //    return true;
  //  }
  //
  //  return false;
}

}  // namespace bustub
