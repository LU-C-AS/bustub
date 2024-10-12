#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

void ForUpdateAndDel(Transaction *txn, Tuple &log_tuple, std::vector<bool> &mod, timestamp_t log_ts, UndoLink *link);

void CheckVersion(TransactionManager *txn_mgr, Transaction *txn, const RID &rid, bool &need_abort, UndoLink &new_link,
                  TableInfo *table_info);

void GetExpr(std::vector<AbstractExpressionRef> &exprs, const AbstractExpressionRef &cur_e);

void DelTxn(Transaction *txn, TransactionManager *txn_mgr, const RID &child_rid, const Tuple &child_tuple,
            const Schema &schema, TableInfo *table_info, bool &need_abort, bool is_up = false);

void InsertTxn(Transaction *txn, TransactionManager *txn_mgr, std::vector<IndexInfo *> &inds, Tuple &child_tuple,
               const Schema &schema, TableInfo *table_info, bool &need_abort, bool is_up = false);

void MyAbort(Transaction *txn, TransactionManager *txn_mgr);
// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

}  // namespace bustub
