//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  // throw NotImplementedException("LimitExecutor is not implemented"); }
  child_executor_->Init();
  num_ = 0;
  //  is_limit_ = false;
}
auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (num_ == plan_->limit_) {
    return false;
  }
  auto s = child_executor_->Next(tuple, rid);
  if (!s) {
    return false;
  }
  num_++;
  return true;
}

}  // namespace bustub
