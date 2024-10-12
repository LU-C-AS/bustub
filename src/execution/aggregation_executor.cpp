//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->aggregates_, plan->agg_types_),
      aht_iterator_(aht_.Begin()),
      out_schema_(AggregationPlanNode::InferAggSchema(plan->group_bys_, plan->aggregates_, plan->agg_types_)) {
  //  std::cout << plan->group_bys_.size() << " " << plan->aggregates_.size() << std::endl;
}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();

  while (true) {
    auto cols = plan_->group_bys_;
    Tuple child_tuple{};
    RID child_rid{};
    auto status = child_executor_->Next(&child_tuple, &child_rid);
    if (!status) {
      break;
    }
    auto key = MakeAggregateKey(&child_tuple);
    auto val = MakeAggregateValue(&child_tuple);
    aht_.InsertCombine(key, val);
    //    std::cout << "num " << ++num << std::endl;
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ != aht_.End()) {
    std::vector<Value> vals;
    vals.reserve(aht_iterator_.Key().group_bys_.size() + aht_iterator_.Val().aggregates_.size());
    vals.insert(vals.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
    vals.insert(vals.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
    //    if (vals.empty()) {
    //      vals = aht_.GenerateInitialAggregateValue().aggregates_;
    //    }
    *tuple = Tuple(vals, &out_schema_);
    ++aht_iterator_;
    //    std::cout << tuple->ToString(&out_schema_) << std::endl;
    return true;
  }

  if (!is_aggr_ && aht_.Begin() == aht_.End() && plan_->group_bys_.empty()) {
    auto init_val = aht_.GenerateInitialAggregateValue().aggregates_;
    std::vector<Value> vals;
    vals.reserve(init_val.size());
    //      vals.insert(vals.end(), plan_->group_bys_.begin(), plan_->group_bys_.end());
    vals.insert(vals.end(), init_val.begin(), init_val.end());
    *tuple = Tuple(vals, &out_schema_);
    //      std::cout << tuple->ToString(&out_schema_) << "empty" << std::endl;
    is_aggr_ = true;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
