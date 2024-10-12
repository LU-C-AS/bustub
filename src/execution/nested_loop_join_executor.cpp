//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

#include "type/value_factory.h"
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // throw NotImplementedException("NestedLoopJoinExecutor is not implemented"); }
  left_executor_->Init();
  right_executor_->Init();
  RID r;
  left_executor_->Next(&next_l_, &r);
  is_joined_ = false;
  row_joined_ = false;
  //  auto s2 = right_executor_->Next(&next_r_, &r);
  //  if (!(s1 && s2)) {
  //    is_joined_ = true;
  //  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_joined_) {
    //    std::cout << " isjoin false\n";
    return false;
  }
  auto schema = NestedLoopJoinPlanNode::InferJoinSchema(*plan_->GetLeftPlan(), *plan_->GetRightPlan());
  auto l_schema = plan_->GetLeftPlan()->output_schema_;
  auto r_schema = plan_->GetRightPlan()->output_schema_;
  //  auto left = next_l_;
  //  auto right = next_r_;

  auto filter = plan_->Predicate();

  while (true) {
    bool flag{false};
    Value value;
    //    if(plan_->GetJoinType() == JoinType::INNER) {
    //    value = filter->EvaluateJoin(&left, *l_schema, &right, *r_schema);
    //    std::cout << value.GetAs<bool>();

    //    auto left = next_l_;
    //    auto right = next_r_;
    auto s1 = right_executor_->Next(&next_r_, rid);
    if (!s1) {
      if (!row_joined_ && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> val;
        val.reserve(schema.GetColumnCount());
        for (uint32_t i = 0; i < l_schema->GetColumnCount(); i++) {
          val.emplace_back(next_l_.GetValue(l_schema.get(), i));
        }
        for (uint32_t i = 0; i < r_schema->GetColumnCount(); i++) {
          auto v = ValueFactory::GetNullValueByType(r_schema->GetColumn(i).GetType());
          val.emplace_back(v);
        }
        *tuple = Tuple(val, &schema);
        flag = true;
        //        std::cout << tuple->ToString(&schema) << " l " << schema.GetColumnCount() << std::endl;
        //        std::cout << " left\n";
      }
      auto s2 = left_executor_->Next(&next_l_, rid);
      if (!s2) {
        is_joined_ = true;
        //        std::cout << "false\n";
        //        std::cout << flag << "\n";
        return flag;
        //        is_joined_ = true;
      }
      //      else {
      //        r_end_ = true;
      //        row_joined = false;
      right_executor_->Init();
      row_joined_ = false;
      if (flag) {
        return true;
      }
      right_executor_->Next(&next_r_, rid);

      //      }
    }
    value = filter->EvaluateJoin(&next_l_, *l_schema, &next_r_, *r_schema);
    if (!value.IsNull() && value.GetAs<bool>()) {
      std::vector<Value> val;
      val.reserve(schema.GetColumnCount());
      for (uint32_t i = 0; i < l_schema->GetColumnCount(); i++) {
        val.emplace_back(next_l_.GetValue(l_schema.get(), i));
      }
      for (uint32_t i = 0; i < r_schema->GetColumnCount(); i++) {
        val.emplace_back(next_r_.GetValue(r_schema.get(), i));
      }
      *tuple = Tuple(val, &schema);
      //      flag = true;
      row_joined_ = true;
      //      std::cout << tuple->ToString(&schema) << " " << schema.GetColumnCount() << std::endl;
      return true;
    }
  }
}

}  // namespace bustub