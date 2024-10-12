#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

void RewriteExpressionForHashJoin(std::vector<AbstractExpressionRef> &l_key_expr,
                                  std::vector<AbstractExpressionRef> &r_key_expr, const AbstractExpressionRef &expr,
                                  bool &is_correct) {
  //  std::vector<AbstractExpressionRef> key_expressions_;
  //  std::vector<AbstractExpressionRef> right_key_expressions_;
  for (auto &e : expr->GetChildren()) {
    //    auto col_e = e
    RewriteExpressionForHashJoin(l_key_expr, r_key_expr, e, is_correct);
  }
  //  std::cout << expr->GetChildren().size() << std::endl;
  if (expr->GetChildren().empty()) {
    if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
        column_value_expr != nullptr) {
      if (column_value_expr->GetTupleIdx() == 0) {
        l_key_expr.emplace_back(expr);
      } else if (column_value_expr->GetTupleIdx() == 1) {
        r_key_expr.emplace_back(expr);
      }
    } else {
      //      std::cout << expr->GetReturnType();
      is_correct = false;
    }
  }
  //  if (const auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
  //      column_value_expr != nullptr) {
  //    if (column_value_expr->GetTupleIdx() == is_left) {
  //      return std::make_shared<ColumnValueExpression>(is_left, column_value_expr->GetColIdx(),
  //                                                     column_value_expr->GetReturnType());
  //    } else {
  //      return nullptr;
  //    }
  //  } else {
  //    is_correct = false;
  //  }
  //
  //  return expr->CloneWithChildren(key_expressions_);
}
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...

  //  std::cout << "hash join\n";
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));  // recursive
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    //    std::cout << "nest\n";
    auto nlj_plan = dynamic_cast<NestedLoopJoinPlanNode *>(optimized_plan.get());
    //    auto exprr = nlj_plan->Predicate();

    bool is_correct{true};
    std::vector<AbstractExpressionRef> left_key;
    std::vector<AbstractExpressionRef> right_key;
    RewriteExpressionForHashJoin(left_key, right_key, nlj_plan->Predicate(), is_correct);
    if (!is_correct) {
      return optimized_plan;
    }
    //    auto right_expr = RewriteExpressionForHashJoin(nlj_plan->Predicate(), 0, is_correct);
    //    if (!is_correct) {
    //      return optimized_plan;
    //    }
    return std::make_shared<HashJoinPlanNode>(nlj_plan->output_schema_, nlj_plan->GetLeftPlan(),
                                              nlj_plan->GetRightPlan(), left_key, right_key, nlj_plan->GetJoinType());
  }

  //  std::cout << "hash join\n";
  return optimized_plan;
  //  auto nlj_plan = plan->CloneWithChildren();
  //      dynamic_cast<NestedLoopJoinPlanNode *>(plan.get());
  //  auto left_expr = RewriteExpressionForHashJoin(plan.) std::vector<AbstractExpressionRef> left_key_expressions_;
  //  std::vector<AbstractExpressionRef> right_key_expressions_;
  //  for (auto &c : plan) return plan;
}

}  // namespace bustub
