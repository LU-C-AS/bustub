#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

// auto SortLimitAsTopN(const AbstractPlanNodeRef &plan, bool suc) -> AbstractPlanNodeRef {
//
// }
//}
auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    auto c_p = OptimizeSortLimitAsTopN(child);
    //    if (c_p->GetType() == PlanType::TopN) {
    //      auto cc = c_p->GetChildren()[0];

    children.emplace_back(c_p);
    //    }
  }
  //  std::cout << "topn\n";
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    auto limit_plan = dynamic_cast<LimitPlanNode *>(optimized_plan.get());
    auto limit = limit_plan->limit_;

    auto c_plan = limit_plan->GetChildPlan();
    auto sort_plan = dynamic_cast<const SortPlanNode *>(c_plan.get());
    if (sort_plan == nullptr) {
      return optimized_plan;
    }
    auto orders = sort_plan->order_bys_;

    //    auto orders = sort_plan->order_bys_;
    //    auto c_plan = sort_plan->GetChildPlan();
    //    std::cout << (c_plan->GetType() == PlanType::Limit) << std::endl;
    //    auto limit_plan = dynamic_cast<LimitPlanNode *>(optimized_plan.get());
    //    if (limit_plan == nullptr) {
    //      return optimized_plan;
    //    }
    //
    //    auto limit = limit_plan->limit_;
    return std::make_shared<TopNPlanNode>(limit_plan->output_schema_, sort_plan->GetChildPlan(), orders, limit);
  }

  //  if (optimized_plan->GetType() == PlanType::Sort) {
  //    return nullptr;
  //  }
  //  std::cout << (optimized_plan->ToString()) << std::endl;
  //  std::cout << "topn\n";
  return optimized_plan;
}

}  // namespace bustub
