#include "execution/executors/sort_executor.h"
#include "exception"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  // throw NotImplementedException("SortExecutor is not implemented"); }
  child_executor_->Init();
  sorted_l_.clear();
  while (true) {
    Tuple t{};
    RID r{};
    auto s = child_executor_->Next(&t, &r);
    if (!s) {
      break;
    }
    sorted_l_.emplace_back(t);
  }
  auto orders = plan_->order_bys_;
  auto schema = plan_->OutputSchema();
  auto f = [&orders, &schema](Tuple &A, Tuple &B) {
    for (auto &p : orders) {
      //      auto col = dynamic_cast<ColumnValueExpression *>(p.second.get());
      //      if (col == nullptr) {
      //        throw Exception("cmp: cannot transfer to col\n");
      //      }
      auto a = p.second->Evaluate(&A, schema);
      auto b = p.second->Evaluate(&B, schema);

      //      auto a = A.GetValue(&schema, col->GetColIdx());
      //      auto b = B.GetValue(&schema, col->GetColIdx());
      //      std::cout << col->GetReturnType() << std::endl;

      //      auto val_a = a.GetAs<col->GetReturnType()>()
      if (p.first == OrderByType::DEFAULT || p.first == OrderByType::ASC) {
        if (a.CompareLessThan(b) == CmpBool::CmpTrue) {
          return true;
        }
        if (a.CompareGreaterThan(b) == CmpBool::CmpTrue) {
          return false;
        }
        { continue; }
      } else if (p.first == OrderByType::DESC) {
        if (a.CompareGreaterThan(b) == CmpBool::CmpTrue) {
          return true;
        }
        if (a.CompareLessThan(b) == CmpBool::CmpTrue) {
          return false;
        }
        { continue; }
      }
    }
    return true;
  };
  std::sort(sorted_l_.begin(), sorted_l_.end(), f);
  //  std::cout << sorted_l_[0].ToString(&plan_->OutputSchema()) << " " << sorted_l_[1].ToString(&plan_->OutputSchema())
  //            << std::endl;
  iter_ = sorted_l_.cbegin();
}
auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == sorted_l_.cend()) {
    return false;
  }
  *tuple = *iter_;
  *rid = tuple->GetRid();
  ++iter_;
  return true;
}

}  // namespace bustub
