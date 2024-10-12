#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  // throw NotImplementedException("TopNExecutor is not implemented"); }
  child_executor_->Init();
  heap_.clear();
  auto orders = plan_->order_bys_;
  auto schema = plan_->output_schema_;
  auto f = [&orders, &schema](const Tuple &A, const Tuple &B) {
    for (auto &p : orders) {
      auto a = p.second->Evaluate(&A, *schema);
      auto b = p.second->Evaluate(&B, *schema);
      if (p.first == OrderByType::DEFAULT || p.first == OrderByType::ASC) {
        if (a.CompareLessThan(b) == CmpBool::CmpTrue) {
          return true;
        }
        if (a.CompareGreaterThan(b) == CmpBool::CmpTrue) {
          return false;
        }

        continue;
      }
      if (p.first == OrderByType::DESC) {
        if (a.CompareGreaterThan(b) == CmpBool::CmpTrue) {
          return true;
        }
        if (a.CompareLessThan(b) == CmpBool::CmpTrue) {
          return false;
        }

        continue;
      }
    }
    return true;
  };

  heap_ = std::set<Tuple, std::function<bool(const bustub::Tuple &, const bustub::Tuple &)>>(f);
  while (true) {
    Tuple t{};
    RID r{};
    auto s = child_executor_->Next(&t, &r);
    if (!s) {
      break;
    }
    heap_.insert(t);
    if (heap_.size() > plan_->n_) {
      heap_.erase(--heap_.end());
    }
  }
  iter_ = heap_.cbegin();
  //  heap_ = q;
  //  heap_ = h;
  //      auto col = dynamic_cast<ColumnValueExpression *>(p.second.get());
  //      if (col == nullptr) {
  //        throw Exception("cmp: cannot transfer to col\n");
  //      }
  //  const size_t n = plan_->n_;
  //  std::array<Tuple
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == heap_.cend()) {
    return false;
  }
  *tuple = *iter_;
  std::cout << tuple->ToString(&GetOutputSchema()) << std::endl;
  *rid = tuple->GetRid();
  ++iter_;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
  // throw NotImplementedException("TopNExecutor is not implemented"); };
  return plan_->n_;
}
}  // namespace bustub
