#include "execution/executors/window_function_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      wht_(&plan->window_functions_) {}

void WindowFunctionExecutor::Init() {  // throw NotImplementedException("WindowFunctionExecutor is not implemented"); }
  child_executor_->Init();
  sorted_l_.clear();
  last_.clear();
  for (uint32_t i = 0; i < plan_->window_functions_.size(); i++) {
    last_.push_back(AggVal{ValueFactory::GetNullValueByType(TypeId::INTEGER)});
  }
  while (true) {
    Tuple t{};
    RID r{};
    auto s = child_executor_->Next(&t, &r);
    if (!s) {
      break;
    }
    sorted_l_.emplace_back(t);
  }
  // 只看第一个function有没有order_by_?如果有则所有都有
  auto it = plan_->window_functions_.begin();
  auto orders = it->second.order_by_;
  //  std::cout << "ls " << sorted_l_.size();

  bool is_order{false};
  if (!orders.empty()) {
    is_order = true;
  }
  if (is_order) {
    auto schema = plan_->GetChildPlan()->OutputSchema();
    auto f = [&orders, &schema](Tuple &A, Tuple &B) {
      for (auto &p : orders) {
        auto a = p.second->Evaluate(&A, schema);
        auto b = p.second->Evaluate(&B, schema);
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
  }

  auto schema = WindowFunctionPlanNode::InferWindowSchema(plan_->columns_);
  if (is_order) {
    for (auto &t : sorted_l_) {
      //      uint32_t j = 0;
      //      for (auto it = plan_->window_functions_.begin(); it != plan_->window_functions_.end(); it++, j++) {
      auto key = MakeKey(&t);
      auto val = MakeVal(&t);

      std::vector<Value> v;
      for (auto &c : plan_->columns_) {
        auto col = dynamic_cast<ColumnValueExpression *>(c.get());
        if (col->GetColIdx() != UINT32_MAX) {
          auto x = c->Evaluate(&t, plan_->GetChildPlan()->OutputSchema());
          if (!x.IsNull()) {
            v.emplace_back(x);
          }
        }
      }

      auto new_val = wht_.InsertCombine(key, val, last_);
      last_ = val;
      //      if (i == plan_->window_functions_.begin()) {
      //        wht_[j].InsertCombine(key, val);
      //      } else {
      //        if (last_.val_) wht_[j].InsertCombine()
      //      }
      //      auto iter = wht_.Find(key);
      //      auto new_value = iter.Val();
      v.insert(v.end(), new_val.begin(), new_val.end());
      auto new_t = Tuple(v, &schema);
      res_.push_back(new_t);
      //    }
    }
  } else {
    //    std::cout << "not order\n";
    for (auto &t : sorted_l_) {
      auto key = MakeKey(&t);
      auto val = MakeVal(&t);
      wht_.InsertCombine(key, val, last_);
      last_ = val;
    }

    //    for (size_t m = 0; m != sorted_l_.size(); m++) {
    //    }
    //    std::vector<Value> new_val;

    wht_.InitIter();

    for (auto &t : sorted_l_) {
      auto new_val = wht_.GetValue();
      if (new_val.empty()) {
        //        std::cout << "break\n";
        break;
      }
      std::vector<Value> v;
      for (auto &c : plan_->columns_) {
        auto col = dynamic_cast<ColumnValueExpression *>(c.get());
        //        std::cout << col->GetColIdx() << std::endl;
        if (col->GetColIdx() != UINT32_MAX) {
          auto x = c->Evaluate(&t, plan_->GetChildPlan()->OutputSchema());
          if (!x.IsNull()) {
            v.emplace_back(x);
          }
        }
      }
      v.insert(v.end(), new_val.begin(), new_val.end());
      auto new_t = Tuple(v, &schema);
      //      std::cout << new_t.ToString(&schema) << std::endl;
      res_.push_back(new_t);
    }
  }
  iter_ = res_.cbegin();
  //  iter_ = sorted_l_.cbegin();
}
auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == res_.cend()) {
    return false;
  }

  //  std::cout << "next\n";
  *tuple = *iter_;
  //  for (auto c : plan_->columns_) {
  //    std::cout << c->Evaluate(tuple, schema) << " ";
  //  }

  auto schema = WindowFunctionPlanNode::InferWindowSchema(plan_->columns_);
  //  std::cout << schema.ToString() << std::endl;
  //  for (auto c : plan_->columns_) {
  //    std::cout << c->Evaluate(tuple, schema).ToString() << " ";
  //  }
  std::cout << (*iter_).ToString(&schema) << std::endl;
  *rid = tuple->GetRid();
  iter_++;
  return true;
}
}  // namespace bustub
