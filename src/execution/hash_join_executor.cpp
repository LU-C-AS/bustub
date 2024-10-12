//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  // throw NotImplementedException("HashJoinExecutor is not implemented"); }
  left_child_->Init();
  right_child_->Init();
  l_iter_ = l_ht_.Begin();
  r_iter_ = r_ht_.End();
  l_vec_i_ = 0;
  r_vec_i_ = 0;
  auto l_schema = plan_->GetLeftPlan()->output_schema_;
  auto r_schema = plan_->GetRightPlan()->output_schema_;
  std::vector<Column> sch;
  for (const auto &column : l_schema->GetColumns()) {
    sch.emplace_back(column);
  }
  for (const auto &column : r_schema->GetColumns()) {
    sch.emplace_back(column);
  }
  schema_ = std::make_shared<const Schema>(sch);
  while (true) {
    Tuple c_t{};
    RID r{};
    auto s = left_child_->Next(&c_t, &r);
    if (!s) {
      break;
    }
    auto key = MakeHashKey(&c_t, true, plan_->GetLeftPlan()->OutputSchema());
    auto val = MakeHashVal(&c_t);
    l_ht_.Insert(key, val);
    //    std::cout << key.key_[0].ToString() << "l" << std::endl;
  }
  while (true) {
    Tuple c_t{};
    RID r{};
    auto s = right_child_->Next(&c_t, &r);
    if (!s) {
      break;
    }
    auto key = MakeHashKey(&c_t, false, plan_->GetRightPlan()->OutputSchema());
    auto val = MakeHashVal(&c_t);
    r_ht_.Insert(key, val);
    //    std::cout << key.key_[0].ToString() << "r" << std::endl;
  }
  l_iter_ = l_ht_.Begin();
  r_iter_ = r_ht_.End();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (l_iter_ == l_ht_.End()) {
    return false;
  }
  auto l_schema = plan_->GetLeftPlan()->output_schema_;
  auto r_schema = plan_->GetRightPlan()->output_schema_;

  if (r_iter_ == r_ht_.End()) {
    while (true) {
      if ((plan_->GetJoinType() == JoinType::LEFT && !on_doing_lj_) || plan_->GetJoinType() == JoinType::INNER) {
        auto key = l_iter_.Key();
        r_iter_ = r_ht_.Find(key);
        if (r_iter_ == r_ht_.End()) {
          if (plan_->GetJoinType() == JoinType::LEFT) {
            on_doing_lj_ = true;
            l_vec_i_ = 0;
          } else {
            ++l_iter_;
            if (l_iter_ == l_ht_.End()) {
              return false;
            }
          }
        } else {
          //          std::cout << "init " << l_iter_.Key().key_[0].ToString() << std::endl;
          l_vec_i_ = 0;
          r_vec_i_ = 0;
          break;
        }
      }
      if (on_doing_lj_) {
        // do left join
        std::vector<Value> val;
        val.reserve(schema_->GetColumnCount());
        for (uint32_t i = 0; i < l_schema->GetColumnCount(); i++) {
          val.emplace_back(l_iter_.Val()[l_vec_i_].val_.GetValue(l_schema.get(), i));
        }
        for (uint32_t i = 0; i < r_schema->GetColumnCount(); i++) {
          auto v = ValueFactory::GetNullValueByType(r_schema->GetColumn(i).GetType());
          val.emplace_back(v);
        }
        *tuple = Tuple(val, schema_.get());

        //        std::cout << tuple->ToString(schema_.get()) << "l" << std::endl;
        l_vec_i_++;
        if (l_vec_i_ == l_iter_.Val().size()) {
          //          l_vec_i_ = UINT32_MAX;
          on_doing_lj_ = false;
          ++l_iter_;
        }
        return true;
      }
    }
  }

  auto l_t = l_iter_.Val()[l_vec_i_];
  auto r_t = r_iter_.Val()[r_vec_i_];

  std::vector<Value> val;
  val.reserve(schema_->GetColumnCount());
  for (uint32_t i = 0; i < l_schema->GetColumnCount(); i++) {
    val.emplace_back(l_t.val_.GetValue(l_schema.get(), i));
  }
  for (uint32_t i = 0; i < r_schema->GetColumnCount(); i++) {
    //    auto v = ValueFactory::GetNullValueByType(r_schema->GetColumn(i).GetType());
    val.emplace_back(r_t.val_.GetValue(r_schema.get(), i));
  }
  *tuple = Tuple(val, schema_.get());
  //  std::cout << tuple->ToString(schema_.get()) << std::endl;
  if (r_vec_i_ == r_iter_.Val().size() - 1) {
    l_vec_i_++;
    if (l_vec_i_ == l_iter_.Val().size()) {
      ++l_iter_;
      r_iter_ = r_ht_.End();
    } else {
      r_vec_i_ = 0;
    }
  } else {
    r_vec_i_++;
  }
  return true;
}

}  // namespace bustub
