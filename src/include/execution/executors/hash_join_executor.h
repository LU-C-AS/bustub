//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct HashJoinKey {
  std::vector<Value> key_;
  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.key_.size(); i++) {
      if (key_[i].CompareEquals(other.key_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
struct HashJoinVal {
  Tuple val_;
};
}  // namespace bustub
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
// struct HashJoinKey {
//   std::vector<Value> key_;
//   auto operator==(const HashJoinKey &other) const -> bool {
//     for (uint32_t i = 0; i < other.key_.size(); i++) {
//       if (key_[i].CompareEquals(other.key_[i]) != CmpBool::CmpTrue) {
//         return false;
//       }
//     }
//     return true;
//   }
// };
// struct HashJoinVal {
//   std::vector<Tuple> val_;
// };

namespace std {

template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &hash_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : hash_key.key_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std
namespace bustub {
class HashJoinTable {
 public:
  void Clear() { ht_.clear(); }
  void Insert(const HashJoinKey &key, const HashJoinVal &val) {
    if (ht_.count(key) == 0) {
      ht_.insert({key, {val}});
    } else {
      auto i = ht_.find(key);
      i->second.emplace_back(val);
    }
  }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<HashJoinKey, std::vector<HashJoinVal>>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const HashJoinKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const std::vector<HashJoinVal> & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::unordered_map<HashJoinKey, std::vector<HashJoinVal>>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }
  auto Find(const HashJoinKey &key) -> Iterator {
    auto i = ht_.find(key);
    return Iterator{i};
    //    if (i == ht_.end()) {
    //      return ;
    //    } else {
    //      return &i->second;
    //    }
  }

 private:
  std::unordered_map<HashJoinKey, std::vector<HashJoinVal>> ht_;
};
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  HashJoinTable l_ht_{};
  HashJoinTable r_ht_{};
  //  std::unordered_map<HashJoinKey, std::vector<HashJoinVal>> l_ht_{};
  //  std::unordered_map<HashJoinKey, std::vector<HashJoinVal>> r_ht_{};
  HashJoinTable::Iterator l_iter_{l_ht_.Begin()};
  HashJoinTable::Iterator r_iter_{r_ht_.Begin()};
  size_t l_vec_i_{0};
  size_t r_vec_i_{0};
  bool on_doing_lj_{false};
  //  std::unordered_map<HashJoinKey, std::vector<HashJoinVal>>::const_iterator l_iter_;
  //  std::unordered_map<HashJoinKey, std::vector<HashJoinVal>>::const_iterator r_iter_;
  auto MakeHashKey(const Tuple *tuple, bool is_left, const Schema &schema) -> HashJoinKey {
    std::vector<Value> keys;
    std::vector<AbstractExpressionRef> exprs;
    if (is_left) {
      exprs = plan_->LeftJoinKeyExpressions();
      //      schema = plan_->GetLeftPlan()->OutputSchema();
    } else {
      exprs = plan_->RightJoinKeyExpressions();
      //      schema = plan_->GetRightPlan()->OutputSchema();
    }
    keys.reserve(exprs.size());
    for (const auto &expr : exprs) {
      keys.emplace_back(expr->Evaluate(tuple, schema));
    }
    return {keys};
  }
  auto MakeHashVal(const Tuple *tuple) -> HashJoinVal { return {*tuple}; }

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  SchemaRef schema_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
};

}  // namespace bustub
