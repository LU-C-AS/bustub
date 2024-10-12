//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

struct AggVal {
  //  std::vector<Value> val_;
  Value val_;
};

struct AggKey {
  std::vector<Value> key_;

  auto operator==(const AggKey &other) const -> bool {
    for (uint32_t i = 0; i < other.key_.size(); i++) {
      if (key_[i].CompareEquals(other.key_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::AggKey> {
  auto operator()(const bustub::AggKey &hash_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : hash_key.key_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

// template <>
// struct hash<std::vector<bustub::AbstractExpressionRef>> {
//   auto operator()(const std::vector<bustub::AbstractExpressionRef> &partition) const -> std::size_t {
//     size_t curr_hash = 0;
//     for (const auto &p : partition) {
//       auto v = bustub::Value(bustub::VARCHAR, p->ToString());
//       //      if (!p->ToString()) {
//       curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&v));
//       //      }
//     }
//     return curr_hash;
//   }
// };
//
}  // namespace std
namespace bustub {
class WinAggTable {
  // 如果一个表只存一个window function，那也可以直接用funtion初始化，不用switch
 public:
  explicit WinAggTable(const std::unordered_map<uint32_t, WindowFunctionPlanNode::WindowFunction> *win) {
    uint32_t i = 0;
    ht_.reserve(win->size());
    for (auto &w : *win) {
      agg_exprs_.push_back(w.second.function_);
      agg_types_.push_back(w.second.type_);
      //      std::cout << (w.second.type_ == WindowFunctionType::CountStarAggregate) << std::endl;
      if (w.second.type_ == WindowFunctionType::Rank) {
        rank_ids_.insert(i);
      }
      ht_.emplace_back();
      i++;
    }
    //    rank_id.reserve(agg_types_.size());
    //    for (uint32_t j = 0; i < agg_types_.size(); j++) {
    //      rank_id[j] = UINT32_MAX;
    //      for (uint32_t t = 0; t < agg_types_[i].size(); t++) {
    //        if (agg_types_[i][t] == WindowFunctionType::Rank) {
    //          rank_id[j] = t;
    //        }
    //      }
    //    }
    //    ht_.reserve(i);
    //    std::cout << "size " << agg_types_.size() << std::endl;
  }

  /** @return The initial aggregate value for this aggregation executor */
  auto GenerateInitialAggregateValue(uint32_t i) -> AggVal {
    //    std::vector<Value> values{};
    Value value;
    //    for (const auto &agg_type : agg_types_[i]) {
    switch (agg_types_[i]) {
      case WindowFunctionType::CountStarAggregate:
        // Count start starts at zero.
        //        values.emplace_back(ValueFactory::GetIntegerValue(0));
        value = ValueFactory::GetIntegerValue(0);
        break;
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
      case WindowFunctionType::Rank:
        // Others starts at null.
        //        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
        value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        break;
        //      }
    }

    return {value};
  }

  auto CombineAggregateValues(std::pair<uint32_t, AggVal> *res, const AggVal &input, uint32_t ind, bool is_eq = false)
      -> Value {
    //    std::cout << "combine " << input.val_.ToString() << std::endl;
    //    for (uint32_t i = 0; i < agg_exprs_.size(); i++) {

    auto result = &res->second;
    //    for (size_t i = 0; i < agg_types_[ind].size(); i++) {
    auto v_ori = result->val_.GetAs<int32_t>();
    auto in = input.val_.GetAs<int32_t>();
    //    auto v_ori = result->val_.GetAs<int32_t>();
    //    auto in = input.val_.GetAs<int32_t>();
    switch (agg_types_[ind]) {
        // 可能是aggr[i]，只存被aggr操作后的value，输出也是这些value
      case WindowFunctionType::CountStarAggregate:
        v_ori++;
        result->val_ = ValueFactory::GetIntegerValue(v_ori);
        break;
      case WindowFunctionType::CountAggregate:
        if (v_ori == ValueFactory::GetNullValueByType(INTEGER).GetAs<int32_t>()) {
          v_ori = 0;
        }
        if (in != ValueFactory::GetNullValueByType(INTEGER).GetAs<int32_t>()) {
          v_ori++;
        }
        result->val_ = ValueFactory::GetIntegerValue(v_ori);
        //        *result = ValueFactory::GetIntegerValue(v_ori);
        break;
      case WindowFunctionType::SumAggregate:
        if (v_ori == ValueFactory::GetNullValueByType(INTEGER).GetAs<int32_t>()) {
          v_ori = 0;
        }
        if (in != ValueFactory::GetNullValueByType(INTEGER).GetAs<int32_t>()) {
          v_ori = v_ori + in;
        }
        //        result->val_[i] = ValueFactory::GetIntegerValue(v_ori);
        result->val_ = ValueFactory::GetIntegerValue(v_ori);
        //        *result = ValueFactory::GetIntegerValue(v_ori);
        break;
      case WindowFunctionType::MinAggregate:
        if (v_ori == ValueFactory::GetNullValueByType(INTEGER).GetAs<int32_t>()) {
          v_ori = INT32_MAX;
        }
        if (in != ValueFactory::GetNullValueByType(INTEGER).GetAs<int32_t>()) {
          v_ori = v_ori < in ? v_ori : in;
        }
        //        result->val_[i] = ValueFactory::GetIntegerValue(v_ori);
        result->val_ = ValueFactory::GetIntegerValue(v_ori);
        //        *result = ValueFactory::GetIntegerValue(v_ori);
        break;
      case WindowFunctionType::MaxAggregate:
        if (v_ori == ValueFactory::GetNullValueByType(INTEGER).GetAs<int32_t>()) {
          v_ori = INT32_MIN;
        }
        if (in != ValueFactory::GetNullValueByType(INTEGER).GetAs<int32_t>()) {
          v_ori = v_ori > in ? v_ori : in;
        }
        //        result->val_[i] = ValueFactory::GetIntegerValue(v_ori);
        result->val_ = ValueFactory::GetIntegerValue(v_ori);
        //        *result = ValueFactory::GetIntegerValue(v_ori);
        break;
      case WindowFunctionType::Rank:
        // rank一定有orderby

        if (!is_eq) {
          result->val_ = ValueFactory::GetIntegerValue(res->first + 1);
        }
        break;
        //    }
        //      }
    }
    res->first++;
    //    std::cout << result->val_.ToString() << std::endl;
    return result->val_;
  }

  auto InsertCombine(const std::vector<AggKey> &agg_keys, const std::vector<AggVal> &agg_vals,
                     const std::vector<AggVal> &last_input) -> std::vector<Value> {
    std::vector<Value> res;
    //    std::cout << "insery\n";
    //    for (uint32_t i = 0; i < agg_keys.size(); i++) {
    for (int32_t i = agg_keys.size() - 1; i >= 0; i--) {
      //      auto j = partitions_[agg_keys[i].key_];
      bool rank_eq{false};
      if (ht_[i].count(agg_keys[i]) == 0) {
        //        std::cout << "count 0\n";
        ht_[i].insert({agg_keys[i], {0, GenerateInitialAggregateValue(i)}});
      } else {
        if (rank_ids_.find(i) != rank_ids_.end()) {
          rank_eq = agg_vals[i].val_.CompareEquals(last_input[i].val_) == CmpBool::CmpTrue;
        }
      }
      //      std::cout << "res\n";
      //      std::cout << (agg_types_[i] == WindowFunctionType::CountStarAggregate) << std::endl;
      auto ret = CombineAggregateValues(&ht_[i][agg_keys[i]], agg_vals[i], i, rank_eq);
      res.emplace_back(ret);
    }
    return res;
  }

  void Clear() { ht_.clear(); }

  auto operator[](uint32_t i) { return ht_[i]; }
  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<AggKey, std::pair<uint32_t, AggVal>>::iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const AggKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> std::pair<uint32_t, AggVal> & { return iter_->second; }

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
    std::unordered_map<AggKey, std::pair<uint32_t, AggVal>>::iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin(uint32_t i) -> Iterator { return Iterator{ht_[i].begin()}; }

  /** @return Iterator to the end of the hash table */
  auto End(uint32_t i) -> Iterator { return Iterator{ht_[i].end()}; }
  auto Find(const AggKey &key, uint32_t i) -> Iterator {
    auto it = ht_[i].find(key);
    return Iterator{it};
  }
  void InitIter() {
    for (uint32_t i = 0; i < ht_.size(); i++) {
      iters_.push_back(Begin(i));
    }
  }

  auto GetValue() -> std::vector<Value> {
    std::vector<Value> vals;

    for (int32_t i = ht_.size() - 1; i >= 0; i--) {
      //      auto p = &iters_[i].Val();
      if (iters_[i].Val().first == 0) {
        ++iters_[i];
        //        p=&iters_[i].Val();
      }
      if (iters_[i] == End(i)) {
        //        std::cout << "end\n";
        return {};
      }
      auto v = iters_[i].Val().second.val_;
      //      std::cout << "get v " << v.ToString() << std::endl;
      //      vals.insert(vals.end(), v.begin(), v.end());
      vals.emplace_back(v);
      iters_[i].Val().first--;
    }
    return vals;
  }

 private:
  // 也可以把这个变成vector
  std::vector<std::unordered_map<AggKey, std::pair<uint32_t, AggVal>>> ht_;
  std::vector<AbstractExpressionRef> agg_exprs_;
  std::vector<WindowFunctionType> agg_types_;
  std::vector<Iterator> iters_;
  //  std::unordered_map<std::vector<AbstractExpressionRef>, uint32_t> partitions_;
  std::unordered_set<uint32_t> rank_ids_;
};
/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  const WindowFunctionPlanNode *plan_;
  auto MakeKey(const Tuple *tuple
               /*std::unordered_map<uint32_t, WindowFunctionPlanNode::WindowFunction>::const_iterator &it*/)
      -> std::vector<AggKey> {
    std::vector<AggKey> keys{};
    keys.reserve(plan_->window_functions_.size());
    //    for (uint32_t i = 0; i < plan_->window_functions_.size(); i++) {
    //    auto it = plan_->window_functions_.begin();
    //      if(it->second.partition_by_.empty()){
    //
    //      }
    std::vector<Value> k{};
    uint32_t i = 0;
    // 按partition分组
    for (auto it = plan_->window_functions_.begin(); it != plan_->window_functions_.end(); it++, i++) {
      //      if (!it->second.partition_by_.empty()) {
      for (const auto &expr : it->second.partition_by_) {
        k.push_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
        //      }
      }

      //      std::cout << (it->second.type_ == WindowFunctionType::CountStarAggregate) << std::endl;
      if (k.empty()) {
        k.emplace_back(ValueFactory::GetIntegerValue(0));
      }
      keys.push_back(AggKey{k});
      //      } else {
      //        keys[i] = {};
      //      }
    }
    //    std::cout << keys.size() << std::endl;
    return keys;
  }

  /** @return The tuple as an AggregateValue */
  auto MakeVal(const Tuple *tuple
               /*std::unordered_map<uint32_t, WindowFunctionPlanNode::WindowFunction>::const_iterator &it*/)
      -> std::vector<AggVal> {
    std::vector<AggVal> vals;

    //    auto expr = it->second;
    for (const auto &expr : plan_->window_functions_) {
      auto f = expr.second.type_;
      if (f == WindowFunctionType::Rank) {
        auto c = expr.second.order_by_[0].second;
        vals.emplace_back(AggVal{c->Evaluate(tuple, child_executor_->GetOutputSchema())});
      } else {
        vals.emplace_back(AggVal{expr.second.function_->Evaluate(tuple, child_executor_->GetOutputSchema())});
      }
    }
    return vals;
  }
  /** The window aggregation plan node to be executed */

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<Tuple> sorted_l_{};
  std::vector<Tuple> res_{};
  std::vector<Tuple>::const_iterator iter_;

  // 存不同函数的partition_by;
  WinAggTable wht_;
  //  AggVal last_;
  std::vector<AggVal> last_;
  //  std::vector<WinAggTable::Iterator> ht_iters_;
};
}  // namespace bustub
