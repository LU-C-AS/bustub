#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  //  std::cout << "optimize index\n";
  //  std::cout << plan->GetChildren().size() << std::endl;
  //  std::cout << plan->GetType() == PlanType::SeqScan << std::endl;
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto opt_plan = plan->CloneWithChildren(std::move(children));

  //  std::vector<AbstractPlanNodeRef> c;
  //  auto opt_plan = plan->CloneWithChildren(std::move(c));
  if (opt_plan->GetType() == PlanType::SeqScan) {
    //    std::cout << "cast seq\n";
    auto seq_plan = dynamic_cast<const SeqScanPlanNode &>(*opt_plan);
    //    if (seq_plan.filter_predicate_) {
    //      std::cout << seq_plan.filter_predicate_->children_.size() << std::endl;
    //    }
    //    std::cout << seq_plan.filter_predicate_->children_.size() << "\n";

    auto pred = seq_plan.filter_predicate_;
    //    if (pred) {
    //      std::cout << pred->ToString() << std::endl;
    //    }
    if (pred && pred->children_.size() == 2 && pred->children_[0]->children_.empty() &&
        pred->children_[1]->children_.empty()) {
      auto comp = dynamic_cast<ComparisonExpression *>(pred.get());
      if (comp == nullptr || comp->comp_type_ != ComparisonType::Equal) {
        return opt_plan;
      }

      auto col_expr = dynamic_cast<ColumnValueExpression &>(*seq_plan.filter_predicate_->children_[0]);

      //      std::cout << col_expr.GetColIdx() << std::endl;

      auto table_name = seq_plan.table_name_;
      auto inds = catalog_.GetTableIndexes(table_name);
      auto table_info = catalog_.GetTable(seq_plan.table_name_);
      auto table_schema = table_info->schema_;
      for (auto &i : inds) {
        //        std::cout << i->key_schema_.GetColIdx(i->name_) << " " << col_expr.GetColIdx() << " "
        //                  << seq_plan.output_schema_->GetColumn(col_expr.GetColIdx()).GetName() << std::endl;
        //        auto col_nam = seq_plan.output_schema_->GetColumn(col_expr.GetColIdx()).GetName();
        //        auto col_name = table_schema.GetColumn(col_expr.GetColIdx()).GetName();
        //            col_nam.substr(seq_plan.table_name_.size() + 1);

        //        std::cout << i->key_schema_.GetColumnCount() << " " << i->key_schema_.GetColumn(0).GetName() << " " <<
        //        col_name
        //                  << std::endl;

        if (col_expr.GetColIdx() == i->index_->GetMetadata()->GetKeyAttrs()[0]) {
          //        if (i->key_schema_.GetColumnCount() == 1 && i->key_schema_.GetColumns()[0].GetName() == col_name) {
          //          auto const_key = dynamic_cast<ConstantValueExpression
          //          &>(*seq_plan.filter_predicate_->children_[1]);
          auto ind_id = i->index_oid_;
          //          std::cout << "indscan" << std::endl;
          auto con = dynamic_cast<ConstantValueExpression *>(seq_plan.filter_predicate_->children_[1].get());
          BUSTUB_ASSERT(con != nullptr, "seq2ind\n");
          return std::make_shared<IndexScanPlanNode>(
              seq_plan.output_schema_, seq_plan.table_oid_, ind_id, seq_plan.filter_predicate_,
              &dynamic_cast<ConstantValueExpression &>(*seq_plan.filter_predicate_->children_[1]));
        }
      }
    }
  }
  //  std::cout << "index\n";
  return opt_plan;
}

}  // namespace bustub
