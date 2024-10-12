#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  if (current_reads_.find(read_ts) == current_reads_.end()) {
    if (current_reads_.empty()) {
      watermark_ = read_ts;
    }
    current_reads_heap_.insert(read_ts);
    current_reads_.insert({read_ts, 1});
  } else {
    current_reads_[read_ts]++;
  }

  // TODO(fall2023): implement me!
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  if (current_reads_[read_ts] == 1) {
    current_reads_.erase(read_ts);
    current_reads_heap_.erase(read_ts);
    if (read_ts == watermark_) {
      watermark_ = *current_reads_heap_.begin();
    }
  } else {
    current_reads_[read_ts]--;
  }
  //  current_reads_.erase(read_ts);
  //  if (read_ts <= watermark_) {
  //    watermark_ = *current_reads_.begin();
  //  }
}

}  // namespace bustub
