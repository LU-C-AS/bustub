//
// Created by lucas on 2024/3/27.
//
#include <stdio.h>
#include
using namespace std;
int main() {
  function<bool(int, frame_id_t)> comp = [this](frame_id_t i1, frame_id_t i2) {
    auto n1 = nodes_[i1];
    auto n2 = nodes_[i2];

    if (n1.GetTime() >= n2.GetTime()) {
      return true;
    }
    return false;
  };

  nk_l_ = std::multiset<frame_id_t, std::function<bool(frame_id_t, frame_id_t)>>(comp);
}
