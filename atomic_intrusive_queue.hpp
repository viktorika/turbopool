/*
 * @Author: victorika
 * @Date: 2025-12-03 16:15:50
 * @Last Modified by: victorika
 * @Last Modified time: 2025-12-04 17:11:59
 */

#pragma once

#include <atomic>
#include <iterator>
#include <utility>
#include "intrusive_queue.hpp"

namespace turbo_pool {

// class intrusive_queue {
//   pop_all_reversed();
// };

// class atomic_intrusive_queue {
//   void append(intrusive_queue &other);
// };

template <class T, T *T::*NextPtr>
class alignas(64) AtomicIntrusiveQueue {
 public:
  using node_pointer = T *;
  using atomic_node_pointer = std::atomic<T *>;

  [[nodiscard]]
  bool Empty() const noexcept {
    return head_.load(std::memory_order_relaxed) == nullptr;
  }

  struct TryPushResult {
    bool success;
    bool was_empty;
  };

  TryPushResult TryPushFront(node_pointer t) noexcept {
    node_pointer old_head = head_.load(std::memory_order_relaxed);
    t->*NextPtr = old_head;
    return {head_.compare_exchange_strong(old_head, t, std::memory_order_acq_rel), old_head == nullptr};
  }

  auto PushFront(node_pointer t) noexcept -> bool {
    node_pointer old_head = head_.load(std::memory_order_relaxed);
    do {
      t->*NextPtr = old_head;
    } while (!head_.compare_exchange_weak(old_head, t, std::memory_order_acq_rel));
    return old_head == nullptr;
  }

  void Prepend(IntrusiveQueue<T, NextPtr> queue) noexcept {
    node_pointer new_head = queue.front();
    node_pointer tail = queue.back();
    node_pointer old_head = head_.load(std::memory_order_relaxed);
    tail->*NextPtr = old_head;
    while (!head_.compare_exchange_weak(old_head, new_head, std::memory_order_acq_rel)) {
      tail->*NextPtr = old_head;
    }
    queue.clear();
  }

  IntrusiveQueue<T, NextPtr> PopAll() noexcept { return IntrusiveQueue<T, NextPtr>::Make(ResetHead()); }

  IntrusiveQueue<T, NextPtr> PopAllReversed() noexcept {
    return IntrusiveQueue<T, NextPtr>::MakeReversed(ResetHead());
  }

 private:
  node_pointer ResetHead() noexcept {
    node_pointer old_head = head_.load(std::memory_order_relaxed);
    while (!head_.compare_exchange_weak(old_head, nullptr, std::memory_order_acq_rel)) {
      ;
    }
    return old_head;
  }

  atomic_node_pointer head_{nullptr};
};

}  // namespace turbo_pool