/*
 * @Author: victorika
 * @Date: 2025-12-03 16:15:50
 * @Last Modified by: victorika
 * @Last Modified time: 2025-12-09 10:59:06
 */

#pragma once

#include <atomic>
#include "intrusive_queue.hpp"
#include "macro.h"

namespace turbo_pool {

template <class T, T *T::*NextPtr>
class alignas(CACHE_LINE_SIZE) AtomicIntrusiveQueue {
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

  bool PushFront(node_pointer t) noexcept {
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
    queue.Clear();
  }

  IntrusiveQueue<T, NextPtr> PopAll() noexcept { return IntrusiveQueue<T, NextPtr>::Make(ResetHead()); }

  IntrusiveQueue<T, NextPtr> PopAllReversed() noexcept { return IntrusiveQueue<T, NextPtr>::MakeReversed(ResetHead()); }

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