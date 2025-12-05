/*
 * @Author: victorika
 * @Date: 2025-12-04 16:30:56
 * @Last Modified by: victorika
 * @Last Modified time: 2025-12-04 17:16:40
 */
#pragma once

#include <iterator>
#include <utility>

namespace turbo_pool {

template <class T, T* T::* NextPtr>
class IntrusiveQueue {
 public:
  using value_type = T;
  using pointer = T*;
  using reference = T&;

  IntrusiveQueue() noexcept = default;

  // 移动构造
  IntrusiveQueue(IntrusiveQueue&& other) noexcept
      : head_(std::exchange(other.head_, nullptr)), tail_(std::exchange(other.tail_, nullptr)) {}

  // 原始指针构造
  IntrusiveQueue(T* head, T* tail) noexcept : head_(head), tail_(tail) {}

  // 移动赋值
  IntrusiveQueue& operator=(IntrusiveQueue other) noexcept {
    std::swap(head_, other.head_);
    std::swap(tail_, other.tail_);
    return *this;
  }

  ~IntrusiveQueue() {
    // 侵入式容器通常不负责销毁对象本身，只负责断言队列为空
    // 如果需要自动清理，使用者需要手动遍历 delete
    assert(Empty());
  }

  // 将一个原始链表反转并构建队列
  static IntrusiveQueue MakeReversed(T* list) noexcept {
    T* new_head = nullptr;
    T* new_tail = list;
    while (list != nullptr) {
      T* next = list->*NextPtr;
      list->*NextPtr = new_head;
      new_head = list;
      list = next;
    }

    IntrusiveQueue result;
    result.head_ = new_head;
    result.tail_ = new_tail;
    return result;
  }

  // 从原始链表构建队列（需要遍历找到尾部）
  static IntrusiveQueue Make(T* list) noexcept {
    IntrusiveQueue result;
    result.head_ = list;
    result.tail_ = list;
    if (list == nullptr) {
      return result;
    }
    while (result.tail_->*NextPtr != nullptr) {
      result.tail_ = result.tail_->*NextPtr;
    }
    return result;
  }

  [[nodiscard]]
  bool Empty() const {
    return head_ == nullptr;
  }

  void Clear() noexcept {
    head_ = nullptr;
    tail_ = nullptr;
  }

  [[nodiscard]]
  T* PopFront() noexcept {
    assert(!Empty());
    T* item = std::exchange(head_, head_->*NextPtr);

    // 如果取出的元素是最后一个，tail_ 也需要置空
    // 注意：这里 item->*NextPtr 此时应该指向原来的第二个元素或者 nullptr
    if (item->*NextPtr == nullptr) {
      tail_ = nullptr;
    }
    return item;
  }

  void PushFront(T* item) noexcept {
    assert(item != nullptr);
    item->*NextPtr = head_;
    head_ = item;
    if (tail_ == nullptr) {
      tail_ = item;
    }
  }

  void PushBack(T* item) noexcept {
    assert(item != nullptr);
    item->*NextPtr = nullptr;
    if (tail_ == nullptr) {
      head_ = item;
    } else {
      tail_->*NextPtr = item;
    }
    tail_ = item;
  }

  void Append(IntrusiveQueue other) noexcept {
    if (other.Empty()) {
      return;
    }
    auto* other_head = std::exchange(other.head_, nullptr);
    if (Empty()) {
      head_ = other_head;
    } else {
      tail_->*NextPtr = other_head;
    }
    tail_ = std::exchange(other.tail_, nullptr);
  }

  void Prepend(IntrusiveQueue other) noexcept {
    if (other.Empty()) {
      return;
    }

    other.tail_->*NextPtr = head_;
    head_ = other.head_;
    if (tail_ == nullptr) {
      tail_ = other.tail_;
    }

    other.tail_ = nullptr;
    other.head_ = nullptr;
  }

  // 迭代器定义
  struct Iterator {
    using difference_type = ptrdiff_t;
    using value_type = T*;
    using pointer = T**;
    using reference = T*&;
    using iterator_category = std::forward_iterator_tag;

    T* predecessor_ = nullptr;
    T* item_ = nullptr;

    Iterator() noexcept = default;

    explicit Iterator(T* pred, T* item) noexcept : predecessor_(pred), item_(item) {}

    [[nodiscard]]
    T* operator*() const noexcept {
      assert(item_ != nullptr);
      return item_;
    }

    [[nodiscard]]
    T** operator->() const noexcept {
      assert(item_ != nullptr);
      return &item_;
    }

    Iterator& operator++() noexcept {
      predecessor_ = item_;
      if (item_) {
        item_ = item_->*NextPtr;
      }
      return *this;
    }

    Iterator operator++(int) noexcept {
      Iterator result = *this;
      ++*this;
      return result;
    }

    friend bool operator==(const Iterator& a, const Iterator& b) noexcept { return a.item_ == b.item_; }

    friend bool operator!=(const Iterator& a, const Iterator& b) noexcept { return !(a == b); }
  };

  [[nodiscard]]
  Iterator begin() const noexcept {
    return Iterator(nullptr, head_);
  }

  [[nodiscard]]
  Iterator end() const noexcept {
    return Iterator(tail_, nullptr);
  }

  // 拼接操作：将 other 队列中 [first, last) 的元素移动到当前队列 pos 之前
  void splice(Iterator pos, IntrusiveQueue& other, Iterator first, Iterator last) noexcept {
    if (first == last) {
      return;
    }
    assert(first.item_ != nullptr);
    assert(last.predecessor_ != nullptr);

    // 从 other 中断开这段链表
    if (other.head_ == first.item_) {
      other.head_ = last.item_;
      if (other.head_ == nullptr) {
        other.tail_ = nullptr;
      }
    } else {
      assert(first.predecessor_ != nullptr);
      first.predecessor_->*NextPtr = last.item_;
      last.predecessor_->*NextPtr = pos.item_;
    }

    if (Empty()) {
      head_ = first.item_;
      tail_ = last.predecessor_;
    } else {
      pos.predecessor_->*NextPtr = first.item_;
      if (pos.item_ == nullptr) {
        tail_ = last.predecessor_;
      }
    }
  }

  T* front() const noexcept { return head_; }

  T* back() const noexcept { return tail_; }

 private:
  T* head_ = nullptr;
  T* tail_ = nullptr;
};

}  // namespace turbo_pool