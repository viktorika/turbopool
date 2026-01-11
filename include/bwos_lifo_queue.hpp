/*
 * @Author: victorika
 * @Date: 2025-12-03 16:40:30
 * @Last Modified by: victorika
 * @Last Modified time: 2025-12-09 11:08:19
 */
#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <vector>
#include "macro.h"
#include "spin_loop_pause.h"

namespace turbo_pool {

/**
 * This is an implementation of the BWOS queue as described in
 * BWoS: Formally Verified Block-based Work Stealing for Parallel Processing (Wang et al. 2023)
 */

enum class BWOSLifoQueueErrorCode : uint8_t {
  kSuccess,
  kDone,
  kEmpty,
  kFull,
  kConflict,
};

template <class Tp>
struct FetchResult {
  BWOSLifoQueueErrorCode status;
  Tp value;
};

struct TakeoverResult {
  size_t front;
  size_t back;
};

inline size_t bit_ceil(size_t x) noexcept {
#ifdef __clang__
  return 1 << (32 - __builtin_clz(x - 1));
#elif defined(__GNUC__)
  return 1 << (32 - __builtin_clz(x - 1));
#else
  x--;
  x |= x >> 1;
  x |= x >> 2;
  x |= x >> 4;
  x |= x >> 8;
  x |= x >> 16;
  return x + 1;
#endif
}

template <class Tp, class Allocator = std::allocator<Tp>>
class BWOSLifoQueue {
 public:
  explicit BWOSLifoQueue(size_t num_blocks, size_t block_size, Allocator allocator = Allocator());

  Tp PopBack() noexcept;

  Tp StealFront() noexcept;

  bool PushBack(Tp value) noexcept;

  template <class Iterator, class Sentinel>
  Iterator PushBack(Iterator first, Sentinel last) noexcept;

  [[nodiscard]]
  size_t GetAvailableCapacity() const noexcept;

  [[nodiscard]]
  size_t GetFreeCapacity() const noexcept;

  [[nodiscard]]
  size_t GetBlockSize() const noexcept;

  [[nodiscard]]
  size_t GetNumBlocks() const noexcept;

 private:
  template <class Sp>
  using allocator_of_t = typename std::allocator_traits<Allocator>::template rebind_alloc<Sp>;

  struct BlockType {
    explicit BlockType(size_t block_size, Allocator allocator = Allocator());

    BlockType(const BlockType & /*other*/);
    BlockType &operator=(const BlockType & /*other*/);

    BlockType(BlockType && /*other*/) noexcept;
    BlockType &operator=(BlockType && /*other*/) noexcept;

    BWOSLifoQueueErrorCode Put(Tp value) noexcept;

    template <class Iterator, class Sentinel>
    Iterator BulkPut(Iterator first, Sentinel last) noexcept;

    FetchResult<Tp> Get() noexcept;

    FetchResult<Tp> Steal() noexcept;

    TakeoverResult Takeover() noexcept;

    [[nodiscard]]
    bool IsWritable() const noexcept;

    [[nodiscard]]
    size_t FreeCapacity() const noexcept;

    void Grant() noexcept;

    bool Reclaim() noexcept;

    [[nodiscard]]
    bool IsStealable() const noexcept;

    [[nodiscard]]
    size_t GetBlockSize() const noexcept;

    alignas(CACHE_LINE_SIZE) std::atomic<std::uint64_t> head_;
    alignas(CACHE_LINE_SIZE) std::atomic<std::uint64_t> tail_;
    alignas(CACHE_LINE_SIZE) std::atomic<std::uint64_t> steal_head_;
    alignas(CACHE_LINE_SIZE) std::atomic<std::uint64_t> steal_tail_;
    std::vector<Tp, Allocator> ring_buffer_;
  };

  bool AdvanceGetIndex() noexcept;
  bool AdvanceStealIndex(size_t expected_thief_counter) noexcept;
  bool AdvancePutIndex() noexcept;

  alignas(CACHE_LINE_SIZE) std::atomic<size_t> owner_block_{1};
  alignas(CACHE_LINE_SIZE) std::atomic<size_t> thief_block_{0};
  std::vector<BlockType, allocator_of_t<BlockType>> blocks_;
  size_t mask_;
};

/////////////////////////////////////////////////////////////////////////////
// Implementation of lifo_queue member methods

template <class Tp, class Allocator>
BWOSLifoQueue<Tp, Allocator>::BWOSLifoQueue(size_t num_blocks, size_t block_size, Allocator allocator)
    : blocks_(std::max(static_cast<size_t>(2), bit_ceil(num_blocks)), BlockType(block_size, allocator),
              allocator_of_t<BlockType>(allocator)),
      mask_(blocks_.size() - 1) {
  blocks_[owner_block_.load()].Reclaim();  // TODO(victorika): 这个逻辑是否有意义？
}

template <class Tp, class Allocator>
Tp BWOSLifoQueue<Tp, Allocator>::PopBack() noexcept {
  do {
    size_t owner_index = owner_block_.load(std::memory_order_relaxed) & mask_;
    auto &current_block = blocks_[owner_index];
    auto [ec, value] = current_block.Get();
    if (ec == BWOSLifoQueueErrorCode::kSuccess) {
      return value;
    }
    if (ec == BWOSLifoQueueErrorCode::kDone) {
      return Tp{};
    }
  } while (AdvanceGetIndex());
  return Tp{};
}

template <class Tp, class Allocator>
Tp BWOSLifoQueue<Tp, Allocator>::StealFront() noexcept {
  size_t thief = 0;
  do {
    thief = thief_block_.load(std::memory_order_acquire);
    size_t thief_index = thief & mask_;
    auto &block = blocks_[thief_index];
    auto result = block.Steal();
    while (result.status != BWOSLifoQueueErrorCode::kDone) {
      if (result.status == BWOSLifoQueueErrorCode::kSuccess) {
        return result.value;
      }
      if (result.status == BWOSLifoQueueErrorCode::kEmpty) {
        return Tp{};
      }
      result = block.Steal();
    }
  } while (AdvanceStealIndex(thief));
  return Tp{};
}

template <class Tp, class Allocator>
bool BWOSLifoQueue<Tp, Allocator>::PushBack(Tp value) noexcept {
  do {
    size_t owner_index = owner_block_.load(std::memory_order_relaxed) & mask_;
    auto &current_block = blocks_[owner_index];
    auto ec = current_block.Put(value);
    if (ec == BWOSLifoQueueErrorCode::kSuccess) {
      return true;
    }
  } while (AdvancePutIndex());
  return false;
}

template <class Tp, class Allocator>
template <class Iterator, class Sentinel>
Iterator BWOSLifoQueue<Tp, Allocator>::PushBack(Iterator first, Sentinel last) noexcept {
  do {
    size_t owner_index = owner_block_.load(std::memory_order_relaxed) & mask_;
    auto &current_block = blocks_[owner_index];
    first = current_block.BulkPut(first, last);
  } while (first != last && AdvancePutIndex());
  return first;
}

template <class Tp, class Allocator>
size_t BWOSLifoQueue<Tp, Allocator>::GetFreeCapacity() const noexcept {
  size_t owner_counter = owner_block_.load(std::memory_order_relaxed);
  size_t owner_index = owner_counter & mask_;
  size_t local_capacity = blocks_[owner_index].FreeCapacity();
  size_t thief_counter = thief_block_.load(std::memory_order_relaxed);
  size_t diff = owner_counter - thief_counter;
  size_t rest = blocks_.size() - diff - 1;
  return local_capacity + (rest * GetBlockSize());
}

template <class Tp, class Allocator>
size_t BWOSLifoQueue<Tp, Allocator>::GetAvailableCapacity() const noexcept {
  return GetNumBlocks() * GetBlockSize();
}

template <class Tp, class Allocator>
size_t BWOSLifoQueue<Tp, Allocator>::GetBlockSize() const noexcept {
  return blocks_[0].GetBlockSize();
}

template <class Tp, class Allocator>
size_t BWOSLifoQueue<Tp, Allocator>::GetNumBlocks() const noexcept {
  return blocks_.size();
}

template <class Tp, class Allocator>
bool BWOSLifoQueue<Tp, Allocator>::AdvanceGetIndex() noexcept {
  size_t owner_counter = owner_block_.load(std::memory_order_relaxed);
  size_t predecessor = owner_counter - 1UL;
  size_t predecessor_index = predecessor & mask_;
  auto &previous_block = blocks_[predecessor_index];
  auto result = previous_block.Takeover();
  if (result.front != result.back) {
    size_t thief_counter = thief_block_.load(std::memory_order_relaxed);
    if (thief_counter == predecessor) {
      predecessor += blocks_.size();
      thief_counter += blocks_.size() - 1UL;
      thief_block_.store(thief_counter, std::memory_order_release);
    }
    owner_block_.store(predecessor, std::memory_order_relaxed);
    return true;
  }
  return false;
}

template <class Tp, class Allocator>
bool BWOSLifoQueue<Tp, Allocator>::AdvancePutIndex() noexcept {
  size_t owner_counter = owner_block_.load(std::memory_order_relaxed);
  size_t next_counter = owner_counter + 1UL;
  size_t thief_counter = thief_block_.load(std::memory_order_relaxed);
  assert(thief_counter < next_counter);
  if (next_counter - thief_counter >= blocks_.size()) {
    return false;
  }
  size_t next_index = next_counter & mask_;
  auto &next_block = blocks_[next_index];
  if (unlikely(!next_block.IsWritable())) {
    return false;
  }
  size_t owner_index = owner_counter & mask_;
  auto &current_block = blocks_[owner_index];
  current_block.Grant();
  owner_block_.store(next_counter, std::memory_order_relaxed);
  next_block.Reclaim();
  return true;
}

template <class Tp, class Allocator>
bool BWOSLifoQueue<Tp, Allocator>::AdvanceStealIndex(size_t expected_thief_counter) noexcept {
  size_t thief_counter = expected_thief_counter;
  size_t next_counter = thief_counter + 1;
  size_t next_index = next_counter & mask_;
  auto &next_block = blocks_[next_index];
  if (next_block.IsStealable()) {
    thief_block_.compare_exchange_strong(thief_counter, next_counter, std::memory_order_acq_rel);
    return true;
  }
  return thief_block_.load(std::memory_order_relaxed) != thief_counter;
}

/////////////////////////////////////////////////////////////////////////////
// Implementation of lifo_queue::block_type member methods

template <class Tp, class Allocator>
BWOSLifoQueue<Tp, Allocator>::BlockType::BlockType(size_t block_size, Allocator allocator)
    : head_{0}, tail_{0}, steal_head_{0}, steal_tail_{block_size}, ring_buffer_(block_size, allocator) {}

template <class Tp, class Allocator>
BWOSLifoQueue<Tp, Allocator>::BlockType::BlockType(const BlockType &other) : ring_buffer_(other.ring_buffer_) {
  head_.store(other.head_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  tail_.store(other.tail_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  steal_tail_.store(other.steal_tail_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  steal_head_.store(other.steal_head_.load(std::memory_order_relaxed), std::memory_order_relaxed);
}

template <class Tp, class Allocator>
typename BWOSLifoQueue<Tp, Allocator>::BlockType &BWOSLifoQueue<Tp, Allocator>::BlockType::operator=(
    const BlockType &other) {
  head_.store(other.head_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  tail_.store(other.tail_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  steal_tail_.store(other.steal_tail_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  steal_head_.store(other.steal_head_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  ring_buffer_ = other.ring_buffer_;
  return *this;
}

template <class Tp, class Allocator>
BWOSLifoQueue<Tp, Allocator>::BlockType::BlockType(BlockType &&other) noexcept {
  head_.store(other.head_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  tail_.store(other.tail_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  steal_tail_.store(other.steal_tail_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  steal_head_.store(other.steal_head_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  ring_buffer_ = std::exchange(std::move(other.ring_buffer_), {});
}

template <class Tp, class Allocator>
typename BWOSLifoQueue<Tp, Allocator>::BlockType &BWOSLifoQueue<Tp, Allocator>::BlockType::operator=(
    BlockType &&other) noexcept {
  head_.store(other.head_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  tail_.store(other.tail_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  steal_tail_.store(other.steal_tail_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  steal_head_.store(other.steal_head_.load(std::memory_order_relaxed), std::memory_order_relaxed);
  ring_buffer_ = std::exchange(std::move(other.ring_buffer_), {});
  return *this;
}

template <class Tp, class Allocator>
BWOSLifoQueueErrorCode BWOSLifoQueue<Tp, Allocator>::BlockType::Put(Tp value) noexcept {
  std::uint64_t back = tail_.load(std::memory_order_relaxed);
  if (likely(back < GetBlockSize())) {
    ring_buffer_[static_cast<size_t>(back)] = static_cast<Tp &&>(value);
    tail_.store(back + 1, std::memory_order_release);
    return BWOSLifoQueueErrorCode::kSuccess;
  }
  return BWOSLifoQueueErrorCode::kFull;
}

template <class Tp, class Allocator>
template <class Iterator, class Sentinel>
Iterator BWOSLifoQueue<Tp, Allocator>::BlockType::BulkPut(Iterator first, Sentinel last) noexcept {
  std::uint64_t back = tail_.load(std::memory_order_relaxed);
  while (first != last && back < GetBlockSize()) {
    ring_buffer_[static_cast<size_t>(back)] = static_cast<Tp &&>(*first);
    ++back;
    ++first;
  }
  tail_.store(back, std::memory_order_release);
  return first;
}

template <class Tp, class Allocator>
FetchResult<Tp> BWOSLifoQueue<Tp, Allocator>::BlockType::Get() noexcept {
  std::uint64_t front = head_.load(std::memory_order_relaxed);
  if (unlikely(front == GetBlockSize())) {
    return {BWOSLifoQueueErrorCode::kDone, nullptr};
  }
  std::uint64_t back = tail_.load(std::memory_order_acquire);
  if (unlikely(front == back)) {
    return {BWOSLifoQueueErrorCode::kEmpty, nullptr};
  }
  Tp value = static_cast<Tp &&>(ring_buffer_[static_cast<size_t>(back - 1)]);
  tail_.store(back - 1, std::memory_order_release);
  return {BWOSLifoQueueErrorCode::kSuccess, value};
}

template <class Tp, class Allocator>
FetchResult<Tp> BWOSLifoQueue<Tp, Allocator>::BlockType::Steal() noexcept {
  std::uint64_t spos = steal_tail_.load(std::memory_order_acquire);
  FetchResult<Tp> result{};
  if (unlikely(spos == GetBlockSize())) {
    result.status = BWOSLifoQueueErrorCode::kDone;
    return result;
  }
  std::uint64_t back = tail_.load(std::memory_order_acquire);
  if (unlikely(spos == back)) {
    result.status = BWOSLifoQueueErrorCode::kEmpty;
    return result;
  }
  if (!steal_tail_.compare_exchange_strong(spos, spos + 1, std::memory_order_acquire)) {
    result.status = BWOSLifoQueueErrorCode::kConflict;
    return result;
  }
  result.value = static_cast<Tp &&>(ring_buffer_[static_cast<size_t>(spos)]);
  steal_head_.fetch_add(1, std::memory_order_release);
  result.status = BWOSLifoQueueErrorCode::kSuccess;
  return result;
}

template <class Tp, class Allocator>
TakeoverResult BWOSLifoQueue<Tp, Allocator>::BlockType::Takeover() noexcept {
  std::uint64_t spos = steal_tail_.exchange(GetBlockSize(), std::memory_order_acq_rel);
  if (unlikely(spos == GetBlockSize())) {
    return {.front = static_cast<size_t>(head_.load(std::memory_order_relaxed)),
            .back = static_cast<size_t>(tail_.load(std::memory_order_relaxed))};
  }
  head_.store(spos, std::memory_order_relaxed);
  return {.front = static_cast<size_t>(spos), .back = static_cast<size_t>(tail_.load(std::memory_order_relaxed))};
}

template <class Tp, class Allocator>
bool BWOSLifoQueue<Tp, Allocator>::BlockType::IsWritable() const noexcept {
  std::uint64_t expected_steal = GetBlockSize();
  std::uint64_t spos = steal_tail_.load(std::memory_order_relaxed);
  return spos == expected_steal;
}

template <class Tp, class Allocator>
size_t BWOSLifoQueue<Tp, Allocator>::BlockType::FreeCapacity() const noexcept {
  std::uint64_t back = tail_.load(std::memory_order_relaxed);
  return GetBlockSize() - back;
}

template <class Tp, class Allocator>
bool BWOSLifoQueue<Tp, Allocator>::BlockType::Reclaim() noexcept {
  std::uint64_t expected_steal_head = tail_.load(std::memory_order_relaxed);
  while (steal_head_.load(std::memory_order_acquire) != expected_steal_head) {
    SpinLoopPause();
  }
  head_.store(0, std::memory_order_relaxed);
  tail_.store(0, std::memory_order_relaxed);
  steal_tail_.store(GetBlockSize(), std::memory_order_relaxed);
  steal_head_.store(0, std::memory_order_relaxed);
  return false;
}

template <class Tp, class Allocator>
size_t BWOSLifoQueue<Tp, Allocator>::BlockType::GetBlockSize() const noexcept {
  return ring_buffer_.size();
}

template <class Tp, class Allocator>
void BWOSLifoQueue<Tp, Allocator>::BlockType::Grant() noexcept {
  std::uint64_t old_head = head_.exchange(GetBlockSize(), std::memory_order_relaxed);
  steal_tail_.store(old_head, std::memory_order_release);
}

template <class Tp, class Allocator>
bool BWOSLifoQueue<Tp, Allocator>::BlockType::IsStealable() const noexcept {
  return steal_tail_.load(std::memory_order_acquire) != GetBlockSize();
}

}  // namespace turbo_pool