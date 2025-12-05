/*
 * @Author: victorika
 * @Date: 2025-12-03 15:39:27
 * @Last Modified by: victorika
 * @Last Modified time: 2025-12-04 17:38:24
 */

#pragma once

#include <cassert>
#include <condition_variable>
#include <limits>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <span>
#include <thread>
#include "atomic_intrusive_queue.hpp"
#include "bwos_lifo_queue.hpp"
#include "intrusive_queue.hpp"
#include "xorshift.hpp"

namespace turbo_pool {
class TurboPool;

struct BWOSParams {
  size_t num_blocks{32};
  size_t block_size{8};
};

struct TaskBase {
  TaskBase *next = nullptr;
  void (*execute)(TaskBase *, uint32_t tid) noexcept = nullptr;
};

struct RemoteQueue {
  explicit RemoteQueue(size_t nthreads) noexcept : queues_(nthreads) {}
  explicit RemoteQueue(RemoteQueue *next, size_t nthreads) noexcept : next_(next), queues_(nthreads) {}
  RemoteQueue *next_ = nullptr;
  std::vector<AtomicIntrusiveQueue<TaskBase, &TaskBase::next>> queues_;
  std::thread::id id_ = std::this_thread::get_id();
  size_t index_ = std::numeric_limits<size_t>::max();
};

struct RemoteQueueList {
 private:
  std::atomic<RemoteQueue *> head_;
  RemoteQueue *tail_;
  size_t nthreads_;
  RemoteQueue this_remotes_;

 public:
  explicit RemoteQueueList(size_t nthreads) noexcept
      : head_{&this_remotes_},
        tail_{&this_remotes_},
        nthreads_(nthreads),
        this_remotes_(nthreads) {}  // TODO(victorika): 确认初始化顺序有没有问题
  ~RemoteQueueList() noexcept {
    RemoteQueue *head = head_.load(std::memory_order_acquire);
    while (head != tail_) {
      RemoteQueue *tmp = std::exchange(head, head->next_);
      delete tmp;
    }
  }
  IntrusiveQueue<TaskBase, &TaskBase::next> PopAllReversed(size_t tid) noexcept {
    RemoteQueue *head = head_.load(std::memory_order_acquire);
    IntrusiveQueue<TaskBase, &TaskBase::next> tasks;
    while (head != nullptr) {
      tasks.Append(head->queues_[tid].PopAllReversed());
      head = head->next_;
    }
    return tasks;
  }
  RemoteQueue *Get() {
    // TODO(victorika): 这段代码是否还有优化空间？
    thread_local std::thread::id this_id = std::this_thread::get_id();
    RemoteQueue *head = head_.load(std::memory_order_acquire);
    RemoteQueue *queue = head;
    while (queue != tail_) {
      if (queue->id_ == this_id) {
        return queue;
      }
      queue = queue->next_;
    }
    auto *new_head = new RemoteQueue{head, nthreads_};
    while (!head_.compare_exchange_weak(head, new_head, std::memory_order_acq_rel)) {
      new_head->next_ = head;
    }
    return new_head;
  }
};
class WorkStealingVictim {
 public:
  explicit WorkStealingVictim(bwos_lifo_queue *queue,  // TODO(victorika): 修改queue
                              uint32_t index, int numa_node) noexcept
      : queue_(queue), index_(index), numa_node_(numa_node) {}
  TaskBase *TrySteal() noexcept {
    // return queue_->steal_front(); TODO：实现steal_frong
  }
  [[nodiscard]] uint32_t GetIndex() const noexcept { return index_; }
  [[nodiscard]] int GetNumaNode() const noexcept { return numa_node_; }

 private:
  bwos_lifo_queue *queue_;
  uint32_t index_;
  int numa_node_;
};

// TODO(victorika): 确认是否需要保留numa相关逻辑

class ThreadState {
 public:
  struct PopResult {
    TaskBase *task;
    uint32_t queue_index;
  };
  explicit ThreadState(TurboPool *pool, uint32_t index, BWOSParams params) noexcept
      : index_(index),
        local_queue_(params.num_blocks, params.block_size),  // TODO(victorika): maybe need allocator?
        state_(state::kRunning),
        pool_(pool) {
    std::random_device rd;
    rng_.seed(rd);
  }
  PopResult Pop();
  void PushLocal(TaskBase *task);
  void PushLocal(IntrusiveQueue<TaskBase, &TaskBase::next> &&tasks);
  bool Notify();
  void RequestStop();
  void Victims(std::vector<WorkStealingVictim> const &victims) {
    for (WorkStealingVictim v : victims) {
      if (v.GetIndex() == index_) {
        continue;
      }
      // TODO(victorika): maybe need to filter by numa node
      all_victims_.push_back(v);
    }
  }
  [[nodiscard]] uint32_t GetIndex() const noexcept { return index_; }
  WorkStealingVictim AsVictim() noexcept {
    return WorkStealingVictim{&local_queue_, index_, 0};  // TODO(victorika): maybe need teleport numa node
  }

 private:
  enum state : uint8_t { kRunning, kSleeping, kNotified };

  PopResult TryPop();
  PopResult TryRemote();
  PopResult TrySteal(std::vector<WorkStealingVictim> &victims);  // TODO(victorika): 找个开源span库替换？
  PopResult TryStealAny();
  void NotifyOneSleeping();
  void SetStealing();
  void ClearStealing();
  void SetSleeping();
  void ClearSleeping();

  uint32_t index_;
  bwos_lifo_queue local_queue_;  // TODO(victorika):
  IntrusiveQueue<TaskBase, &TaskBase::next> pending_queue_;
  std::mutex mut_;
  std::condition_variable cv_;
  bool stop_requested_{false};
  std::vector<WorkStealingVictim> near_victims_;  // TODO(victorika): ，待确认为何没有这个
  std::vector<WorkStealingVictim> all_victims_;
  std::atomic<state> state_;
  TurboPool *pool_;
  xorshift rng_;
};
class TurboPool {
 public:
  TurboPool();
  explicit TurboPool(uint32_t thread_count, BWOSParams params = {});  // TODO(victorika): ,numa
  ~TurboPool();
  RemoteQueue *GetRemoteQueue() noexcept {
    RemoteQueue *queue = remotes_.Get();
    size_t index = 0;
    for (auto &t : threads_) {
      if (t.get_id() == queue->id_) {
        queue->index_ = index;
        break;
      }
      ++index;
    }
    return queue;
  }
  void RequestStop() noexcept;
  [[nodiscard]] uint32_t AvailableParallelism() const { return thread_count_; }
  [[nodiscard]] BWOSParams GetParams() const { return params_; }
  void Enqueue(TaskBase *task) noexcept;
  void Enqueue(RemoteQueue &queue, TaskBase *task) noexcept;
  void Enqueue(RemoteQueue &queue, TaskBase *task, size_t thread_index) noexcept;
  void Run(std::uint32_t index) noexcept;
  void Join() noexcept;
  alignas(64) std::atomic<std::uint32_t> num_active_{};
  alignas(64) RemoteQueueList remotes_;
  uint32_t thread_count_;
  uint32_t max_steals_{thread_count_ + 1};
  BWOSParams params_;
  std::vector<std::thread> threads_;
  std::vector<std::optional<ThreadState>> thread_states_;
  // TODO(victorika): numa
  [[nodiscard]] size_t NumThreads() const noexcept;
};

inline void MovePendingToLocal(IntrusiveQueue<TaskBase, &TaskBase::next> &pending_queue, bwos_lifo_queue &local_queue) {
  //   auto last = local_queue.push_back(pending_queue.begin(), pending_queue.end());
  //   intrusive_queue<&task_base::next> tmp{};
  //   tmp.splice(tmp.begin(), pending_queue, pending_queue.begin(), last);
  //   tmp.clear();
  // TODO(victorika): 待实现
}

inline ThreadState::PopResult ThreadState::TryRemote() {
  PopResult result{.task = nullptr, .queue_index = index_};
  IntrusiveQueue<TaskBase, &TaskBase::next> remotes = pool_->remotes_.PopAllReversed(index_);
  pending_queue_.Append(std::move(remotes));
  if (!pending_queue_.Empty()) {
    MovePendingToLocal(pending_queue_, local_queue_);
    //   result.task = local_queue_.PopBack();
    // TODO(victorika): 待实现
  }
  return result;
}

inline ThreadState::PopResult ThreadState::TryPop() {
  PopResult result{.task = nullptr, .queue_index = index_};
  //   result.task = local_queue_.pop_back();
  //   if (result.task) [[likely]] {
  //     return result;
  //   }
  // TODO(victorika): 待实现
  return TryRemote();
}

inline ThreadState::PopResult ThreadState::TrySteal(std::vector<WorkStealingVictim> &victims) {
  if (victims.empty()) {
    return {.task = nullptr, .queue_index = index_};
  }
  std::uniform_int_distribution<std::uint32_t> dist(0, static_cast<std::uint32_t>(victims.size() - 1));
  uint32_t victim_index = dist(rng_);
  auto &v = victims[victim_index];
  return {.task = v.TrySteal(), .queue_index = v.GetIndex()};
}

inline ThreadState::PopResult ThreadState::TryStealAny() { return TrySteal(all_victims_); }

inline void ThreadState::PushLocal(TaskBase *task) {
  //   if (!local_queue_.push_back(task)) {
  //     pending_queue_.push_back(task);
  //   }
  // TODO(victorika): 待实现
}

inline void ThreadState::PushLocal(IntrusiveQueue<TaskBase, &TaskBase::next> &&tasks) {
  pending_queue_.Prepend(std::move(tasks));
}

inline void ThreadState::SetStealing() {
  const std::uint32_t diff = 1u - (1u << 16u);
  pool_->num_active_.fetch_add(diff, std::memory_order_relaxed);
}

inline void ThreadState::ClearStealing() {
  constexpr std::uint32_t diff = 1 - (1u << 16u);
  const std::uint32_t num_active = pool_->num_active_.fetch_sub(diff, std::memory_order_relaxed);
  const std::uint32_t num_victims = num_active >> 16u;
  const std::uint32_t num_thiefs = num_active & 0xffffu;
  if (num_thiefs == 1 && num_victims != 0) {
    NotifyOneSleeping();
  }
}

inline void ThreadState::NotifyOneSleeping() {
  std::uniform_int_distribution<std::uint32_t> dist(0, pool_->thread_count_ - 1);
  std::uint32_t start_index = dist(rng_);
  for (std::uint32_t i = 0; i < pool_->thread_count_; ++i) {
    std::uint32_t index = (start_index + i) % pool_->thread_count_;
    if (index == index_) {
      continue;
    }
    if (pool_->thread_states_[index]->Notify()) {
      return;
    }
  }
}

inline ThreadState::PopResult ThreadState::Pop() {
  PopResult result = TryPop();
  while (result.task == nullptr) {
    SetStealing();
    // TODO(victorika): numa  near steal
    for (size_t i = 0; i < pool_->max_steals_; ++i) {
      result = TryStealAny();
      if (result.task != nullptr) {
        ClearStealing();
        return result;
      }
    }
    std::this_thread::yield();
    ClearStealing();
    std::unique_lock lock{mut_};
    if (stop_requested_) {
      return result;
    }
    state expected = state::kRunning;
    if (state_.compare_exchange_weak(expected, state::kSleeping, std::memory_order_relaxed)) {
      result = TryRemote();
      if (result.task != nullptr) {
        return result;
      }
      // TODO(victorika): 这部分跟摘抄实现不一致，回头再看看
      SetSleeping();
      cv_.wait(lock);
      lock.unlock();
      ClearSleeping();
    }
    if (lock.owns_lock()) {
      lock.unlock();
    }
    state_.store(state::kRunning, std::memory_order_relaxed);
    result = TryPop();
  }
  return result;
}

inline bool ThreadState::Notify() {
  if (state_.exchange(state::kNotified, std::memory_order_relaxed) == state::kSleeping) {
    {
      std::lock_guard lock{mut_};
    }
    cv_.notify_one();
    return true;
  }
  return false;
}

inline void ThreadState::RequestStop() {
  {
    std::lock_guard lock{mut_};
    stop_requested_ = true;
  }
  cv_.notify_one();
}

inline TurboPool::TurboPool() : TurboPool(std::thread::hardware_concurrency()) {}

inline TurboPool::TurboPool(uint32_t thread_count, BWOSParams params)  // TODO(victorika): numa
    : remotes_(thread_count), thread_count_(thread_count), params_(params), thread_states_(thread_count) {
  assert(thread_count > 0);
  for (uint32_t index = 0; index < thread_count; ++index) {
    thread_states_[index].emplace(this, index, params);  // TODO(victorika): numa
  }
  std::vector<WorkStealingVictim> victims;
  victims.reserve(thread_states_.size());
  for (auto &state : thread_states_) {
    victims.emplace_back(state->AsVictim());
  }
  for (auto &state : thread_states_) {
    state->Victims(victims);
  }
  threads_.reserve(thread_count);
  for (uint32_t i = 0; i < thread_count; ++i) {
    threads_.emplace_back([this, i] { Run(i); });
  }
}

inline TurboPool::~TurboPool() {
  RequestStop();
  Join();
}

inline void TurboPool::RequestStop() noexcept {
  for (auto &state : thread_states_) {
    state->RequestStop();
  }
}

inline void TurboPool::Run(uint32_t thread_index) noexcept {
  assert(thread_index < thread_count_);
  // TODO(victorika): numa
  while (true) {
    auto [task, queue_index] = thread_states_[thread_index]->Pop();
    if (task == nullptr) {
      return;
    }
    task->execute(task, queue_index);
  }
}

inline void TurboPool::Join() noexcept {
  for (auto &t : threads_) {
    t.join();
  }
  threads_.clear();
}

inline void TurboPool::Enqueue(TaskBase *task) noexcept {
  Enqueue(*GetRemoteQueue(), task);
}  // TODO(victorika): constraints不知道有没有用

inline size_t TurboPool::NumThreads() const noexcept {
  // TODO(victorika): 这个实现跟原版也不一样，需要确认有没有问题
  return thread_count_;
}

inline void TurboPool::Enqueue(RemoteQueue &queue,
                               TaskBase *task) noexcept {  // TODO(victorika): 确认constraints变量有没有用
  thread_local std::thread::id this_id = std::this_thread::get_id();
  RemoteQueue *correct_queue =
      this_id == queue.id_ ? &queue : GetRemoteQueue();  // TODO(victorika): 这里的校验是不是有必要？
  size_t idx = correct_queue->index_;
  if (idx < thread_states_.size()) {
    // TODO(victorika): 同样有一些numa处理需要确认
    thread_states_[idx]->PushLocal(task);
    return;
  }
  const size_t thread_index = uint64_t(std::random_device{}()) % NumThreads();
  queue.queues_[thread_index].PushFront(task);
  thread_states_[thread_index]->Notify();
}

inline void TurboPool::Enqueue(RemoteQueue &queue, TaskBase *task, size_t thread_index) noexcept {
  thread_index %= thread_count_;
  queue.queues_[thread_index].PushFront(task);
  thread_states_[thread_index]->Notify();
}

}  // namespace turbo_pool