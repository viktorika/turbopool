/*
 * @Author: victorika
 * @Date: 2025-12-03 15:39:27
 * @Last Modified by: victorika
 * @Last Modified time: 2025-12-09 11:05:02
 */

#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <functional>
#include <limits>
#include <mutex>
#include <optional>
#include <thread>
#include "atomic_intrusive_queue.hpp"
#include "bwos_lifo_queue.hpp"
#include "intrusive_queue.hpp"
#include "macro.h"
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
      : head_{&this_remotes_}, tail_{&this_remotes_}, nthreads_(nthreads), this_remotes_(nthreads) {}
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
  explicit WorkStealingVictim(BWOSLifoQueue<TaskBase *> *queue, uint32_t index) noexcept
      : queue_(queue), index_(index) {}
  TaskBase *TrySteal() noexcept { return queue_->StealFront(); }
  [[nodiscard]] uint32_t GetIndex() const noexcept { return index_; }

 private:
  BWOSLifoQueue<TaskBase *> *queue_;
  uint32_t index_;
};

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
      all_victims_.push_back(v);
    }
  }
  [[nodiscard]] uint32_t GetIndex() const noexcept { return index_; }
  WorkStealingVictim AsVictim() noexcept { return WorkStealingVictim{&local_queue_, index_}; }

 private:
  enum state : uint8_t { kRunning, kSleeping, kNotified };

  PopResult TryPop();
  PopResult TryRemote();
  PopResult TrySteal(std::vector<WorkStealingVictim> &victims);
  PopResult TryStealAny();
  void NotifyOneSleeping();
  void SetStealing();
  void ClearStealing();
  void SetSleeping();
  void ClearSleeping();

  uint32_t index_;
  BWOSLifoQueue<TaskBase *> local_queue_;
  IntrusiveQueue<TaskBase, &TaskBase::next> pending_queue_;
  std::mutex mut_;
  std::condition_variable cv_;
  bool stop_requested_{false};
  std::vector<WorkStealingVictim> all_victims_;
  std::atomic<state> state_;
  TurboPool *pool_;
  Xorshift rng_;
};
class TurboPool {
 public:
  TurboPool();
  explicit TurboPool(uint32_t thread_count, BWOSParams params = {});
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
  [[nodiscard]] BWOSParams GetParams() const { return params_; }
  void Enqueue(TaskBase *task) noexcept;
  void Enqueue(RemoteQueue &queue, TaskBase *task) noexcept;
  void Enqueue(RemoteQueue &queue, TaskBase *task, size_t thread_index) noexcept;
  void Enqueue(RemoteQueue &queue, IntrusiveQueue<TaskBase, &TaskBase::next> tasks) noexcept;
  void Enqueue(RemoteQueue &queue, IntrusiveQueue<TaskBase, &TaskBase::next> tasks, size_t thread_index) noexcept;
  void Run(std::uint32_t index) noexcept;
  void Join() noexcept;
  alignas(CACHE_LINE_SIZE) std::atomic<std::uint32_t> num_active_{};
  alignas(CACHE_LINE_SIZE) RemoteQueueList remotes_;
  uint32_t thread_count_;
  uint32_t max_steals_{thread_count_ + 1};
  BWOSParams params_;
  std::vector<std::thread> threads_;
  std::vector<std::optional<ThreadState>> thread_states_;
  [[nodiscard]] size_t NumThreads() const noexcept;
};

inline void MovePendingToLocal(IntrusiveQueue<TaskBase, &TaskBase::next> &pending_queue,
                               BWOSLifoQueue<TaskBase *> &local_queue) {
  auto last = local_queue.PushBack(pending_queue.begin(), pending_queue.end());
  IntrusiveQueue<TaskBase, &TaskBase::next> tmp{};
  tmp.Splice(tmp.begin(), pending_queue, pending_queue.begin(), last);
  tmp.Clear();
}

inline ThreadState::PopResult ThreadState::TryRemote() {
  PopResult result{.task = nullptr, .queue_index = index_};
  IntrusiveQueue<TaskBase, &TaskBase::next> remotes = pool_->remotes_.PopAllReversed(index_);
  pending_queue_.Append(std::move(remotes));
  if (!pending_queue_.Empty()) {
    MovePendingToLocal(pending_queue_, local_queue_);
    result.task = local_queue_.PopBack();
  }
  return result;
}

inline ThreadState::PopResult ThreadState::TryPop() {
  PopResult result{.task = nullptr, .queue_index = index_};
  result.task = local_queue_.PopBack();
  if (likely(result.task != nullptr)) {
    return result;
  }
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
  if (!local_queue_.PushBack(task)) {
    pending_queue_.PushBack(task);
  }
}

inline void ThreadState::PushLocal(IntrusiveQueue<TaskBase, &TaskBase::next> &&tasks) {
  pending_queue_.Prepend(std::move(tasks));
}

inline void ThreadState::SetStealing() {
  constexpr std::uint32_t kDiff = 1U - (1U << 16U);
  pool_->num_active_.fetch_add(kDiff, std::memory_order_relaxed);
}

inline void ThreadState::ClearStealing() {
  constexpr std::uint32_t kDiff = 1U - (1U << 16U);
  const std::uint32_t num_active = pool_->num_active_.fetch_sub(kDiff, std::memory_order_relaxed);
  const std::uint32_t num_victims = num_active >> 16U;
  const std::uint32_t num_thiefs = num_active & 0xffffU;
  if (num_thiefs == 1 && num_victims != 0) {
    NotifyOneSleeping();
  }
}

inline void ThreadState::SetSleeping() { pool_->num_active_.fetch_sub(1U << 16U, std::memory_order_relaxed); }

inline void ThreadState::ClearSleeping() {
  const std::uint32_t num_active = pool_->num_active_.fetch_add(1U << 16U, std::memory_order_relaxed);
  if (num_active == 0) {
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

inline TurboPool::TurboPool(uint32_t thread_count, BWOSParams params)
    : remotes_(thread_count), thread_count_(thread_count), params_(params), thread_states_(thread_count) {
  assert(thread_count > 0);
  for (uint32_t index = 0; index < thread_count; ++index) {
    thread_states_[index].emplace(this, index, params);
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

inline void TurboPool::Enqueue(TaskBase *task) noexcept { Enqueue(*GetRemoteQueue(), task); }

inline size_t TurboPool::NumThreads() const noexcept { return thread_count_; }

inline void TurboPool::Enqueue(RemoteQueue &queue, TaskBase *task) noexcept {
  thread_local std::thread::id this_id = std::this_thread::get_id();
  RemoteQueue *correct_queue = this_id == queue.id_ ? &queue : GetRemoteQueue();
  size_t idx = correct_queue->index_;
  if (idx < thread_states_.size()) {
    thread_states_[idx]->PushLocal(task);
    return;
  }
  std::uniform_int_distribution<std::uint32_t> dist(0, static_cast<std::uint32_t>(thread_count_ - 1));
  thread_local Xorshift rng(std::random_device{}());
  const uint32_t thread_index = dist(rng);
  queue.queues_[thread_index].PushFront(task);
  thread_states_[thread_index]->Notify();
}

inline void TurboPool::Enqueue(RemoteQueue &queue, TaskBase *task, size_t thread_index) noexcept {
  queue.queues_[thread_index].PushFront(task);
  thread_states_[thread_index]->Notify();
}

inline void TurboPool::Enqueue(RemoteQueue &queue, IntrusiveQueue<TaskBase, &TaskBase::next> tasks) noexcept {
  thread_local std::thread::id this_id = std::this_thread::get_id();
  RemoteQueue *correct_queue = this_id == queue.id_ ? &queue : GetRemoteQueue();
  size_t idx = correct_queue->index_;
  if (idx < thread_states_.size()) {
    thread_states_[idx]->PushLocal(std::move(tasks));
    return;
  }
  std::uniform_int_distribution<std::uint32_t> dist(0, static_cast<std::uint32_t>(thread_count_ - 1));
  thread_local Xorshift rng(std::random_device{}());
  const uint32_t thread_index = dist(rng);
  queue.queues_[thread_index].Prepend(std::move(tasks));
  thread_states_[thread_index]->Notify();
}

inline void TurboPool::Enqueue(RemoteQueue &queue, IntrusiveQueue<TaskBase, &TaskBase::next> tasks,
                               size_t thread_index) noexcept {
  queue.queues_[thread_index].Prepend(std::move(tasks));
  thread_states_[thread_index]->Notify();
}

class SyncTask : public TaskBase {
 public:
  explicit SyncTask(std::function<void()> func) : func_(std::move(func)) {}

  void BuildExecute(std::function<void()> call_back) {
    call_back_ = std::move(call_back);
    execute = [](TaskBase *task, uint32_t /*tid*/) noexcept {
      auto *sync_task = static_cast<SyncTask *>(task);
      sync_task->func_();
      sync_task->call_back_();
    };
  }

 private:
  std::function<void()> func_;
  std::function<void()> call_back_;
};

template <class T>
struct AlignedAtomic {
  alignas(CACHE_LINE_SIZE) std::atomic<T> value;

  explicit AlignedAtomic(T v = 0) : value(v) {}

  AlignedAtomic(const AlignedAtomic &other) : value(other.value.load(std::memory_order_relaxed)) {}

  AlignedAtomic &operator=(const AlignedAtomic &other) {
    value.store(other.value.load(std::memory_order_relaxed), std::memory_order_relaxed);
    return *this;
  }
};

class SyncTaskScheduler {
 public:
  SyncTaskScheduler() = delete;
  explicit SyncTaskScheduler(TurboPool *pool) : pool_(pool), done_(false) {}

  void Enqueue(std::function<void()> func) noexcept { tasks_.emplace_back(std::move(func)); }

  void Run() {
    if (tasks_.empty()) {
      return;
    }
    auto *queue = pool_->GetRemoteQueue();
    auto batch_size = tasks_.size() / queue->queues_.size();
    auto remainder = tasks_.size() % queue->queues_.size();
    if (0 == batch_size) {
      task_counters_.resize(remainder);
      batch_task_counter_ = AlignedAtomic{static_cast<uint32_t>(remainder)};
    } else {
      tmp_queues_.resize(queue->queues_.size());
      task_counters_.resize(queue->queues_.size(), AlignedAtomic{static_cast<uint32_t>(batch_size)});
      batch_task_counter_ = AlignedAtomic{static_cast<uint32_t>(queue->queues_.size())};
    }
    for (size_t i = 0; i < tmp_queues_.size(); i++) {
      for (size_t j = 0; j < batch_size; j++) {
        auto task_index = (i * batch_size) + j;
        auto &task = tasks_[task_index];
        task.BuildExecute([this, i]() noexcept {
          if (1 == task_counters_[i].value.fetch_sub(1, std::memory_order_acq_rel)) {
            if (1 == batch_task_counter_.value.fetch_sub(1, std::memory_order_acq_rel)) {
              std::lock_guard lock{mut_};
              done_ = true;
              cv_.notify_one();
            }
          }
        });
        tmp_queues_[i].PushBack(&task);
      }
    }
    for (size_t i = 0; i < remainder; i++) {
      auto task_index = (batch_size * queue->queues_.size()) + i;
      auto &task = tasks_[task_index];
      task_counters_[i].value.fetch_add(1, std::memory_order_acq_rel);
      task.BuildExecute([this, i]() noexcept {
        if (1 == task_counters_[i].value.fetch_sub(1, std::memory_order_acq_rel)) {
          if (1 == batch_task_counter_.value.fetch_sub(1, std::memory_order_acq_rel)) {
            std::lock_guard lock{mut_};
            done_ = true;
            cv_.notify_one();
          }
        }
      });
    }
    for (size_t i = 0; i < tmp_queues_.size(); i++) {
      pool_->Enqueue(*queue, std::move(tmp_queues_[i]), i);
    }
    for (size_t i = (batch_size * queue->queues_.size()); i < tasks_.size(); i++) {
      pool_->Enqueue(*queue, &tasks_[i]);
    }
    {
      std::unique_lock lock{mut_};
      cv_.wait(lock, [this] { return done_; });
    }
    tasks_.clear();
    tmp_queues_.clear();
    task_counters_.clear();
    done_ = false;
  }

 private:
  TurboPool *pool_;
  std::vector<SyncTask> tasks_;
  std::vector<IntrusiveQueue<TaskBase, &TaskBase::next>> tmp_queues_;
  std::vector<AlignedAtomic<uint32_t>> task_counters_;
  AlignedAtomic<uint32_t> batch_task_counter_;
  std::mutex mut_;
  std::condition_variable cv_;
  bool done_;
};

}  // namespace turbo_pool