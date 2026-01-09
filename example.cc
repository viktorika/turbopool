#include <atomic>
#include <memory>
#include <thread>
#include <vector>
#include "benchmark/benchmark.h"
#include "include/turbo_pool.h"

// ----------------------------------------------------------------------------
// Global Resources
// ----------------------------------------------------------------------------

// 使用单例模式或静态对象来保持线程池，避免在每次 benchmark 迭代中创建/销毁
// 除非我们专门想测试创建/销毁的开销。这里我们关注任务执行性能。
static std::unique_ptr<turbo_pool::TurboPool> g_turbo_pool;

// 初始化资源的辅助函数
void SetupResources() {
  static std::once_flag flag;
  std::call_once(flag, []() {
    int num_threads = std::thread::hardware_concurrency();
    g_turbo_pool = std::make_unique<turbo_pool::TurboPool>(num_threads);
  });
}

// 模拟较重的计算任务 (约 5000 次浮点乘加运算)
void SimulateHeavyTask() {
  double result = 0.0;
  for (int i = 0; i < 5000; ++i) {
    result += i * 3.14159;
  }
  benchmark::DoNotOptimize(result);
}

// ----------------------------------------------------------------------------
// Benchmark: Folly CPUThreadPoolExecutor
// ----------------------------------------------------------------------------

class CountDownLatch {
 public:
  explicit CountDownLatch(int count) : count_(count) {}

  void count_down() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (--count_ == 0) {
      cv_.notify_all();
    }
  }

  void wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return count_ == 0; });
  }

 private:
  int count_;
  std::mutex mutex_;
  std::condition_variable cv_;
};

// ----------------------------------------------------------------------------
// Benchmark: TurboPool with SyncTaskScheduler
// ----------------------------------------------------------------------------

static void BM_TurboPool_SyncScheduler(benchmark::State& state) {
  SetupResources();

  int num_tasks = state.range(0);
  turbo_pool::SyncTaskScheduler scheduler(g_turbo_pool.get());
  for (auto _ : state) {
    for (int i = 0; i < num_tasks; ++i) {
      scheduler.Enqueue([]() { benchmark::DoNotOptimize(1); });
    }

    // Run 会批量提交并等待完成
    scheduler.Run();
  }

  state.SetItemsProcessed(state.iterations() * num_tasks);
}

static void BM_TurboPool_SyncScheduler_Heavy(benchmark::State& state) {
  SetupResources();

  int num_tasks = state.range(0);
  turbo_pool::SyncTaskScheduler scheduler(g_turbo_pool.get());
  for (auto _ : state) {
    for (int i = 0; i < num_tasks; ++i) {
      scheduler.Enqueue([]() { SimulateHeavyTask(); });
    }

    scheduler.Run();
  }

  state.SetItemsProcessed(state.iterations() * num_tasks);
}

// ----------------------------------------------------------------------------
// Benchmark Configuration
// ----------------------------------------------------------------------------

// 测试不同量级的任务数: 100, 1k, 10k, 100k
BENCHMARK(BM_TurboPool_SyncScheduler)->Range(100, 100000)->UseRealTime();

// 多线程并发提交测试 (模拟 4 个并发线程向同一个池子提交任务)
BENCHMARK(BM_TurboPool_SyncScheduler)->Range(100, 10000)->Threads(4)->UseRealTime();

// 重任务测试
BENCHMARK(BM_TurboPool_SyncScheduler_Heavy)->Range(100, 1000)->UseRealTime();

// 多线程并发提交重任务
BENCHMARK(BM_TurboPool_SyncScheduler_Heavy)->Range(100, 500)->Threads(4)->UseRealTime();

BENCHMARK_MAIN();