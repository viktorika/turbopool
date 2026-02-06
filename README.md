# TurboPool
High-performance CPU-bound thread pool

# Introduce
This is a high-performance, CPU-bound thread pool implemented only in a header file. It requires C++17 or later to run and has no other dependencies. An example of its usage is as follows:

```c++
#include <iostream>
#include "include/turbo_pool.h"

int main() {
  std::atomic<uint32_t> counter;
  turbo_pool::TurboPool pool;
  turbo_pool::SyncTaskScheduler scheduler(&pool);
  std::cout << "pool_size=" << pool.NumThreads() << "\n";
  auto loop_cnt = 24000;
  counter.store(loop_cnt);
  for (int i = 0; i < loop_cnt; i++) {
    scheduler.Enqueue([&counter]() {
      std::cout << "do task\n";
      counter.fetch_sub(1);
    });
  }
  scheduler.Run();
  std::cout << "all tasks done\n";
  std::cout << "remaining: " << counter.load() << "\n";
  return 0;
}
```

# implementation

* Minimized granularity reduces contention, with each enqueue thread and each thread pool worker thread having its own dedicated queue.
* Supports task stealing by using Block-based Work Stealing queues to minimize contention caused by stealing.
* Uses intrusive queues to reduce discrete memory allocations.
* Uses a more lightweight Xorshift algorithm for generating random numbers.