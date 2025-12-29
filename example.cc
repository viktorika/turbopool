#include <iostream>
#include "turbo_pool.h"

int main() {
  std::atomic<uint32_t> counter;
  turbo_pool::TurboPool pool;
  // recommend to use thread_local scheduler to avoid repeated construction/destruction
  thread_local static turbo_pool::SyncTaskScheduler scheduler(&pool);
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