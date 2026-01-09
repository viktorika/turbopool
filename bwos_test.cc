#include "include/bwos_lifo_queue.hpp"
#include <thread>
#include <vector>
#include <atomic>
#include <iostream>
#include <cassert>
#include <chrono>
#include <cstdint>

using namespace turbo_pool;

// Configuration
const int NUM_ITEMS = 50000000;
const int NUM_THIEVES = 16;
const int BLOCK_SIZE = 256;
const int NUM_BLOCKS = 64; // Ensure enough capacity: 64 * 256 = 16384. 
                           // Since we push 1M items, the queue will definitely fill up and wrap around/block if not consumed fast enough.

std::atomic<int> consumed_count{0};
std::vector<std::atomic<bool>> visited(NUM_ITEMS + 1);
std::atomic<bool> owner_finished{false};

void check_item(int* item_ptr) {
    int item = static_cast<int>(reinterpret_cast<uintptr_t>(item_ptr));
    if (item <= 0 || item > NUM_ITEMS) {
        std::cerr << "Error: Item out of range: " << item << std::endl;
        std::terminate();
    }
    bool expected = false;
    if (!visited[item].compare_exchange_strong(expected, true)) {
        std::cerr << "Error: Duplicate item: " << item << std::endl;
        std::terminate();
    }
    int current = consumed_count.fetch_add(1, std::memory_order_relaxed);
    if (current % 5000000 == 0 && current > 0) {
        std::cout << "Processed " << current << " items..." << std::endl;
    }
}

void owner_thread(BWOSLifoQueue<int*>& queue) {
    for (int i = 1; i <= NUM_ITEMS; ++i) {
        int* val_ptr = reinterpret_cast<int*>(static_cast<uintptr_t>(i));
        // Try to push
        while (!queue.PushBack(val_ptr)) {
            // Queue is full, try to consume some items to make space
            int* val = queue.PopBack();
            if (val != nullptr) {
                check_item(val);
            } else {
                // If we can't push and can't pop, we must yield and wait for thieves
                std::this_thread::yield();
            }
        }
        
        // Simulate some work and occasional self-consumption
        if (i % 4 == 0) {
             int* val = queue.PopBack();
             if (val != nullptr) {
                 check_item(val);
             }
        }
    }
    owner_finished = true;
    
    // Drain remaining items
    while (consumed_count < NUM_ITEMS) {
        int* val = queue.PopBack();
        if (val != nullptr) {
            check_item(val);
        } else {
             std::this_thread::yield();
        }
    }
}

void thief_thread(BWOSLifoQueue<int*>& queue) {
    while (consumed_count < NUM_ITEMS) {
        int* val = queue.StealFront();
        if (val != nullptr) {
            check_item(val);
        } else {
            std::this_thread::yield();
        }
    }
}

int main() {
    // Initialize visited array
    for (int i = 0; i <= NUM_ITEMS; ++i) {
        visited[i] = false;
    }

    std::cout << "Initializing BWOSLifoQueue with " << NUM_BLOCKS << " blocks of size " << BLOCK_SIZE << std::endl;
    BWOSLifoQueue<int*> queue(NUM_BLOCKS, BLOCK_SIZE);

    std::cout << "Starting Owner Thread..." << std::endl;
    std::thread owner(owner_thread, std::ref(queue));

    std::cout << "Starting " << NUM_THIEVES << " Thief Threads..." << std::endl;
    std::vector<std::thread> thieves;
    for (int i = 0; i < NUM_THIEVES; ++i) {
        thieves.emplace_back(thief_thread, std::ref(queue));
    }

    auto start = std::chrono::high_resolution_clock::now();

    owner.join();
    for (auto& t : thieves) {
        t.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;

    std::cout << "Finished processing " << NUM_ITEMS << " items." << std::endl;
    std::cout << "Time elapsed: " << elapsed.count() << " seconds." << std::endl;
    std::cout << "Throughput: " << NUM_ITEMS / elapsed.count() / 1000000.0 << " M ops/sec" << std::endl;

    // Verification
    int missing = 0;
    for (int i = 1; i <= NUM_ITEMS; ++i) {
        if (!visited[i]) {
            missing++;
            if (missing < 10) {
                std::cout << "Missing item: " << i << std::endl;
            }
        }
    }
    
    if (missing > 0) {
        std::cout << "Total missing items: " << missing << std::endl;
        return 1;
    }

    std::cout << "Verification Successful! All items consumed exactly once." << std::endl;
    return 0;
}
