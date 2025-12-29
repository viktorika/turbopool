/*
 * @Author: victorika
 * @Date: 2025-12-09 11:01:24
 * @Last Modified by: victorika
 * @Last Modified time: 2025-12-09 11:03:06
 */
#pragma once

namespace turbo_pool {

#if defined(__GNUC__) || defined(__clang__)
#  define likely(x) __builtin_expect(!!(x), 1)
#  define unlikely(x) __builtin_expect(!!(x), 0)
#else
#  define likely(x) (x)
#  define unlikely(x) (x)
#endif

#ifdef __cpp_lib_hardware_interference_size
#  define CACHE_LINE_SIZE std::hardware_destructive_interference_size
#else
#  define CACHE_LINE_SIZE 64
#endif

}  // namespace turbo_pool
