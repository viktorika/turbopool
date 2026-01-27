/*
 * @Author: victorika 
 * @Date: 2025-12-04 19:45:58 
 * @Last Modified by:   victorika 
 * @Last Modified time: 2025-12-04 19:45:58 
 */
#pragma once

#if defined(_MSC_VER)
#  include <intrin.h>  // MSVC
#elif defined(__GNUC__) || defined(__clang__)
#  if defined(__i386__) || defined(__x86_64__)
#    include <immintrin.h>  // GCC/Clang x86
#  endif
// ARM 通常使用内联汇编，不需要特定头文件，或者使用 <arm_acle.h>
#endif

namespace turbo_pool {

/**
 * @brief CPU 级别的忙等待提示指令
 *
 * 作用：
 * 1. 告诉 CPU 当前处于自旋循环中，避免流水线过度推测执行。
 * 2. 在超线程（Hyper-Threading）架构上，让出流水线资源给同核心的另一个线程。
 * 3. 稍微降低 CPU 功耗。
 * 4. 防止内存重排导致的退出循环延迟（Memory Order Violation）。
 */
inline void SpinLoopPause() {
#if defined(_MSC_VER)
// --------------------------------------------------
// Microsoft Visual C++ (Windows)
// --------------------------------------------------
#  if defined(_M_IX86) || defined(_M_X64)
  // x86 / x64 架构
  _mm_pause();
#  elif defined(_M_ARM) || defined(_M_ARM64)
  // ARM / ARM64 架构
  __yield();
#  else
  // 其他架构 fallback
#  endif

#elif defined(__GNUC__) || defined(__clang__)
// --------------------------------------------------
// GCC / Clang (Linux, macOS, Android, iOS)
// --------------------------------------------------
#  if defined(__i386__) || defined(__x86_64__)
  // x86 / x64 架构
  // _mm_pause() 是最标准的写法，底层对应 "rep; nop" 或 "pause" 指令
  _mm_pause();
  // 或者使用内联汇编: __asm__ __volatile__("pause");

#  elif defined(__aarch64__) || defined(__arm__)
  // ARM / ARM64 架构
  // "yield" 是 ARM 的标准提示指令
  __asm__ __volatile__("yield" ::: "memory");

#  elif defined(__powerpc__) || defined(__ppc__) || defined(__PPC__)
  // PowerPC 架构
  __asm__ __volatile__("or 27,27,27" ::: "memory");

#  elif defined(__riscv)
  // RISC-V 架构 (Zihintpause 扩展)
  // .insn 是为了兼容旧汇编器，对应 pause 指令
  __asm__ __volatile__(".insn i 0x0F, 0, x0, x0, 0x010" ::: "memory");
#  else
  // 未知架构：做一个编译器屏障，防止编译器优化掉空循环
  __asm__ __volatile__("" ::: "memory");
#  endif

#else
  // --------------------------------------------------
  // 其他编译器
  // --------------------------------------------------
  // 尽力而为，防止空循环被优化消失
#endif
}

}  // namespace turbo_pool