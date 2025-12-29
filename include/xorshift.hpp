/*
 * @Author: victorika
 * @Date: 2025-12-09 11:00:14
 * @Last Modified by:   victorika
 * @Last Modified time: 2025-12-09 11:00:14
 */
#pragma once

#include <cstdint>
#include <random>

namespace turbo_pool {

class Xorshift {
 public:
  using result_type = uint32_t;

  static constexpr result_type(min)() { return 0; }

  static constexpr result_type(max)() { return UINT32_MAX; }

  friend bool operator==(Xorshift const &a, Xorshift const &b) noexcept { return a.m_seed_ == b.m_seed_; }

  Xorshift() : m_seed_(0xc1f651c67c62c6e0ULL) {}

  explicit Xorshift(std::random_device &rd) { seed(rd); }

  explicit Xorshift(std::uint64_t seed) : m_seed_(seed) {}

  void seed(std::random_device &rd) { m_seed_ = static_cast<uint64_t>(rd()) << 31 | static_cast<uint64_t>(rd()); }

  result_type operator()() {
    std::uint64_t result = m_seed_ * 0xd989bcacc137dcd5ULL;
    m_seed_ ^= m_seed_ >> 11;
    m_seed_ ^= m_seed_ << 31;
    m_seed_ ^= m_seed_ >> 18;
    return static_cast<uint32_t>(result >> 32ULL);
  }

  void discard(uint64_t n) {
    for (uint64_t i = 0; i < n; ++i) {
      operator()();
    }
  }

 private:
  std::uint64_t m_seed_;
};

}  // namespace turbo_pool