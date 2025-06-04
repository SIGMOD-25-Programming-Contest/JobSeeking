#ifndef JOB_SEEKING_UTILS_H_
#define JOB_SEEKING_UTILS_H_

#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>

inline uint32_t my_hash(uint32_t u) {
  uint32_t v = u * 2654435761u;
  v ^= v >> 16;
  v *= 2246822519u;
  v ^= v >> 13;
  v *= 3266489909u;
  v ^= v >> 16;
  return v;
}

inline uint32_t my_hash(int32_t u) { return my_hash((uint32_t)u); }

inline uint32_t my_hash_2(uint32_t u) { return my_hash(~u); }

inline uint32_t my_hash_2(int32_t u) { return my_hash_2((uint32_t)u); }

inline uint64_t my_hash(uint64_t u) {
  uint64_t v = u * 3935559000370003845ul + 2691343689449507681ul;
  v ^= v >> 21;
  v ^= v << 37;
  v ^= v >> 4;
  v *= 4768777513237032717ul;
  v ^= v << 20;
  v ^= v >> 41;
  v ^= v << 5;
  return v;
}

inline uint64_t my_hash(void* u) { return my_hash((uint64_t)u); }

inline uint64_t my_hash_2(void* u) { return my_hash(~((uint64_t)u)); }

struct timer {
 private:
  using time_t = decltype(std::chrono::system_clock::now());
  double total_so_far;
  time_t last;
  bool on;
  std::string name;

  auto get_time() { return std::chrono::system_clock::now(); }

  void report(double time, std::string str) {
    std::ios::fmtflags cout_settings = std::cout.flags();
    std::cout.precision(4);
    std::cout << std::fixed;
    std::cout << name << ": ";
    if (str.length() > 0) std::cout << str << ": ";
    std::cout << time << std::endl;
    std::cout.flags(cout_settings);
  }

  double diff(time_t t1, time_t t2) {
    return std::chrono::duration_cast<std::chrono::microseconds>(t1 - t2)
               .count() /
           1000000.0;
  }

 public:
  timer(std::string name = "Time", bool start_ = true)
      : total_so_far(0.0), on(false), name(name) {
    if (start_) start();
  }

  timer(bool start_) : timer("Time", start_) {}

  void start() {
    on = true;
    last = get_time();
  }

  double stop() {
    on = false;
    double d = diff(get_time(), last);
    total_so_far += d;
    return d;
  }

  void reset() {
    total_so_far = 0.0;
    on = 0;
  }

  double next_time() {
    if (!on) return 0.0;
    time_t t = get_time();
    double td = diff(t, last);
    total_so_far += td;
    last = t;
    return td;
  }

  double total_time() {
    if (on) return total_so_far + diff(get_time(), last);
    else return total_so_far;
  }

  void next(std::string str) {
    if (on) report(next_time(), str);
  }

  void total() { report(total_time(), "total"); }
};

#endif  // JOB_SEEKING_UTILS_H_
