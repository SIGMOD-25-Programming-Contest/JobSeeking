#ifndef MY_SCHEDULER_H_
#define MY_SCHEDULER_H_

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <utility>
#include <vector>

namespace job_seeking {

struct Job {
  Job() : done{false} {}

  void operator()() {
    assert(done.load(std::memory_order_relaxed) == false);
    execute();
    done.store(true, std::memory_order_release);
  }

  bool finished() const noexcept {
    return done.load(std::memory_order_acquire);
  }

 protected:
  virtual void execute() = 0;
  std::atomic<bool> done;
};

template <typename F>
struct JobImpl : Job {
  explicit JobImpl(F& _f) : Job(), f(_f) {}
  void execute() override { f(); }

 private:
  F& f;
};

template <typename F>
JobImpl<F> make_job(F& f) {
  return JobImpl(f);
}

// Deque from Arora, Blumofe, and Plaxton (SPAA, 1998).
//
// Supports:
//
// push_bottom:   Only the owning thread may call this
// pop_bottom:    Only the owning thread may call this
// pop_top:       Non-owning threads may call this
//
struct Deque {
  using qidx = unsigned int;
  using tag_t = unsigned int;

  // use std::atomic<age_t> for atomic access.
  // Note: Explicit alignment specifier required
  // to ensure that Clang inlines atomic loads.
  struct alignas(int64_t) age_t {
    // cppcheck bug prevents it from seeing usage with braced initializer
    tag_t tag;  // cppcheck-suppress unusedStructMember
    qidx top;   // cppcheck-suppress unusedStructMember
  };

  // align to avoid false sharing
  struct alignas(64) padded_job {
    std::atomic<Job*> job;
  };

  static constexpr int q_size = 1000;
  std::atomic<qidx> bot;
  std::atomic<age_t> age;
  std::array<padded_job, q_size> deq;

  Deque() : bot(0), age(age_t{0, 0}) {}

  // Adds a new job to the bottom of the queue. Only the owning
  // thread can push new items. This must not be called by any
  // other thread.
  //
  // Returns true if the queue was empty before this push
  bool push_bottom(Job* job) {
    auto local_bot = bot.load(std::memory_order_acquire);      // atomic load
    deq[local_bot].job.store(job, std::memory_order_release);  // shared store
    local_bot += 1;
    if (local_bot == q_size) {
      std::cerr << "internal error: scheduler queue overflow\n";
      std::abort();
    }
    bot.store(local_bot, std::memory_order_seq_cst);  // shared store
    return (local_bot == 1);
  }

  // Pop an item from the top of the queue, i.e., the end that is not
  // pushed onto. Threads other than the owner can use this function.
  //
  // Returns {job, empty}, where empty is true if job was the
  // only job on the queue, i.e., the queue is now empty
  std::pair<Job*, bool> pop_top() {
    auto old_age = age.load(std::memory_order_acquire);    // atomic load
    auto local_bot = bot.load(std::memory_order_acquire);  // atomic load
    if (local_bot > old_age.top) {
      auto job =
          deq[old_age.top].job.load(std::memory_order_acquire);  // atomic load
      auto new_age = old_age;
      new_age.top = new_age.top + 1;
      if (age.compare_exchange_strong(old_age, new_age))
        return {job, (local_bot == old_age.top + 1)};
      else return {nullptr, (local_bot == old_age.top + 1)};
    }
    return {nullptr, true};
  }

  // Pop an item from the bottom of the queue. Only the owning
  // thread can pop from this end. This must not be called by any
  // other thread.
  Job* pop_bottom() {
    Job* result = nullptr;
    auto local_bot = bot.load(std::memory_order_acquire);  // atomic load
    if (local_bot != 0) {
      local_bot--;
      bot.store(local_bot, std::memory_order_release);  // shared store
      std::atomic_thread_fence(std::memory_order_seq_cst);
      auto job =
          deq[local_bot].job.load(std::memory_order_acquire);  // atomic load
      auto old_age = age.load(std::memory_order_acquire);      // atomic load
      if (local_bot > old_age.top) result = job;
      else {
        bot.store(0, std::memory_order_release);  // shared store
        auto new_age = age_t{old_age.tag + 1, 0};
        if ((local_bot == old_age.top) &&
            age.compare_exchange_strong(old_age, new_age))
          result = job;
        else {
          age.store(new_age, std::memory_order_seq_cst);  // shared store
          result = nullptr;
        }
      }
    }
    return result;
  }
};

#define STEAL_TIMEOUT 10000
#define YIELD_FACTOR 200
#define SLEEP_FACTOR 100

class work_stealing_scheduler {
 private:
  uint32_t num_threads;
  std::atomic<bool> finish;
  std::vector<Deque> deques;
  std::vector<__uint128_t> attempts;
  std::vector<std::thread> threads_pool;
  static inline thread_local uint32_t work_id;

 public:
  work_stealing_scheduler(uint32_t n)
      : num_threads(n),
        finish(false),
        deques(num_threads),
        attempts(num_threads) {
    for (uint32_t i = 1; i < num_threads; i++) {
      threads_pool.emplace_back([&, i]() {
        work_id = i;
        worker_loop();
      });
    }
    work_id = 0;
  };

  void clear() {
    finish.store(true, std::memory_order_release);
    for (uint32_t i = 1; i < num_threads; i++) {
      threads_pool[i - 1].join();
    }
  }

  uint32_t num_workers() { return num_threads; }

  void spawn(Job* job) { deques[work_id].push_bottom(job); }

  template <typename F>
  Job* get_job(F&& done, bool time_out = true) {
    Job* job = deques[work_id].pop_bottom();
    if (job) {
      return job;
    }
    job = steal_job(std::forward<F>(done), time_out);
    return job;
  }

  bool finished() { return finish.load(std::memory_order_acquire); }

  template <typename F>
  void wait_until(F&& done) {
    while (!done()) {
      Job* job = get_job(std::forward<F>(done), false);
      if (job) (*job)();
    }
  }

 private:
  void worker_loop() {
    while (!finished()) {
      Job* job = get_job([&]() { return finished(); });
      if (job) (*job)();
    }
  }

  size_t hash(uint64_t x) {
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    x = x ^ (x >> 31);
    return static_cast<size_t>(x);
  }

  template <typename F>
  Job* steal_job(F&& done, bool time_out) {
    uint32_t id = work_id;
    const auto start_time = std::chrono::steady_clock::now();
    while (!time_out || std::chrono::duration_cast<std::chrono::nanoseconds>(
                            (std::chrono::steady_clock::now() - start_time))
                                .count() < STEAL_TIMEOUT) {
      for (size_t i = 0; i <= YIELD_FACTOR * num_threads; i++) {
        if (done()) {
          return nullptr;
        }
        uint32_t target_id = (hash(id) + (hash(attempts[id]))) % num_threads;
        attempts[id]++;
        auto [job, empty] = deques[target_id].pop_top();
        if (job) {
          return job;
        }
      }
      std::this_thread::sleep_for(
          std::chrono::nanoseconds(num_threads * SLEEP_FACTOR));
    }
    return nullptr;
  }
};

inline unsigned int init_num_workers() {
  if (const auto env_p = std::getenv("PARLAY_NUM_THREADS")) {
    return std::stoi(env_p);
  } else {
#if defined(SPC__CORE_COUNT) && defined(SPC__THREAD_COUNT)
    if (SPC__THREAD_COUNT == SPC__CORE_COUNT) {
      return SPC__CORE_COUNT;
    } else if (SPC__THREAD_COUNT == SPC__CORE_COUNT * 2) {
      return SPC__CORE_COUNT * 2;
    } else {
      return SPC__CORE_COUNT;
    }
#else
    return std::thread::hardware_concurrency();
#endif
  }
}

alignas(work_stealing_scheduler) char scheduler_buf[sizeof(
    work_stealing_scheduler)];

work_stealing_scheduler* my_scheduler;

class fork_join_scheduler {
 public:
  fork_join_scheduler() = default;

  int num_workers() { return my_scheduler->num_workers(); }

  template <typename L, typename R>
  static void pardo(L&& left, R&& right) {
    auto execute_right = [&]() { std::forward<R>(right)(); };
    auto right_job = make_job<R>(right);
    my_scheduler->spawn(&right_job);
    std::forward<L>(left)();
    my_scheduler->wait_until([&]() { return right_job.finished(); });
    assert(right_job.finished());
  }

  template <typename F>
  static void parfor(size_t start, size_t end, F&& f, size_t granularity = 0) {
    if (end == start) {
      return;
    }
    if (granularity == 0) {
      size_t sample_g = get_granularity(start, end, std::forward<F>(f));
      granularity = std::max(
          sample_g, (end - start) / (my_scheduler->num_workers() * 128));
      start += sample_g;
    }
    _parfor(start, end, std::forward<F>(f), granularity);
  }

 private:
  template <typename F>
  static void _parfor(size_t start, size_t end, F&& f, size_t granularity) {
    if (end - start <= granularity) {
      for (auto i = start; i < end; i++) {
        f(i);
      }
    } else {
      size_t n = end - start;
      size_t mid = (start + (9 * (n + 1)) / 16);
      pardo([&]() { _parfor(start, mid, f, granularity); },
            [&]() { _parfor(mid, end, f, granularity); });
    }
  }

  template <typename F>
  static size_t get_granularity(size_t start, size_t end, F&& f) {
    size_t done = 0;
    size_t sz = 1;
    unsigned long long int ticks = 0;
    while (ticks < 1000 && done < (end - start)) {
      sz = std::min(sz, end - (start + done));
      auto t_start = std::chrono::steady_clock::now();
      for (size_t i = 0; i < sz; i++) f(start + done + i);
      auto t_stop = std::chrono::steady_clock::now();
      ticks = std::chrono::duration_cast<std::chrono::nanoseconds>(
                  (t_stop - t_start))
                  .count();
      done += sz;
      sz = sz * 2;
    }
    return done;
  }
};

fork_join_scheduler job_seeking_scheduler;

};  // namespace job_seeking

#endif  // MY_SCHEDULER_H_
