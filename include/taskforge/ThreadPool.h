#pragma once

#include "taskforge/Task.h"
#include "taskforge/Logger.h"

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <atomic>
#include <memory>

// Comparator for the priority queue — we store shared_ptr<Task> so we need
// to dereference before comparing (comparing raw pointers = sorting by address, useless)
struct TaskPtrComparator {
    bool operator()(const std::shared_ptr<Task>& a,
                    const std::shared_ptr<Task>& b) const {
        return a->getPriority() < b->getPriority();
    }
};

// Fixed-size thread pool with a priority queue (max-heap).
// Workers sleep on a condition_variable and wake up when there's work or a shutdown signal.
//
// Lifecycle: construct → submit tasks → destructor joins all threads automatically.
class ThreadPool {
public:
    explicit ThreadPool(std::size_t numThreads);
    ~ThreadPool();

    ThreadPool(const ThreadPool&)            = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&)                 = delete;
    ThreadPool& operator=(ThreadPool&&)      = delete;

    // Enqueue a task. Wakes one sleeping worker. Throws if called after shutdown.
    void submit(std::shared_ptr<Task> task);

    // Signal workers to stop. Idempotent — safe to call more than once.
    void shutdown();

    std::size_t threadCount()  const { return workers_.size(); }
    std::size_t pendingCount() const;
    bool        isShutdown()   const { return shutdown_.load(); }

private:
    void workerLoop();

    // Using shared_ptr<Task> so TaskScheduler can still read task status after execution
    std::priority_queue<
        std::shared_ptr<Task>,
        std::vector<std::shared_ptr<Task>>,
        TaskPtrComparator
    > taskQueue_;

    mutable std::mutex      mutex_;
    std::condition_variable cv_;
    std::atomic<bool>       shutdown_{ false };
    std::vector<std::thread> workers_;
};
