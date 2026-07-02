#include "taskforge/ThreadPool.h"
#include <stdexcept>

ThreadPool::ThreadPool(std::size_t numThreads) {
    if(numThreads == 0) {
        throw std::invalid_argument("ThreadPool: numThreads must be >= 1");
    }

    LOG_TAG("Pool", "Starting thread pool with " +
            std::to_string(numThreads) + " worker threads");

    // reserve() first — emplace_back can't reallocate while threads are running
    // (std::thread is move-only and can't be moved once it's started)
    workers_.reserve(numThreads);
    for(std::size_t i = 0; i < numThreads; ++i) {
        workers_.emplace_back(&ThreadPool::workerLoop, this);
    }
}

// Classic beginner mistake: forgetting to join threads before the object is destroyed.
// If the destructor exits without joining, the OS tears down thread stacks mid-execution → crash/UB.
ThreadPool::~ThreadPool() {
    shutdown();
    for(auto& w : workers_) {
        if(w.joinable()) w.join();
    }
    LOG_TAG("Pool", "All worker threads joined. Pool destroyed cleanly.");
}

void ThreadPool::submit(std::shared_ptr<Task> task) {
    if(!task) {
        throw std::invalid_argument("ThreadPool::submit: task cannot be null");
    }
    if(shutdown_.load()) {
        throw std::runtime_error("ThreadPool::submit: cannot submit to a shut-down pool");
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        taskQueue_.push(task);
    }
    // notify_one after releasing the lock — otherwise the woken worker immediately
    // blocks trying to reacquire it
    cv_.notify_one();
}

void ThreadPool::shutdown() {
    // exchange() returns old value; if already true, someone else already shut down
    if(shutdown_.exchange(true)) return;
    LOG_TAG("Pool", "Shutdown requested — waking all workers");
    cv_.notify_all();
}

std::size_t ThreadPool::pendingCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return taskQueue_.size();
}

void ThreadPool::workerLoop() {
    std::ostringstream oss;
    oss << std::this_thread::get_id();
    const std::string tid = oss.str();
    LOG_TAG("Worker-" + tid.substr(tid.size() > 4 ? tid.size()-4 : 0),
            "Started, waiting for tasks");

    while(true) {
        std::shared_ptr<Task> task;

        {
            // unique_lock required here because cv_.wait() needs to unlock mid-scope —
            // lock_guard has no unlock(), so it can't be used with condition_variable
            std::unique_lock<std::mutex> lock(mutex_);

            // Wait until there's something to do, or we're shutting down.
            // The 'while' (via the lambda predicate) guards against spurious wakeups —
            // the OS can wake a waiting thread for no reason, and we need to re-check.
            cv_.wait(lock, [this] {
                return !taskQueue_.empty() || shutdown_.load();
            });

            if(taskQueue_.empty()) {
                // Only other reason to wake up is shutdown
                break;
            }

            task = taskQueue_.top();
            taskQueue_.pop();
        } // lock released here

        // Execute OUTSIDE the lock — if we held it during execution, workers
        // would run serially (only one at a time), which defeats the whole point
        task->execute();
    }

    LOG_TAG("Pool", "Worker thread exiting cleanly");
}
