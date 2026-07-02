#pragma once

#include "taskforge/Task.h"
#include "taskforge/ThreadPool.h"
#include "taskforge/Logger.h"

#include <memory>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <string>

// DAG-aware scheduler that sits on top of ThreadPool.
// You register tasks with their dependencies, call run(), and it handles
// the rest — tasks get submitted to the pool automatically as their deps complete.
//
// Two mutexes on purpose (see waitAll() comments in .cpp for the deadlock story):
//   graphMutex_  — protects all DAG state
//   allDoneMutex_ — only used by waitAll()'s condition_variable
class TaskScheduler {
public:
    explicit TaskScheduler(std::size_t numThreads);
    ~TaskScheduler();

    // Register a task and its dependencies. All depIds must already be registered.
    // Returns false (and skips the task) if adding it would introduce a cycle.
    // NOTE: not thread-safe with itself — call all addTask()s before run().
    bool addTask(std::shared_ptr<Task>        task,
                 const std::vector<Task::Id>& depIds = {});

    // Submit all tasks with zero pending deps to the pool.
    // The rest get released automatically via the completion callback.
    void run();

    // Block until every registered task reaches COMPLETED or FAILED.
    void waitAll();

    std::size_t totalTasks()     const;
    std::size_t completedTasks() const;
    std::size_t failedTasks()    const;

private:
    // DFS cycle detection — WHITE/GRAY/BLACK coloring
    enum class DfsColor { WHITE, GRAY, BLACK };
    bool hasCycle(Task::Id                                      startId,
                  const std::unordered_map<Task::Id, DfsColor>& colors) const;

    // Called from a worker thread when a task finishes.
    // Decrements dep counts for dependents and submits any that hit zero.
    void onTaskComplete(Task::Id id);

    // Wrap task in a lambda that calls onTaskComplete() on finish, then submit.
    // Must be called with graphMutex_ held — releases it around the actual submit
    // to avoid a deadlock (see implementation for details).
    void submitToPool(std::shared_ptr<Task> task);

    ThreadPool pool_;

    mutable std::mutex graphMutex_;

    std::unordered_map<Task::Id, std::shared_ptr<Task>>          tasks_;
    std::unordered_map<Task::Id, std::unordered_set<Task::Id>>   dependsOn_;
    std::unordered_map<Task::Id, std::vector<Task::Id>>          dependents_;
    std::unordered_map<Task::Id, int>                            pendingDepCount_;

    std::size_t              finishedCount_{ 0 };   // under graphMutex_
    std::atomic<std::size_t> doneCount_{ 0 };       // atomic mirror — lets waitAll() read without graphMutex_
    std::atomic<std::size_t> totalCount_{ 0 };

    std::mutex              allDoneMutex_;
    std::condition_variable allDoneCv_;
};
