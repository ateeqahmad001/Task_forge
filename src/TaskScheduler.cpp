#include "taskforge/TaskScheduler.h"
#include <stdexcept>
#include <sstream>

TaskScheduler::TaskScheduler(std::size_t numThreads)
    : pool_(numThreads)
{
    LOG_TAG("Scheduler", "Initialized with " +
            std::to_string(numThreads) + " worker threads");
}

TaskScheduler::~TaskScheduler() {
    waitAll();
}

bool TaskScheduler::addTask(std::shared_ptr<Task>        task,
                            const std::vector<Task::Id>& depIds)
{
    if(!task) {
        throw std::invalid_argument("TaskScheduler::addTask: task is null");
    }

    std::lock_guard<std::mutex> lock(graphMutex_);
    const Task::Id id = task->getId();

    if(tasks_.count(id)) {
        throw std::invalid_argument(
            "TaskScheduler::addTask: task ID " + std::to_string(id) + " already registered");
    }

    for(Task::Id dep : depIds) {
        if(!tasks_.count(dep)) {
            throw std::invalid_argument(
                "TaskScheduler::addTask: dependency ID " +
                std::to_string(dep) + " not found. Add dependencies first.");
        }
    }

    // Cycle detection: for each dep, DFS from that dep through dependents_ and
    // check if it can reach `id`. If yes, adding id→dep closes a cycle.
    // (id is brand-new so it can't have outgoing edges yet — only need to check deps)
    for(Task::Id dep : depIds) {
        std::unordered_map<Task::Id, DfsColor> colors;
        for(auto& [k, _] : tasks_) colors[k] = DfsColor::WHITE;
        colors[id] = DfsColor::WHITE;
        colors[dep] = DfsColor::GRAY;
        if(hasCycle(dep, colors)) {
            LOG_TAG("Scheduler",
                "CYCLE DETECTED: adding task '" + task->getName() +
                "' (ID=" + std::to_string(id) + ") with dep on ID=" +
                std::to_string(dep) + " would create a cycle. Task rejected.");
            return false;
        }
    }

    // Commit to the graph
    tasks_[id]           = task;
    dependsOn_[id]       = { depIds.begin(), depIds.end() };
    pendingDepCount_[id] = static_cast<int>(depIds.size());

    for(Task::Id dep : depIds) {
        dependents_[dep].push_back(id);
    }
    if(!dependents_.count(id)) dependents_[id] = {};

    LOG_TAG("Scheduler",
        "Registered task '" + task->getName() +
        "' ID=" + std::to_string(id) +
        " deps=[" + [&]{
            std::string s;
            for(Task::Id d : depIds) s += std::to_string(d) + ",";
            if(!s.empty()) s.pop_back();
            return s;
        }() + "]"
    );

    return true;
}

// DFS with WHITE/GRAY/BLACK coloring.
// GRAY = currently on the stack; hitting a GRAY node means we found a back-edge (cycle).
bool TaskScheduler::hasCycle(
    Task::Id                                      startId,
    const std::unordered_map<Task::Id, DfsColor>& colors) const
{
    const auto& deps = dependsOn_.at(startId);
    for(Task::Id dep : deps) {
        auto it = colors.find(dep);
        if(it == colors.end()) continue;
        if(it->second == DfsColor::GRAY)  return true;
        if(it->second == DfsColor::BLACK) continue;

        const_cast<std::unordered_map<Task::Id,DfsColor>&>(colors)[dep] = DfsColor::GRAY;
        if(hasCycle(dep, colors)) return true;
        const_cast<std::unordered_map<Task::Id,DfsColor>&>(colors)[dep] = DfsColor::BLACK;
    }
    return false;
}

// Seed the pool with all tasks that have no dependencies.
// Everything else gets submitted dynamically from onTaskComplete().
void TaskScheduler::run() {
    std::lock_guard<std::mutex> lock(graphMutex_);

    LOG_TAG("Scheduler", "run() called — submitting tasks with no dependencies");

    totalCount_.store(tasks_.size());

    int seeded = 0;
    for(auto& [id, task] : tasks_) {
        if(pendingDepCount_[id] == 0) {
            submitToPool(task);
            ++seeded;
        }
    }

    LOG_TAG("Scheduler", std::to_string(seeded) +
            " task(s) seeded into pool. Remainder waiting on dependencies.");
}

// Wraps the task in a lambda that calls onTaskComplete() after execution,
// then submits the wrapper to the pool.
//
// Has to release graphMutex_ before calling pool_.submit() — otherwise:
//   graphMutex_ held → pool_.submit() → worker wakes immediately →
//   onTaskComplete() tries to lock graphMutex_ → deadlock
//
// This is why submitToPool() manually unlocks/relocks instead of using lock_guard.
void TaskScheduler::submitToPool(std::shared_ptr<Task> task) {
    const Task::Id    id       = task->getId();
    const std::string name     = task->getName();
    const int         priority = task->getPriority();

    LOG_TAG("Scheduler", "Releasing to pool: '" + name + "' ID=" + std::to_string(id));

    auto wrapper = std::make_shared<Task>(
        name,
        [this, task]() {
            task->execute();
            onTaskComplete(task->getId());
        },
        priority
    );

    graphMutex_.unlock();
    pool_.submit(wrapper);
    graphMutex_.lock();
}

// Called from worker threads when a task finishes.
// Decrements pending dep counts for all dependents; submits any that hit zero.
// Then signals waitAll() if everything is done.
void TaskScheduler::onTaskComplete(Task::Id id) {
    const std::string statusStr = tasks_.count(id)
        ? statusToString(tasks_[id]->getStatus())
        : "UNKNOWN";

    {
        std::lock_guard<std::mutex> lock(graphMutex_);

        ++finishedCount_;
        const std::size_t total = tasks_.size();

        LOG_TAG("Scheduler",
            "Task ID=" + std::to_string(id) + " finished (" + statusStr + ")" +
            " [" + std::to_string(finishedCount_) + "/" + std::to_string(total) + " done]");

        for(Task::Id depId : dependents_[id]) {
            auto& cnt = pendingDepCount_[depId];
            --cnt;

            LOG_TAG("Scheduler",
                "  Dependent ID=" + std::to_string(depId) +
                " '" + tasks_[depId]->getName() + "'" +
                " now has " + std::to_string(cnt) + " pending dep(s)");

            if(cnt == 0) {
                LOG_TAG("Scheduler",
                    "  -> All deps satisfied for ID=" +
                    std::to_string(depId) + " — submitting now");
                submitToPool(tasks_[depId]);
            }
        }
    }
    // graphMutex_ fully released before touching allDoneMutex_ —
    // this is what prevents the AB/BA deadlock with waitAll().
    // If we notified while still holding graphMutex_, waitAll() could deadlock
    // trying to acquire it inside the predicate.
    doneCount_.fetch_add(1);
    allDoneCv_.notify_all();
}

// Block until doneCount_ >= totalCount_.
//
// The two-mutex design exists because of a deadlock I ran into early on:
//   waitAll() held allDoneMutex_ and tried to read finishedCount_ (under graphMutex_)
//   onTaskComplete() held graphMutex_ and tried to notify (under allDoneMutex_)
//   → AB/BA deadlock, both threads wait forever
//
// Fix: doneCount_ and totalCount_ are atomics — readable without any lock.
// The predicate never touches graphMutex_, so the two mutexes are never held together.
void TaskScheduler::waitAll() {
    if(totalCount_.load() == 0) return;

    std::unique_lock<std::mutex> lock(allDoneMutex_);
    allDoneCv_.wait(lock, [this] {
        return doneCount_.load() >= totalCount_.load();
    });
    LOG_TAG("Scheduler", "waitAll() returned — all tasks in terminal state");
}

std::size_t TaskScheduler::totalTasks() const {
    std::lock_guard<std::mutex> lock(graphMutex_);
    return tasks_.size();
}

std::size_t TaskScheduler::completedTasks() const {
    std::lock_guard<std::mutex> lock(graphMutex_);
    std::size_t n = 0;
    for(auto& [id, t] : tasks_)
        if(t->isCompleted()) ++n;
    return n;
}

std::size_t TaskScheduler::failedTasks() const {
    std::lock_guard<std::mutex> lock(graphMutex_);
    std::size_t n = 0;
    for(auto& [id, t] : tasks_)
        if(t->isFailed()) ++n;
    return n;
}
