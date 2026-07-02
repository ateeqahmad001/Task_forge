#pragma once

#include <string>
#include <functional>
#include <atomic>
#include <stdexcept>

// A Task is basically just a callable + some metadata (name, priority, status).
// Kept simple on purpose — it shouldn't know anything about threads or queues.

enum class TaskStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED
};

inline std::string statusToString(TaskStatus s) {
    switch(s) {
        case TaskStatus::PENDING:   return "PENDING";
        case TaskStatus::RUNNING:   return "RUNNING";
        case TaskStatus::COMPLETED: return "COMPLETED";
        case TaskStatus::FAILED:    return "FAILED";
    }
    return "UNKNOWN";
}

class Task {
public:
    using Id = int;

    static constexpr int DEFAULT_PRIORITY = 0;

    // name: shown in logs; work: the actual lambda; priority: higher = runs first
    explicit Task(std::string           name,
                  std::function<void()> work,
                  int                   priority = DEFAULT_PRIORITY);

    // Copying a Task doesn't make sense (same ID? same status?) — delete it.
    // Moving is fine for storing in containers.
    Task(const Task&)            = delete;
    Task& operator=(const Task&) = delete;
    Task(Task&&)                 = default;
    Task& operator=(Task&&)      = default;

    void execute();

    Id                 getId()       const { return id_; }
    const std::string& getName()     const { return name_; }
    int                getPriority() const { return priority_; }
    TaskStatus         getStatus()   const { return status_.load(); }

    bool isPending()   const { return status_.load() == TaskStatus::PENDING;   }
    bool isRunning()   const { return status_.load() == TaskStatus::RUNNING;   }
    bool isCompleted() const { return status_.load() == TaskStatus::COMPLETED; }
    bool isFailed()    const { return status_.load() == TaskStatus::FAILED;    }

    // Used by the priority queue comparator — higher priority_ value = higher precedence
    bool operator<(const Task& other) const {
        return priority_ < other.priority_;
    }

private:
    static std::atomic<Id> nextId_;

    Id                      id_;
    std::string             name_;
    std::function<void()>   work_;
    int                     priority_;
    std::atomic<TaskStatus> status_;
};
