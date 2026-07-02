#include "taskforge/Task.h"
#include "taskforge/Logger.h"

// Static member — must be defined exactly once outside the class
std::atomic<Task::Id> Task::nextId_{ 1 };

Task::Task(std::string name, std::function<void()> work, int priority)
    : id_      (nextId_.fetch_add(1))
    , name_    (std::move(name))
    , work_    (std::move(work))
    , priority_(priority)
    , status_  (TaskStatus::PENDING)
{
    if(!work_) {
        throw std::invalid_argument(
            "Task '" + name_ + "': work function cannot be null");
    }
}

// Called by a worker thread. Marks RUNNING, runs the callable, marks COMPLETED or FAILED.
// Catching (...) instead of just std::exception because users might throw anything.
void Task::execute() {
    status_.store(TaskStatus::RUNNING);

    LOG_TAG("Task", "Starting  | ID=" + std::to_string(id_) +
                    " name='" + name_ + "'" +
                    " priority=" + std::to_string(priority_));

    try {
        work_();
        status_.store(TaskStatus::COMPLETED);
        LOG_TAG("Task", "Completed | ID=" + std::to_string(id_) +
                        " name='" + name_ + "'");

    } catch(const std::exception& e) {
        status_.store(TaskStatus::FAILED);
        LOG_TAG("Task", "FAILED    | ID=" + std::to_string(id_) +
                        " name='" + name_ + "' error='" + e.what() + "'");
    } catch(...) {
        status_.store(TaskStatus::FAILED);
        LOG_TAG("Task", "FAILED    | ID=" + std::to_string(id_) +
                        " name='" + name_ + "' error='unknown exception'");
    }
}
