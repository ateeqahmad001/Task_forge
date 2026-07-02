#pragma once

#include <string>
#include <mutex>
#include <chrono>
#include <thread>
#include <sstream>
#include <iostream>

// Thread-safe console logger. std::cout isn't safe to call from multiple
// threads without synchronization — this wraps it with a mutex.
// Singleton because I don't want to pass a logger pointer into every class.

class Logger {
public:
    static Logger& get() {
        static Logger instance;
        return instance;
    }

    Logger(const Logger&)            = delete;
    Logger& operator=(const Logger&) = delete;
    Logger(Logger&&)                 = delete;
    Logger& operator=(Logger&&)      = delete;

    void log(const std::string& message) {
        // Build the line first, then lock — minimize time holding the mutex
        std::ostringstream oss;
        oss << "[" << elapsedMs() << "ms]"
            << "[T-" << shortThreadId() << "] "
            << message;

        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << oss.str() << "\n";
    }

    void log(const std::string& tag, const std::string& message) {
        log("[" + tag + "] " + message);
    }

private:
    Logger() : startTime_(std::chrono::steady_clock::now()) {}

    long long elapsedMs() const {
        using namespace std::chrono;
        auto now = steady_clock::now();
        return duration_cast<milliseconds>(now - startTime_).count();
    }

    // Full thread IDs are ugly in logs, just use the last 4 chars
    std::string shortThreadId() const {
        std::ostringstream oss;
        oss << std::this_thread::get_id();
        std::string full = oss.str();
        return full.size() > 4 ? full.substr(full.size() - 4) : full;
    }

    mutable std::mutex mutex_;
    std::chrono::steady_clock::time_point startTime_;
};

#define LOG(msg)      Logger::get().log(msg)
#define LOG_TAG(t, m) Logger::get().log(t, m)
