#include <iostream>
#include <memory>
#include <chrono>
#include <thread>
#include <vector>
#include <iomanip>
#include "taskforge/Task.h"
#include "taskforge/ThreadPool.h"
#include "taskforge/Logger.h"

// Scenario: simulate a download manager fetching 8 files with 3 worker threads.
// Good for showing that wall time ≈ ceil(8/3) * avg, not sum of all tasks.

int main() {
    LOG("╔══════════════════════════════════════════════════════╗");
    LOG("║       TaskForge — Thread Pool Demo                  ║");
    LOG("║  Scenario: Concurrent file downloader (3 workers)   ║");
    LOG("╚══════════════════════════════════════════════════════╝");
    LOG("");

    struct DownloadJob {
        std::string filename;
        int         sizeKb;
    };

    const std::vector<DownloadJob> jobs = {
        { "dataset_train.csv",    450 },
        { "model_weights.bin",    820 },
        { "config.json",           30 },
        { "vocab.txt",            210 },
        { "test_labels.csv",      180 },
        { "README.md",             15 },
        { "checkpoint_epoch5.pt", 640 },
        { "eval_results.json",     90 },
    };

    // 1 KB ≈ 0.1ms, keeps task durations in the 15–82ms range
    auto downloadTimeMs = [](int kb) { return kb / 10; };

    int totalSequentialMs = 0;
    for(auto& j : jobs) totalSequentialMs += downloadTimeMs(j.sizeKb);

    LOG("Sequential download estimate: " + std::to_string(totalSequentialMs) + "ms");
    LOG("With 3 parallel workers, expect roughly: " +
        std::to_string(totalSequentialMs / 3) + "ms");
    LOG("");
    LOG("Starting downloads...");
    LOG("──────────────────────────────────────────────────────");

    auto wallStart = std::chrono::steady_clock::now();

    {
        ThreadPool pool(3);

        for(auto& job : jobs) {
            int ms = downloadTimeMs(job.sizeKb);
            auto task = std::make_shared<Task>(
                job.filename,
                [name = job.filename, kb = job.sizeKb, ms]() {
                    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
                    LOG_TAG("Download",
                        "✓ " + name + "  [" + std::to_string(kb) + " KB, " +
                        std::to_string(ms) + "ms]");
                },
                /*priority=*/0
            );
            pool.submit(task);
        }
        // pool destructor joins all 3 worker threads here
    }

    auto wallEnd = std::chrono::steady_clock::now();
    int actualMs = static_cast<int>(
        std::chrono::duration_cast<std::chrono::milliseconds>(wallEnd - wallStart).count());

    LOG("──────────────────────────────────────────────────────");
    LOG("All 8 files downloaded.");
    LOG("  Wall time:        " + std::to_string(actualMs) + "ms");
    LOG("  Sequential would: " + std::to_string(totalSequentialMs) + "ms");
    LOG("  Speedup:          " +
        [&]{
            std::ostringstream oss;
            oss << std::fixed << std::setprecision(1)
                << (static_cast<double>(totalSequentialMs) / actualMs) << "x";
            return oss.str();
        }());
    LOG("");
    LOG("╔══════════════════════════════════════════════════════╗");
    LOG("║  OS CONCEPTS SHOWN:                                 ║");
    LOG("║  • Thread pool: reuse N threads for M > N tasks     ║");
    LOG("║  • mutex + condition_variable: safe task handoff    ║");
    LOG("║  • RAII: pool destructor joins threads automatically║");
    LOG("╚══════════════════════════════════════════════════════╝");

    return 0;
}
