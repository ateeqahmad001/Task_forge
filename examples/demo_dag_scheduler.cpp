#include <iostream>
#include <iomanip>
#include <sstream>
#include <memory>
#include <chrono>
#include <thread>
#include "taskforge/Task.h"
#include "taskforge/TaskScheduler.h"
#include "taskforge/Logger.h"

// ML training pipeline with realistic data dependencies.
//
// The DAG:
//   [FetchRawData] ──────────────────────────────────────────┐
//   [LoadConfig]  ──► [ValidateSchema] ──► [CleanData] ──► [MergeInputs]
//                                                             │
//                                         ┌──────────────────┘
//                                         ▼
//                              ┌─► [TrainModelA] ──┐
//                              │                   ├──► [EnsembleEval]
//                              └─► [TrainModelB] ──┘         │
//                                                             ▼
//                                                     [GenerateReport]
//
// FetchRawData and LoadConfig start immediately (no deps).
// TrainModelA and TrainModelB run in parallel after MergeInputs.
// The timestamps in the output prove ordering was respected.

static std::shared_ptr<Task> makeStage(
    const std::string& name,
    const std::string& description,
    int durationMs,
    int priority = 0)
{
    return std::make_shared<Task>(
        name,
        [name, description, durationMs]() {
            LOG_TAG("Pipeline", "▶ START  " + name + " — " + description);
            std::this_thread::sleep_for(std::chrono::milliseconds(durationMs));
            LOG_TAG("Pipeline", "■ DONE   " + name);
        },
        priority
    );
}

int main() {
    LOG("╔══════════════════════════════════════════════════════════════╗");
    LOG("║       TaskForge — DAG Scheduler Demo                        ║");
    LOG("║  Scenario: ML training pipeline with data dependencies      ║");
    LOG("╚══════════════════════════════════════════════════════════════╝");
    LOG("");
    LOG("Pipeline DAG:");
    LOG("  [FetchRawData] ──────────────────────────────────┐          ");
    LOG("  [LoadConfig] ──► [ValidateSchema] ──► [CleanData]──► [MergeInputs]");
    LOG("                                                        │      ");
    LOG("                                        ┌──────────────┘      ");
    LOG("                                        ▼                     ");
    LOG("                             ┌─► [TrainModelA] ──┐            ");
    LOG("                             │                   ├──► [EnsembleEval]");
    LOG("                             └─► [TrainModelB] ──┘        │   ");
    LOG("                                                           ▼   ");
    LOG("                                                   [GenerateReport]");
    LOG("");
    LOG("Starting pipeline with 4 worker threads...");
    LOG("──────────────────────────────────────────────────────────────");

    TaskScheduler pipeline(4);

    auto fetchRaw   = makeStage("FetchRawData",   "download raw dataset from S3",       120);
    auto loadConfig = makeStage("LoadConfig",     "parse YAML training configuration",   40);
    auto validate   = makeStage("ValidateSchema", "check column types and ranges",        60);
    auto clean      = makeStage("CleanData",      "impute nulls, remove outliers",        80);
    auto merge      = makeStage("MergeInputs",    "join cleaned features with raw data",  50);
    auto trainA     = makeStage("TrainModelA",    "gradient boosting on feature set A",  150, /*priority=*/5);
    auto trainB     = makeStage("TrainModelB",    "neural net on feature set B",         130, /*priority=*/5);
    auto ensemble   = makeStage("EnsembleEval",   "blend predictions, compute metrics",   70);
    auto report     = makeStage("GenerateReport", "write PDF report and upload to GCS",   60);

    pipeline.addTask(fetchRaw);
    pipeline.addTask(loadConfig);
    pipeline.addTask(validate, { loadConfig->getId() });
    pipeline.addTask(clean,    { validate->getId() });
    pipeline.addTask(merge,    { fetchRaw->getId(), clean->getId() });
    pipeline.addTask(trainA,   { merge->getId() });
    pipeline.addTask(trainB,   { merge->getId() });
    pipeline.addTask(ensemble, { trainA->getId(), trainB->getId() });
    pipeline.addTask(report,   { ensemble->getId() });

    LOG("");
    LOG("All 9 stages registered. Launching...");
    LOG("──────────────────────────────────────────────────────────────");

    auto wallStart = std::chrono::steady_clock::now();
    pipeline.run();
    pipeline.waitAll();
    auto wallEnd = std::chrono::steady_clock::now();

    int wallMs = static_cast<int>(
        std::chrono::duration_cast<std::chrono::milliseconds>(wallEnd - wallStart).count());

    int seqMs = 120+40+60+80+50+150+130+70+60;  // = 760ms total if run serially

    LOG("──────────────────────────────────────────────────────────────");
    LOG("");
    LOG("Pipeline complete.");
    LOG("  Stages completed:  " +
        std::to_string(pipeline.completedTasks()) + "/" +
        std::to_string(pipeline.totalTasks()));
    LOG("  Wall time:         " + std::to_string(wallMs) + "ms");
    LOG("  Sequential would:  " + std::to_string(seqMs) + "ms");
    LOG("  Speedup:           " +
        [&]{
            std::ostringstream oss;
            oss << std::fixed << std::setprecision(1)
                << (static_cast<double>(seqMs) / wallMs) << "x";
            return oss.str();
        }());
    LOG("");
    LOG("ORDER PROOF (check timestamps above):               ");
    LOG("  FetchRawData  starts at t=0  (no deps)           ");
    LOG("  LoadConfig    starts at t=0  (no deps, parallel) ");
    LOG("  ValidateSchema starts AFTER LoadConfig            ");
    LOG("  CleanData      starts AFTER ValidateSchema        ");
    LOG("  MergeInputs    starts AFTER FetchRawData+CleanData");
    LOG("  TrainA+TrainB  start  AFTER MergeInputs (parallel)");
    LOG("  EnsembleEval   starts AFTER BOTH TrainA+TrainB   ");
    LOG("  GenerateReport starts AFTER EnsembleEval          ");
    LOG("");
    LOG("╔══════════════════════════════════════════════════════════════╗");
    LOG("║  OS / CS CONCEPTS SHOWN:                                    ║");
    LOG("║  • DAG scheduling: topological dependency ordering          ║");
    LOG("║  • Event-driven dispatch: task releases trigger on complete ║");
    LOG("║  • Priority scheduling: TrainA/B get priority=5             ║");
    LOG("║  • Concurrent critical path: parallel branches save time    ║");
    LOG("║  • mutex + condition_variable: safe cross-thread signalling ║");
    LOG("╚══════════════════════════════════════════════════════════════╝");

    return 0;
}
