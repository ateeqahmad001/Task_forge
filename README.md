# TaskForge

A thread pool and DAG-based task scheduler I built from scratch in C++17 to get a solid handle on OS concurrency concepts — the kind that come up a lot in SWE internship interviews. No external libraries, just STL primitives.

---

## What it does

There are two layers:

**ThreadPool** — a fixed set of worker threads that sleep on a condition variable until work arrives, grab the highest-priority task off a max-heap, run it, and go back to sleep. The priority queue upgrade was actually one of the more interesting parts: you can't just use `Task::operator<` directly because the queue stores `shared_ptr<Task>`, so comparing by pointer address is meaningless — needed a custom comparator that dereferences first.

**TaskScheduler** — wraps the thread pool with dependency tracking. You register tasks with the IDs of other tasks that must finish first. The scheduler builds a DAG internally, does cycle detection (DFS with WHITE/GRAY/BLACK coloring) before accepting any task, and then releases tasks to the pool automatically the moment all their dependencies complete. No polling — purely event-driven via a completion callback.

---

## Concepts covered

| Concept | Where |
|---|---|
| Thread creation and lifecycle | `ThreadPool` constructor/destructor |
| `std::mutex` — mutual exclusion | Task queue in `ThreadPool` |
| `std::condition_variable` — sleep/wake | `workerLoop()` in `ThreadPool.cpp` |
| RAII lock management (`lock_guard`, `unique_lock`) | Throughout |
| Spurious wakeup defence | `while` predicate in `workerLoop()` |
| `std::atomic` — lock-free reads | `Task::status_`, `TaskScheduler::doneCount_` |
| Producer-Consumer pattern | `submit()` produces, `workerLoop()` consumes |
| Priority scheduling (max-heap) | `std::priority_queue` + `TaskPtrComparator` |
| DAG construction | `TaskScheduler::addTask()` |
| Cycle detection via DFS colouring | `TaskScheduler::hasCycle()` |
| Topological execution order | `TaskScheduler::onTaskComplete()` cascade |
| AB/BA deadlock avoidance | Two-mutex design in `TaskScheduler` |
| Graceful shutdown with thread joining | `ThreadPool::~ThreadPool()` |

---

## Project structure

```
TaskForge/
├── CMakeLists.txt
├── README.md
├── include/taskforge/
│   ├── Logger.h          — thread-safe timestamped logger (header-only)
│   ├── Task.h            — unit of work: callable + priority + status
│   ├── ThreadPool.h      — fixed worker thread pool with priority queue
│   └── TaskScheduler.h   — DAG scheduler wrapping the thread pool
├── src/
│   ├── Logger.cpp
│   ├── Task.cpp
│   ├── ThreadPool.cpp
│   └── TaskScheduler.cpp
├── examples/
│   ├── demo_basic_pool.cpp      — concurrent downloader (8 files, 3 workers)
│   └── demo_dag_scheduler.cpp   — ML pipeline with 9 stages and real deps
└── tests/
    └── manual_test_notes.md
```

### Architecture

```
  User code
     │
     │  addTask(task, deps)         submit(task)
     ▼                                  ▼
 ┌─────────────────────────────────────────────┐
 │             TaskScheduler                   │
 │                                             │
 │  tasks_          dependsOn_    dependents_  │
 │  {id→Task}       {id→[ids]}    {id→[ids]}   │
 │                                             │
 │  pendingDepCount_[id] == 0 → submitToPool() │
 │  onTaskComplete(id)   → decrement deps      │
 └──────────────┬──────────────────────────────┘
                │  submit(shared_ptr<Task>)
                ▼
 ┌─────────────────────────────────────────────┐
 │               ThreadPool                    │
 │                                             │
 │  priority_queue<Task>  (max-heap)           │
 │  mutex_  +  condition_variable cv_          │
 │                                             │
 │  Worker-1 ──┐                               │
 │  Worker-2 ──┼──► workerLoop():             │
 │  Worker-3 ──┘    wait(cv_) → top() → pop() │
 │  Worker-N        → task->execute()          │
 └─────────────────────────────────────────────┘
                │
                ▼
           Task::execute()
           sets status RUNNING → COMPLETED | FAILED
           calls onTaskComplete() callback
```

---

## Build

Requires CMake ≥ 3.16 and a C++17 compiler (GCC ≥ 7, Clang ≥ 5, MSVC 2017+).

```bash
git clone <repo-url>
cd TaskForge
cmake -S . -B build
cmake --build build
```

Executables land in `build/`.

---

## Run

### Demo 1 — Thread Pool

```bash
./build/demo_basic_pool
```

Simulates fetching 8 files of varying sizes with 3 worker threads. Wall time should be roughly a third of sequential; the logs show multiple thread IDs active at t=0 which proves actual parallelism.

Example output (abridged):

```
[0ms][T-4864]  Sequential download estimate: 243ms
[0ms][T-4864]  With 3 parallel workers, expect roughly: 81ms
[0ms][T-8880]  [Task] Starting  | ID=1 name='dataset_train.csv' priority=0
[0ms][T-6176]  [Task] Starting  | ID=3 name='config.json'       priority=0
[1ms][T-3472]  [Task] Starting  | ID=7 name='checkpoint_epoch5.pt' priority=0
...
[124ms][T-4864]  Wall time:        124ms
[124ms][T-4864]  Speedup:          2.0x
```

### Demo 2 — DAG Scheduler

```bash
./build/demo_dag_scheduler
```

A 9-stage ML training pipeline. The scheduler enforces all the ordering constraints automatically — you can verify from the timestamps that e.g. `MergeInputs` only starts after both `FetchRawData` and `CleanData` finish.

```
[4ms]   ▶ START  FetchRawData
[4ms]   ▶ START  LoadConfig
[44ms]  ■ DONE   LoadConfig
[44ms]  ▶ START  ValidateSchema
[185ms] ■ DONE   CleanData
[185ms] Releasing to pool: 'MergeInputs'   ← both deps done
[235ms] ▶ START  TrainModelA              ← parallel
[235ms] ▶ START  TrainModelB              ← parallel
[385ms] Releasing to pool: 'EnsembleEval'
[515ms] ■ DONE   GenerateReport

  Stages completed:  9/9
  Wall time:         511ms
  Sequential would:  760ms
  Speedup:           1.5x
```

The 1.5x ceiling comes from Amdahl's Law — the serial critical path (LoadConfig→Validate→Clean→Merge = 230ms) limits the theoretical max regardless of how many threads you throw at it.

---

## Design decisions worth knowing

**Why `shared_ptr<Task>` in the queue?**
The `TaskScheduler` needs to read task status after execution. If the pool owned tasks by value and destroyed them on completion, the scheduler would be left with dangling references. `shared_ptr` lets both share ownership without either having to outlive the other.

**Why two mutexes in `TaskScheduler`?**
I actually ran into a deadlock here during development. The original design used one mutex for everything: `waitAll()` held it while checking the finished count, and `onTaskComplete()` also needed it to update state — classic AB/BA deadlock. The fix was making `doneCount_` and `totalCount_` atomics so `waitAll()`'s predicate can read them without acquiring `graphMutex_` at all. The two mutexes are now never held simultaneously.

**Why `while` and not `if` on `cv_.wait()`?**
Condition variables have spurious wakeups — the OS can wake a sleeping thread for no reason. Without the `while` (re-checking the predicate), a worker could wake up, find an empty queue, and call `taskQueue_.top()` on it which is undefined behaviour. The lambda form `cv_.wait(lock, predicate)` is just syntactic sugar for the same `while` loop.

**Why reject cycles at `addTask()` time?**
A cycle means two tasks would wait on each other forever — it's not a recoverable runtime error, it's a guaranteed deadlock. Catching it upfront before any thread is started gives a clear error message rather than a silent hang that's hard to debug.

---

## What I learned / challenges

Getting the `TaskScheduler` deadlock-free was the hardest part. The interaction between `graphMutex_` and `allDoneMutex_` is subtle — I had to draw out the lock acquisition order across every code path before the two-atomic solution became obvious. I'd recommend doing the same if you're building anything similar.

The `submitToPool()` function manually calling `graphMutex_.unlock()` / `graphMutex_.lock()` around `pool_.submit()` is also a bit fragile — if `pool_.submit()` throws, we'd relock a mutex the caller expects to be locked. It works for now because submit only throws on null or shut-down, neither of which should happen here, but I'd want to make that more robust before calling this production-ready.

Exception handling in tasks is also minimal right now. If a task throws, the status gets set to FAILED and the scheduler moves on — but there's no way for the caller to retrieve what the exception was. Adding a `std::exception_ptr` field to `Task` and a `getException()` getter would fix that.

---

