cmake_minimum_required(VERSION 3.16)
project(TaskForge VERSION 1.0.0 LANGUAGES CXX)

# ── C++17 required for std::optional, structured bindings, etc. ──────────────
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Compiler warnings — treat your code like production ──────────────────────
if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic -Wno-unused-parameter)
endif()

# ── Thread support (pthreads on Linux/macOS, native on Windows) ───────────────
find_package(Threads REQUIRED)

# ── TaskForge static library ──────────────────────────────────────────────────
# All .cpp files in src/ become one library that both demos link against.
add_library(taskforge_lib STATIC
    src/Task.cpp
    src/ThreadPool.cpp
    src/TaskScheduler.cpp
)

target_include_directories(taskforge_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(taskforge_lib PUBLIC Threads::Threads)

# ── Helper macro: adds an example executable and links the library ─────────────
function(add_demo name source)
    add_executable(${name} ${source})
    target_link_libraries(${name} PRIVATE taskforge_lib)
endfunction()

# ── Demo executables ──────────────────────────────────────────────────────────
add_demo(demo_basic_pool    examples/demo_basic_pool.cpp)
add_demo(demo_dag_scheduler examples/demo_dag_scheduler.cpp)

# ── Nice output: tell the user where to find built binaries ───────────────────
message(STATUS "TaskForge build configured.")
message(STATUS "  Binaries will appear in: ${CMAKE_BINARY_DIR}")
message(STATUS "  Run: ./demo_basic_pool  OR  ./demo_dag_scheduler")
