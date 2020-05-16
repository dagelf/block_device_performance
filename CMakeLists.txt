cmake_minimum_required(VERSION 3.1)

project(block_device_performance C)

set(CMAKE_C_FLAGS "-std=gnu11 -Wall -O3")

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_executable(bdp block_device_performance.c)
target_link_libraries(bdp Threads::Threads)
