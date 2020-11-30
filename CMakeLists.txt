cmake_minimum_required(VERSION 3.1)

project(ddnz C)

set(CMAKE_C_FLAGS "-std=gnu11 -Wall -O3")

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_executable(ddnz ddnz.c)
target_link_libraries(ddnz Threads::Threads)
