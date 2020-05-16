cmake_minimum_required(VERSION 3.1)

project(block_device_performance C)

set(CMAKE_C_FLAGS "-std=gnu11 -Wall -O3")

add_executable(bdp block_device_performance.c)
