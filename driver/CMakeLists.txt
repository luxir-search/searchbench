cmake_minimum_required(VERSION 3.20)
project(searchbench_driver LANGUAGES CXX)

find_package(hdr_histogram CONFIG REQUIRED)
find_package(llhttp CONFIG REQUIRED)
find_package(Threads REQUIRED)

add_executable(bench_replay bench_replay.cpp)
target_compile_features(bench_replay PRIVATE cxx_std_20)
target_compile_definitions(bench_replay PRIVATE _GNU_SOURCE)
target_compile_options(bench_replay PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(
  bench_replay
  PRIVATE
    llhttp::llhttp_static
    $<IF:$<TARGET_EXISTS:hdr_histogram::hdr_histogram>,hdr_histogram::hdr_histogram,hdr_histogram::hdr_histogram_static>
    Threads::Threads)
