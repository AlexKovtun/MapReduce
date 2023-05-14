cmake_minimum_required(VERSION 3.0)
project(MapReduce)

set(CMAKE_CXX_STANDARD 11)

add_executable(MapReduce main.cpp MapReduceFramework.h MapReduceClient.h)
