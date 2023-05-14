cmake_minimum_required(VERSION 3.0)
project(MapReduce)

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_CXX_FLAGS -pthread)


add_executable(MapReduce main.cpp MapReduceFramework.h  MapReduceFramework.cpp
        MapReduceClient.h ThreadContext.cpp ThreadContext.h
        JobContext.cpp JobContext.h)
