cmake_minimum_required(VERSION 3.30)
project(test_task)

set(CMAKE_CXX_STANDARD 20)

find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

add_executable(test_task
        event_bus/iface/IEventBus.h
        event_bus/iface/IThreadPool.h
        event_bus/src/ThreadPool.cpp
        event_bus/src/ThreadPool.h
        event_bus/src/main.cpp
        event_bus/src/EventBus.cpp
        event_bus/src/EventBus.h
        event_bus/utils/utils.h
        event_bus/tests/EventBusTest.cpp
)

target_link_libraries(test_task
        ${GTEST_LIBRARIES}
)
