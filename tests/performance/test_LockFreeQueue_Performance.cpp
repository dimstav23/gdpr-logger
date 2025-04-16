#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "LockFreeQueue.hpp"
#include "LogEntry.hpp"
#include <chrono>
#include <memory>

class LockFreeQueuePerformanceTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        queue = std::make_unique<LockFreeQueue>(PERFORMANCE_QUEUE_SIZE);
    }

    void TearDown() override
    {
        queue.reset();
    }

    LogEntry createTestEntry(int id)
    {
        return LogEntry(
            LogEntry::ActionType::READ,
            "data/location/" + std::to_string(id),
            "user" + std::to_string(id),
            "subject" + std::to_string(id % 10));
    }

    const size_t PERFORMANCE_QUEUE_SIZE = 1024;
    std::unique_ptr<LockFreeQueue> queue;
};

TEST_F(LockFreeQueuePerformanceTest, PerformanceBenchmark)
{
    const int NUM_OPERATIONS = 1000000;

    // Measure enqueue performance
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < NUM_OPERATIONS; i++)
    {
        queue->enqueueBlocking(createTestEntry(i), std::chrono::milliseconds(100));
    }

    auto enqueueTime = std::chrono::high_resolution_clock::now() - start;

    // Measure dequeue performance
    start = std::chrono::high_resolution_clock::now();

    LogEntry entry;
    for (int i = 0; i < NUM_OPERATIONS; i++)
    {
        queue->dequeue(entry);
    }

    auto dequeueTime = std::chrono::high_resolution_clock::now() - start;

    std::cout << "Performance Benchmark (Operations: " << NUM_OPERATIONS << ")" << std::endl;
    std::cout << "Enqueue time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(enqueueTime).count()
              << "ms ("
              << (NUM_OPERATIONS * 1000.0 / std::chrono::duration_cast<std::chrono::milliseconds>(enqueueTime).count())
              << " ops/sec)" << std::endl;

    std::cout << "Dequeue time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(dequeueTime).count()
              << "ms ("
              << (NUM_OPERATIONS * 1000.0 / std::chrono::duration_cast<std::chrono::milliseconds>(dequeueTime).count())
              << " ops/sec)" << std::endl;
}