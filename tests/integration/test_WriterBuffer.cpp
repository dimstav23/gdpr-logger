#include <gtest/gtest.h>
#include "Writer.hpp"
#include "LockFreeBuffer.hpp"
#include <chrono>
#include <thread>
#include <vector>

class WriterIntegrationTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        logQueue = std::make_unique<LockFreeQueue>(1024);
        writer = std::make_unique<Writer>(*logQueue);
    }

    void TearDown() override
    {
        if (writer)
        {
            writer->stop();
        }
    }

    std::unique_ptr<LockFreeQueue> logQueue;
    std::unique_ptr<Writer> writer;

    LogEntry createTestLogEntry(int index)
    {
        return LogEntry{LogEntry::ActionType::UPDATE,
                        "location" + std::to_string(index),
                        "user123" + std::to_string(index),
                        "subject456" + std::to_string(index)};
    }
};

// Test basic processing functionality
TEST_F(WriterIntegrationTest, BasicWriteOperation)
{
    const int NUM_ENTRIES = 500;
    for (int i = 0; i < NUM_ENTRIES; ++i)
    {
        ASSERT_TRUE(logQueue->enqueue(createTestLogEntry(i)))
            << "Failed to enqueue entry " << i;
    }

    EXPECT_EQ(logQueue->size(), 500);

    writer->start();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    writer->stop();

    EXPECT_EQ(logQueue->size(), 0) << "Not all entries were processed";
}

// Test concurrent writing and processing
TEST_F(WriterIntegrationTest, ConcurrentWriteAndProcess)
{
    const int NUM_ENTRIES = 1000;
    const int NUM_PRODUCERS = 4;

    // Function to simulate producers adding log entries
    auto producer = [this](int start, int count)
    {
        for (int i = start; i < start + count; ++i)
        {
            // Add some randomness to simulate real-world scenario
            std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 10));
            logQueue->enqueue(createTestLogEntry(i));
        }
    };

    writer->start();

    std::vector<std::thread> producerThreads;
    for (int i = 0; i < NUM_PRODUCERS; ++i)
    {
        producerThreads.emplace_back(producer, i * (NUM_ENTRIES / NUM_PRODUCERS),
                                     NUM_ENTRIES / NUM_PRODUCERS);
    }

    // Wait for all producers to finish
    for (auto &t : producerThreads)
    {
        t.join();
    }

    // Wait for processing
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    writer->stop();

    EXPECT_EQ(logQueue->size(), 0) << "Not all entries were processed";
}
/*
// Test writer behavior with a full queue
TEST_F(WriterIntegrationTest, FullQueueHandling)
{
    const int QUEUE_CAPACITY = 1024;
    const int NUM_ENTRIES = QUEUE_CAPACITY * 2;

    // Attempt to enqueue more entries than queue capacity
    int successfulEntries = 0;
    for (int i = 0; i < NUM_ENTRIES; ++i)
    {
        if (logQueue->enqueue(createTestLogEntry(i)))
        {
            successfulEntries++;
        }
    }

    // Start the writer
    writer->start();

    // Wait for entries to be processed
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Stop the writer
    writer->stop();

    // Verify all entries were processed
    EXPECT_EQ(logQueue->size(), 0) << "Not all entries were processed";

    // Ensure we didn't completely block on a full queue
    EXPECT_GT(successfulEntries, QUEUE_CAPACITY)
        << "Queue should handle more entries than its initial capacity";
}
*/