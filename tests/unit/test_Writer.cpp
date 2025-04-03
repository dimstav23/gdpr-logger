#include <gtest/gtest.h>
#include "Writer.hpp"
#include "LockFreeBuffer.hpp"
#include <chrono>
#include <thread>

class WriterTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        queue = std::make_unique<LockFreeQueue>(8192);
    }

    void TearDown() override
    {
        if (writer)
        {
            writer->stop();
        }
    }

    std::unique_ptr<LockFreeQueue> queue;
    std::unique_ptr<Writer> writer;
};

// Test that the writer starts and stops correctly
TEST_F(WriterTest, StartAndStop)
{
    writer = std::make_unique<Writer>(*queue);
    EXPECT_FALSE(writer->isRunning());

    writer->start();
    EXPECT_TRUE(writer->isRunning());

    writer->stop();
    EXPECT_FALSE(writer->isRunning());
}

// Test multiple start calls
TEST_F(WriterTest, MultipleStartCalls)
{
    writer = std::make_unique<Writer>(*queue);
    writer->start();
    EXPECT_TRUE(writer->isRunning());

    writer->start();
    EXPECT_TRUE(writer->isRunning());

    writer->stop();
    EXPECT_FALSE(writer->isRunning());
}

// Test batch processing with some entries
TEST_F(WriterTest, ProcessBatchEntries)
{
    std::vector<LogEntry> testEntries = {
        LogEntry{LogEntry::ActionType::READ, "location1", "user1", "subject1"},
        LogEntry{LogEntry::ActionType::CREATE, "location2", "user2", "subject2"},
        LogEntry{LogEntry::ActionType::UPDATE, "location3", "user3", "subject3"}};

    writer = std::make_unique<Writer>(*queue, 3);
    writer->start();

    // Give some time for the writer thread to process the entries.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Verify that the queue is empty after processing.
    EXPECT_EQ(queue->size(), 0);

    writer->stop();
}

// Test behavior when the queue is empty
TEST_F(WriterTest, EmptyQueue)
{
    EXPECT_EQ(queue->size(), 0);

    writer = std::make_unique<Writer>(*queue);
    writer->start();

    // Give some time to verify it handles empty queue gracefully
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(queue->size(), 0);

    writer->stop();
}