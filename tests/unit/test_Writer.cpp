#include <gtest/gtest.h>
#include "Writer.hpp"
#include "LockFreeBuffer.hpp"
#include "SegmentedStorage.hpp"
#include <chrono>
#include <thread>
#include <filesystem>

class WriterTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Create a temporary directory for testing log segments
        testDir = "test_logs";
        std::filesystem::create_directories(testDir);
        queue = std::make_unique<LockFreeQueue>(8192);
        // Create a SegmentedStorage instance with small segment and buffer sizes for testing
        storage = std::make_shared<SegmentedStorage>(
            testDir,
            "test_logsegment",
            1024 * 1024, // max segment size (e.g., 1 MB for test)
            1024         // buffer size (e.g., 1 KB)
        );
    }

    void TearDown() override
    {
        if (writer)
        {
            writer->stop();
        }
        // Cleanup test directory if desired
        std::filesystem::remove_all(testDir);
    }

    std::unique_ptr<LockFreeQueue> queue;
    std::shared_ptr<SegmentedStorage> storage;
    std::unique_ptr<Writer> writer;
    std::string testDir;
};

// Test that the writer starts and stops correctly
TEST_F(WriterTest, StartAndStop)
{
    writer = std::make_unique<Writer>(*queue, storage);
    EXPECT_FALSE(writer->isRunning());

    writer->start();
    EXPECT_TRUE(writer->isRunning());

    writer->stop();
    EXPECT_FALSE(writer->isRunning());
}

// Test multiple start calls
TEST_F(WriterTest, MultipleStartCalls)
{
    writer = std::make_unique<Writer>(*queue, storage);
    writer->start();
    EXPECT_TRUE(writer->isRunning());

    writer->start(); // multiple start calls should not affect the running state
    EXPECT_TRUE(writer->isRunning());

    writer->stop();
    EXPECT_FALSE(writer->isRunning());
}

// Test batch processing with some entries
TEST_F(WriterTest, ProcessBatchEntries)
{
    // Create test log entries (assuming LogEntry has an appropriate constructor)
    std::vector<LogEntry> testEntries = {
        LogEntry{LogEntry::ActionType::READ, "location1", "user1", "subject1"},
        LogEntry{LogEntry::ActionType::CREATE, "location2", "user2", "subject2"},
        LogEntry{LogEntry::ActionType::UPDATE, "location3", "user3", "subject3"}};

    // Enqueue test entries
    for (const auto &entry : testEntries)
    {
        queue->enqueue(entry);
    }

    // Instantiate writer with a batch size equal to number of test entries
    writer = std::make_unique<Writer>(*queue, storage, testEntries.size());
    writer->start();

    // Give some time for the writer thread to process the entries.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Verify that the queue is empty after processing.
    EXPECT_EQ(queue->size(), 0);

    // (Optional) Check that a segment file was written to:
    std::string currentSegmentPath = storage->getCurrentSegmentPath();
    EXPECT_TRUE(std::filesystem::exists(currentSegmentPath));

    writer->stop();
}

// Test behavior when the queue is empty
TEST_F(WriterTest, EmptyQueue)
{
    EXPECT_EQ(queue->size(), 0);

    writer = std::make_unique<Writer>(*queue, storage);
    writer->start();

    // Give some time to verify it handles empty queue gracefully
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(queue->size(), 0);

    writer->stop();
}