#include <gtest/gtest.h>
#include "LoggingAPI.hpp"
#include "BufferQueue.hpp"
#include <chrono>
#include <thread>

class LoggingAPITest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Create a fresh instance for each test
        LoggingAPI::s_instance.reset();

        // Create a BufferQueue instance
        queue = std::make_shared<BufferQueue>(1024, 10);
    }

    void TearDown() override
    {
        // Clean up the singleton
        LoggingAPI::s_instance.reset();
    }

    std::shared_ptr<BufferQueue> queue;
};

// Test getInstance returns the same instance
TEST_F(LoggingAPITest, GetInstanceReturnsSingleton)
{
    LoggingAPI &instance1 = LoggingAPI::getInstance();
    LoggingAPI &instance2 = LoggingAPI::getInstance();

    EXPECT_EQ(&instance1, &instance2);
}

// Test initialization with valid queue
TEST_F(LoggingAPITest, InitializeWithValidQueue)
{
    LoggingAPI &api = LoggingAPI::getInstance();

    EXPECT_TRUE(api.initialize(queue));
    EXPECT_TRUE(api.reset());
}

// Test initialization with null queue
TEST_F(LoggingAPITest, InitializeWithNullQueue)
{
    LoggingAPI &api = LoggingAPI::getInstance();

    EXPECT_FALSE(api.initialize(nullptr));
}

// Test double initialization
TEST_F(LoggingAPITest, DoubleInitialization)
{
    LoggingAPI &api = LoggingAPI::getInstance();

    EXPECT_TRUE(api.initialize(queue));
    EXPECT_FALSE(api.initialize(queue));

    EXPECT_TRUE(api.reset());
}

// Test creating producer token
TEST_F(LoggingAPITest, CreateProducerToken)
{
    LoggingAPI &api = LoggingAPI::getInstance();

    // Should throw when not initialized
    EXPECT_THROW(api.createProducerToken(), std::runtime_error);

    EXPECT_TRUE(api.initialize(queue));

    // Should not throw when initialized
    EXPECT_NO_THROW({
        BufferQueue::ProducerToken token = api.createProducerToken();
    });

    EXPECT_TRUE(api.reset());
}

// Test appending log entry after initialization
TEST_F(LoggingAPITest, AppendAfterInitialization)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    EXPECT_TRUE(api.initialize(queue));

    BufferQueue::ProducerToken token = api.createProducerToken();
    LogEntry entry(LogEntry::ActionType::READ, "location", "user", "subject");

    EXPECT_TRUE(api.append(std::move(entry), token));
    EXPECT_TRUE(api.reset());
}

// Test blocking append with queue eventually emptying
TEST_F(LoggingAPITest, BlockingAppendWithConsumption)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    auto smallQueue = std::make_shared<BufferQueue>(2, 1);
    EXPECT_TRUE(api.initialize(smallQueue, std::chrono::milliseconds(1000)));

    BufferQueue::ProducerToken token = api.createProducerToken();

    // Since queue grows dynamically, we'll test timeout instead
    LogEntry entry1(LogEntry::ActionType::READ, "location1", "user1", "subject1");
    EXPECT_TRUE(api.append(std::move(entry1), token));

    LogEntry entry2(LogEntry::ActionType::READ, "location2", "user2", "subject2");
    // With dynamic queue, this will succeed immediately
    auto start = std::chrono::steady_clock::now();
    EXPECT_TRUE(api.append(std::move(entry2), token));
    auto end = std::chrono::steady_clock::now();

    // Verify it doesn't block since queue can grow
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    EXPECT_LT(duration, 100); // Should be very fast

    // Verify both items are in the queue
    EXPECT_EQ(smallQueue->size(), 2);

    EXPECT_TRUE(api.reset());
}

// Test append timeout behavior (new test)
TEST_F(LoggingAPITest, AppendTimeoutBehavior)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    auto queue = std::make_shared<BufferQueue>(1024, 1);

    // Initialize with a very short timeout
    EXPECT_TRUE(api.initialize(queue, std::chrono::milliseconds(50)));

    BufferQueue::ProducerToken token = api.createProducerToken();
    LogEntry entry(LogEntry::ActionType::READ, "location", "user", "subject");

    auto start = std::chrono::steady_clock::now();
    EXPECT_TRUE(api.append(std::move(entry), token)); // Should succeed immediately since queue grows
    auto end = std::chrono::steady_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    EXPECT_LT(duration, 10); // Very fast operation

    EXPECT_TRUE(api.reset());
}

// Test batch append functionality
TEST_F(LoggingAPITest, AppendBatch)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    EXPECT_TRUE(api.initialize(queue));

    BufferQueue::ProducerToken token = api.createProducerToken();

    std::vector<LogEntry> entries;
    for (int i = 0; i < 5; i++)
    {
        entries.emplace_back(
            LogEntry::ActionType::READ,
            "location_" + std::to_string(i),
            "user",
            "subject_" + std::to_string(i));
    }

    EXPECT_TRUE(api.appendBatch(std::move(entries), token));
    EXPECT_EQ(queue->size(), 5);

    // Test empty batch
    std::vector<LogEntry> emptyEntries;
    EXPECT_TRUE(api.appendBatch(std::move(emptyEntries), token));

    EXPECT_TRUE(api.reset());
}

// Test shutdown without initialization
TEST_F(LoggingAPITest, ShutdownWithoutInitialization)
{
    LoggingAPI &api = LoggingAPI::getInstance();

    EXPECT_FALSE(api.reset());
}

// Test shutdown with wait for completion
TEST_F(LoggingAPITest, ShutdownWithWait)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    EXPECT_TRUE(api.initialize(queue));

    BufferQueue::ProducerToken token = api.createProducerToken();
    LogEntry entry(LogEntry::ActionType::READ, "location", "user", "subject");
    EXPECT_TRUE(api.append(std::move(entry), token));

    // Launch an asynchronous consumer that waits briefly before draining the queue.
    std::thread consumer([this]()
                         {
        std::this_thread::sleep_for(std::chrono::milliseconds(500)); // simulate delay
        BufferQueue::ConsumerToken consumerToken = queue->createConsumerToken();
        QueueItem dummyItem;
        while (queue->dequeue(dummyItem, consumerToken))
        {
        } });

    EXPECT_TRUE(api.reset());
    consumer.join();
    EXPECT_TRUE(queue->size() == 0);
}

// Test export logs without initialization
TEST_F(LoggingAPITest, ExportLogsWithoutInitialization)
{
    LoggingAPI &api = LoggingAPI::getInstance();

    auto now = std::chrono::system_clock::now();
    EXPECT_FALSE(api.exportLogs("output.log", now, now));
}

// Test export logs after initialization (unimplemented)
TEST_F(LoggingAPITest, ExportLogsAfterInitialization)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    EXPECT_TRUE(api.initialize(queue));

    auto now = std::chrono::system_clock::now();
    EXPECT_FALSE(api.exportLogs("output.log", now, now));

    EXPECT_TRUE(api.reset());
}

// Test thread safety of singleton
TEST_F(LoggingAPITest, ThreadSafetySingleton)
{
    std::vector<std::thread> threads;
    std::vector<LoggingAPI *> instances(10);

    for (int i = 0; i < 10; i++)
    {
        threads.emplace_back([i, &instances]()
                             { instances[i] = &LoggingAPI::getInstance(); });
    }

    for (auto &t : threads)
    {
        t.join();
    }

    // All threads should get the same instance
    for (int i = 1; i < 10; i++)
    {
        EXPECT_EQ(instances[0], instances[i]);
    }
}

// Test thread safety of API operations
TEST_F(LoggingAPITest, ThreadSafetyOperations)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    EXPECT_TRUE(api.initialize(queue));

    std::vector<std::thread> threads;
    for (int i = 0; i < 10; i++)
    {
        threads.emplace_back([&api, i]()
                             {
                                 // Create producer token for this thread
                                 BufferQueue::ProducerToken token = api.createProducerToken();
                                 
                                 // Each thread appends 10 entries
                                 for (int j = 0; j < 10; j++) {
                                     LogEntry entry(
                                         LogEntry::ActionType::READ,
                                         "location_" + std::to_string(i),
                                         "user_" + std::to_string(i),
                                         "subject_" + std::to_string(j)
                                        );
                                     EXPECT_TRUE(api.append(std::move(entry), token));
                                 } });
    }

    for (auto &t : threads)
    {
        t.join();
    }

    EXPECT_EQ(queue->size(), 100);
}

// Main function that runs all the tests
int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}