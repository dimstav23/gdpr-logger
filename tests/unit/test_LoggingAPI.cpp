#include <gtest/gtest.h>
#include "LoggingAPI.hpp"
#include "LockFreeBuffer.hpp"
#include <chrono>
#include <thread>

class LoggingAPITest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Create a fresh instance for each test
        LoggingAPI::s_instance.reset();

        // Create a LockFreeQueue instance
        queue = std::make_shared<LockFreeQueue>(1024);
    }

    void TearDown() override
    {
        // Clean up the singleton
        LoggingAPI::s_instance.reset();
    }

    std::shared_ptr<LockFreeQueue> queue;
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
    EXPECT_TRUE(api.shutdown(false));
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

    EXPECT_TRUE(api.shutdown(false));
}

// Test appending log entry before initialization
TEST_F(LoggingAPITest, AppendBeforeInitialization)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    LogEntry entry(LogEntry::ActionType::READ, "location", "user", "subject");
    EXPECT_FALSE(api.append(entry));
}

// Test appending log entry after initialization
TEST_F(LoggingAPITest, AppendAfterInitialization2)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    EXPECT_TRUE(api.initialize(queue));
    LogEntry entry(LogEntry::ActionType::READ, "location", "user", "subject");

    EXPECT_TRUE(api.append(entry));
    EXPECT_TRUE(api.shutdown(false));
}

// Test convenience append method
TEST_F(LoggingAPITest, ConvenienceAppend)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    EXPECT_TRUE(api.initialize(queue));

    EXPECT_TRUE(api.append(LogEntry::ActionType::CREATE, "location", "user", "subject"));
    EXPECT_TRUE(api.shutdown(false));
}

// Test failed append (e.g., queue full scenario)
TEST_F(LoggingAPITest, FailedAppend)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    // Use a small queue to simulate failure
    auto smallQueue = std::make_shared<LockFreeQueue>(2);
    EXPECT_TRUE(api.initialize(smallQueue));

    LogEntry entry1(LogEntry::ActionType::READ, "location1", "user1", "subject1");
    LogEntry entry2(LogEntry::ActionType::READ, "location2", "user2", "subject2");

    EXPECT_TRUE(api.append(entry1));
    EXPECT_FALSE(api.append(entry2));

    EXPECT_TRUE(api.shutdown(false));
}

// Test shutdown without initialization
TEST_F(LoggingAPITest, ShutdownWithoutInitialization)
{
    LoggingAPI &api = LoggingAPI::getInstance();

    EXPECT_FALSE(api.shutdown(false));
}

// Test shutdown with wait for completion
TEST_F(LoggingAPITest, ShutdownWithWait)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    EXPECT_TRUE(api.initialize(queue));

    LogEntry entry(LogEntry::ActionType::READ, "location", "user", "subject");
    EXPECT_TRUE(api.append(entry));

    // Launch an asynchronous consumer that waits briefly before draining the queue.
    std::thread consumer([this]()
                         {
        std::this_thread::sleep_for(std::chrono::milliseconds(500)); // simulate delay
        LogEntry dummy;
        while(queue->dequeue(dummy)) {

        } });

    EXPECT_TRUE(api.shutdown(true));
    consumer.join();
    EXPECT_TRUE(queue->isEmpty());
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

    EXPECT_TRUE(api.shutdown(false));
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
                                 // Each thread appends 10 entries
                                 for (int j = 0; j < 10; j++) {
                                     LogEntry entry(
                                         LogEntry::ActionType::READ,
                                         "location_" + std::to_string(i),
                                         "user_" + std::to_string(i),
                                         "subject_" + std::to_string(j)
                                        );
                                     EXPECT_TRUE(api.append(entry));
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