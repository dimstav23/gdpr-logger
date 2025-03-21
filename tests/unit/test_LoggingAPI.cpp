#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "LoggingAPI.hpp"
#include <chrono>
#include <thread>

class MockLogQueue : public ILogQueue
{
public:
    MOCK_METHOD(bool, enqueue, (const LogEntry &entry), (override));
    MOCK_METHOD(bool, isEmpty, (), (const, override));
    MOCK_METHOD(bool, flush, (), (override));
};

class LoggingAPITest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Create a fresh instance for each test
        LoggingAPI::s_instance.reset();

        // Create a mock log queue
        mockQueue = std::make_shared<::testing::NiceMock<MockLogQueue>>();
    }

    void TearDown() override
    {
        // Clean up the singleton
        LoggingAPI::s_instance.reset();
    }

    std::shared_ptr<MockLogQueue> mockQueue;
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

    EXPECT_TRUE(api.initialize(mockQueue));
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

    EXPECT_TRUE(api.initialize(mockQueue));
    EXPECT_FALSE(api.initialize(mockQueue));

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
    EXPECT_TRUE(api.initialize(mockQueue));
    LogEntry entry(LogEntry::ActionType::READ, "location", "user", "subject");

    // Set expectation on mock with argument capture
    LogEntry capturedEntry(LogEntry::ActionType::CREATE, "", "", "");
    EXPECT_CALL(*mockQueue, enqueue(::testing::_))
        .WillOnce(::testing::DoAll(
            ::testing::SaveArg<0>(&capturedEntry),
            ::testing::Return(true)));

    EXPECT_TRUE(api.append(entry));

    EXPECT_EQ(capturedEntry.getActionType(), entry.getActionType());
    EXPECT_EQ(capturedEntry.getDataLocation(), entry.getDataLocation());
    EXPECT_EQ(capturedEntry.getUserId(), entry.getUserId());
    EXPECT_EQ(capturedEntry.getDataSubjectId(), entry.getDataSubjectId());

    EXPECT_TRUE(api.shutdown(false));
}

// Test convenience append method
TEST_F(LoggingAPITest, ConvenienceAppend)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    EXPECT_TRUE(api.initialize(mockQueue));

    // Set expectation on mock
    EXPECT_CALL(*mockQueue, enqueue(::testing::_))
        .WillOnce(::testing::Return(true));

    EXPECT_TRUE(api.append(LogEntry::ActionType::CREATE, "location", "user", "subject"));

    EXPECT_TRUE(api.shutdown(false));
}

// Test failed append
TEST_F(LoggingAPITest, FailedAppend)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    EXPECT_TRUE(api.initialize(mockQueue));

    // Set expectation on mock to fail
    EXPECT_CALL(*mockQueue, enqueue(::testing::_))
        .WillOnce(::testing::Return(false));

    LogEntry entry(LogEntry::ActionType::READ, "location", "user", "subject");
    EXPECT_FALSE(api.append(entry));

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
    EXPECT_TRUE(api.initialize(mockQueue));

    // Set expectation on mock
    EXPECT_CALL(*mockQueue, flush())
        .WillOnce(::testing::Return(true));

    EXPECT_TRUE(api.shutdown(true));
}

// Test shutdown with wait fails
TEST_F(LoggingAPITest, ShutdownWithWaitFails)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    EXPECT_TRUE(api.initialize(mockQueue));

    // Set expectation on mock to fail
    EXPECT_CALL(*mockQueue, flush())
        .WillOnce(::testing::Return(false));

    EXPECT_FALSE(api.shutdown(true));
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
    EXPECT_TRUE(api.initialize(mockQueue));

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

/*
// Test thread safety of API operations
TEST_F(LoggingAPITest, ThreadSafetyOperations)
{
    LoggingAPI &api = LoggingAPI::getInstance();
    EXPECT_TRUE(api.initialize(mockQueue));

    // Allow unlimited calls to enqueue
    EXPECT_CALL(*mockQueue, enqueue(::testing::_))
        .WillRepeatedly(::testing::Return(true));

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

    EXPECT_TRUE(api.shutdown(true));
}
*/

// Main function that runs all the tests
int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}