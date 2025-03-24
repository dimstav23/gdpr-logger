#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "Writer.hpp"
#include <chrono>
#include <thread>

class MockLogQueue : public ILogQueue
{
public:
    MOCK_METHOD(bool, enqueue, (const LogEntry &entry), (override));
    MOCK_METHOD(bool, dequeue, (LogEntry & entry), (override));
    MOCK_METHOD(size_t, dequeueBatch, (std::vector<LogEntry> & entries, size_t maxEntries), (override));
    MOCK_METHOD(bool, flush, (), (override));
    MOCK_METHOD(size_t, size, (), (const, override));
    MOCK_METHOD(bool, isEmpty, (), (const, override));
    virtual ~MockLogQueue() = default;
};

class WriterTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        mockQueue = std::make_unique<::testing::NiceMock<MockLogQueue>>();
    }

    void TearDown() override
    {
        if (writer)
        {
            writer->stop();
        }
    }

    std::unique_ptr<::testing::NiceMock<MockLogQueue>> mockQueue;
    std::unique_ptr<Writer> writer;
};

// Test that the writer starts and stops correctly
TEST_F(WriterTest, StartAndStop)
{
    writer = std::make_unique<Writer>(*mockQueue);
    EXPECT_FALSE(writer->isRunning());

    writer->start();
    EXPECT_TRUE(writer->isRunning());

    writer->stop();
    EXPECT_FALSE(writer->isRunning());
}

// Test multiple start calls
TEST_F(WriterTest, MultipleStartCalls)
{
    writer = std::make_unique<Writer>(*mockQueue);
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

    EXPECT_CALL(*mockQueue, dequeueBatch)
        .WillOnce(::testing::DoAll(
            ::testing::SetArgReferee<0>(testEntries),
            ::testing::Return(testEntries.size())))
        .WillRepeatedly(::testing::Return(0));

    writer = std::make_unique<Writer>(*mockQueue, 3);
    writer->start();
    // Give some time to verify it handles processing gracefully
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    writer->stop();
}

// Test behavior when queue is empty
TEST_F(WriterTest, EmptyQueue)
{
    EXPECT_CALL(*mockQueue, dequeueBatch)
        .WillRepeatedly(::testing::Return(0));

    writer = std::make_unique<Writer>(*mockQueue);

    writer->start();

    // Give some time to verify it handles empty queue gracefully
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    writer->stop();
}