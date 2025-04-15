#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "LockFreeBuffer.hpp"
#include "LogEntry.hpp"
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <future>
#include <random>

// Basic functionality tests
class LockFreeQueueBasicTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Create a new queue for each test
        queue = std::make_unique<LockFreeQueue>(TEST_QUEUE_SIZE);
    }

    void TearDown() override
    {
        queue.reset();
    }

    // Helper to create a test log entry
    LogEntry createTestEntry(int id)
    {
        return LogEntry(
            LogEntry::ActionType::READ,
            "data/location/" + std::to_string(id),
            "user" + std::to_string(id),
            "subject" + std::to_string(id % 10));
    }

    const size_t TEST_QUEUE_SIZE = 16; // Small size for testing
    std::unique_ptr<LockFreeQueue> queue;
};

// Test enqueue and dequeue operations
TEST_F(LockFreeQueueBasicTest, EnqueueDequeue)
{
    LogEntry entry = createTestEntry(1);
    LogEntry retrievedEntry;

    // Queue should be empty initially
    EXPECT_EQ(queue->size(), 0);

    // Enqueue one item
    EXPECT_TRUE(queue->enqueueBlocking(entry, std::chrono::milliseconds(100)));
    EXPECT_EQ(queue->size(), 1);

    // Dequeue the item
    EXPECT_TRUE(queue->dequeue(retrievedEntry));
    EXPECT_EQ(queue->size(), 0);

    // Verify the item matches
    EXPECT_EQ(retrievedEntry.getUserId(), entry.getUserId());
    EXPECT_EQ(retrievedEntry.getDataLocation(), entry.getDataLocation());
    EXPECT_EQ(retrievedEntry.getDataSubjectId(), entry.getDataSubjectId());
    EXPECT_EQ(retrievedEntry.getActionType(), entry.getActionType());
}

// Test enqueue until full
TEST_F(LockFreeQueueBasicTest, EnqueueUntilFull)
{
    // Fill the queue (capacity - 1 items)
    for (size_t i = 0; i < TEST_QUEUE_SIZE - 1; i++)
    {
        EXPECT_TRUE(queue->enqueueBlocking(createTestEntry(i), std::chrono::milliseconds(100)));
    }

    // One more should fail because head would be equal to tail (using a short timeout)
    EXPECT_FALSE(queue->enqueueBlocking(createTestEntry(TEST_QUEUE_SIZE - 1), std::chrono::milliseconds(100)));
}

// Test enqueue with consumer thread
TEST_F(LockFreeQueueBasicTest, EnqueueWithConsumer)
{
    // Fill the queue (capacity - 1 items)
    for (size_t i = 0; i < TEST_QUEUE_SIZE - 1; i++)
    {
        EXPECT_TRUE(queue->enqueueBlocking(createTestEntry(i), std::chrono::milliseconds(100)));
    }

    EXPECT_EQ(queue->size(), TEST_QUEUE_SIZE - 1);

    std::atomic<bool> producerSucceeded{false};
    std::thread producerThread([this, &producerSucceeded]()
                               {
        // will block until space is available
        if (queue->enqueueBlocking(createTestEntry(TEST_QUEUE_SIZE - 1), std::chrono::seconds(3))) {
            producerSucceeded.store(true);
        } });

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_FALSE(producerSucceeded.load());

    LogEntry retrievedEntry;
    std::thread consumerThread([this, &retrievedEntry]()
                               { EXPECT_TRUE(queue->dequeue(retrievedEntry)); });

    consumerThread.join();
    producerThread.join();

    EXPECT_TRUE(producerSucceeded.load());
    EXPECT_EQ(queue->size(), TEST_QUEUE_SIZE - 1);

    EXPECT_EQ(retrievedEntry.getActionType(), LogEntry::ActionType::READ);
    EXPECT_EQ(retrievedEntry.getDataLocation(), "data/location/0");
}

// Test dequeue from empty queue
TEST_F(LockFreeQueueBasicTest, DequeueFromEmpty)
{
    LogEntry entry;
    EXPECT_FALSE(queue->dequeue(entry));
}

// Test batch dequeue
TEST_F(LockFreeQueueBasicTest, BatchDequeue)
{
    const size_t numEntries = 5;

    // Enqueue several items
    for (size_t i = 0; i < numEntries; i++)
    {
        EXPECT_TRUE(queue->enqueueBlocking(createTestEntry(i), std::chrono::milliseconds(100)));
    }

    // Batch dequeue
    std::vector<LogEntry> entries;
    size_t count = queue->dequeueBatch(entries, numEntries);

    // Verify we got all items
    EXPECT_EQ(count, numEntries);
    EXPECT_EQ(entries.size(), numEntries);
    EXPECT_EQ(queue->size(), 0);

    // Verify entries match what we enqueued
    for (size_t i = 0; i < numEntries; i++)
    {
        EXPECT_EQ(entries[i].getDataLocation(), "data/location/" + std::to_string(i));
    }
}

// Test batch dequeue with more items requested than available
TEST_F(LockFreeQueueBasicTest, BatchDequeuePartial)
{
    const size_t numEntries = 3;
    const size_t requestSize = 5;

    // Enqueue a few items
    for (size_t i = 0; i < numEntries; i++)
    {
        EXPECT_TRUE(queue->enqueueBlocking(createTestEntry(i), std::chrono::milliseconds(100)));
    }

    // Try to dequeue more than available
    std::vector<LogEntry> entries;
    size_t count = queue->dequeueBatch(entries, requestSize);

    // Verify we got what was available
    EXPECT_EQ(count, numEntries);
    EXPECT_EQ(entries.size(), numEntries);
    EXPECT_EQ(queue->size(), 0);
}

// Test flush method
TEST_F(LockFreeQueueBasicTest, Flush)
{
    const size_t numEntries = 5;

    // Enqueue several items
    for (size_t i = 0; i < numEntries; i++)
    {
        EXPECT_TRUE(queue->enqueueBlocking(createTestEntry(i), std::chrono::milliseconds(100)));
    }

    // Start a thread to dequeue all items
    std::thread consumer([&]
                         {
        std::vector<LogEntry> entries;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        queue->dequeueBatch(entries, numEntries); });

    // Flush should wait until queue is empty
    EXPECT_TRUE(queue->flush());
    EXPECT_EQ(queue->size(), 0);

    consumer.join();
}

// Thread safety tests
class LockFreeQueueThreadTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Create a new queue for each test with larger capacity
        queue = std::make_unique<LockFreeQueue>(QUEUE_CAPACITY);
    }

    void TearDown() override
    {
        queue.reset();
    }

    // Helper to create a test log entry
    LogEntry createTestEntry(int id)
    {
        return LogEntry(
            LogEntry::ActionType::READ,
            "data/location/" + std::to_string(id),
            "user" + std::to_string(id),
            "subject" + std::to_string(id % 10));
    }

    const size_t QUEUE_CAPACITY = 4096;
    std::unique_ptr<LockFreeQueue> queue;
};

// Test multiple producers, single consumer
TEST_F(LockFreeQueueThreadTest, MultipleProducersSingleConsumer)
{
    const int NUM_PRODUCERS = 4;
    const int ENTRIES_PER_PRODUCER = 1000;
    const int TOTAL_ENTRIES = NUM_PRODUCERS * ENTRIES_PER_PRODUCER;

    std::atomic<int> totalEnqueued(0);
    std::atomic<int> totalDequeued(0);

    // Start consumer thread
    std::thread consumer([&]
                         {
        LogEntry entry;
        while (totalDequeued.load() < TOTAL_ENTRIES) {
            if (queue->dequeue(entry)) {
                totalDequeued++;
            } else {
                std::this_thread::yield();
            }
        } });

    // Start producer threads
    std::vector<std::thread> producers;
    for (int i = 0; i < NUM_PRODUCERS; i++)
    {
        producers.emplace_back([&, i]
                               {
            for (int j = 0; j < ENTRIES_PER_PRODUCER; j++) {
                int id = i * ENTRIES_PER_PRODUCER + j;
                LogEntry entry = createTestEntry(id);

                // Try until enqueue succeeds
                while (!queue->enqueueBlocking(entry, std::chrono::milliseconds(100)))
                {
                    std::this_thread::yield();
                }

                totalEnqueued++;
            } });
    }

    // Wait for producers to finish
    for (auto &t : producers)
    {
        t.join();
    }

    // Wait for consumer
    consumer.join();

    // Verify counts
    EXPECT_EQ(totalEnqueued.load(), TOTAL_ENTRIES);
    EXPECT_EQ(totalDequeued.load(), TOTAL_ENTRIES);
    EXPECT_EQ(queue->size(), 0);
}

// Test single producer, multiple consumers
TEST_F(LockFreeQueueThreadTest, SingleProducerMultipleConsumers)
{
    const int NUM_CONSUMERS = 4;
    const int TOTAL_ENTRIES = 10000;

    std::atomic<int> totalDequeued(0);

    // Start consumer threads
    std::vector<std::thread> consumers;
    for (int i = 0; i < NUM_CONSUMERS; i++)
    {
        consumers.emplace_back([&]
                               {
            LogEntry entry;
            while (totalDequeued.load() < TOTAL_ENTRIES) {
                if (queue->dequeue(entry)) {
                    totalDequeued++;
                } else {
                    std::this_thread::yield();
                }
            } });
    }

    // Producer thread
    for (int i = 0; i < TOTAL_ENTRIES; i++)
    {
        LogEntry entry = createTestEntry(i);

        // Try until enqueue succeeds
        while (!queue->enqueueBlocking(entry, std::chrono::milliseconds(100)))
        {
            std::this_thread::yield();
        }
    }

    // Wait for consumers
    for (auto &t : consumers)
    {
        t.join();
    }

    // Verify counts
    EXPECT_EQ(totalDequeued.load(), TOTAL_ENTRIES);
    EXPECT_EQ(queue->size(), 0);
}

// Test batch dequeue with multiple threads
TEST_F(LockFreeQueueThreadTest, BatchDequeueMultipleThreads)
{
    const int NUM_PRODUCERS = 4;
    const int NUM_CONSUMERS = 2;
    const int ENTRIES_PER_PRODUCER = 1000;
    const int TOTAL_ENTRIES = NUM_PRODUCERS * ENTRIES_PER_PRODUCER;
    const int BATCH_SIZE = 100;

    std::atomic<int> totalEnqueued(0);
    std::atomic<int> totalDequeued(0);

    // Start consumer threads
    std::vector<std::thread> consumers;
    for (int i = 0; i < NUM_CONSUMERS; i++)
    {
        consumers.emplace_back([&]
                               {
            std::vector<LogEntry> entries;
            while (totalDequeued.load() < TOTAL_ENTRIES) {
                size_t count = queue->dequeueBatch(entries, BATCH_SIZE);
                if (count > 0) {
                    totalDequeued += count;
                } else {
                    std::this_thread::yield();
                }
            } });
    }

    // Start producer threads
    std::vector<std::thread> producers;
    for (int i = 0; i < NUM_PRODUCERS; i++)
    {
        producers.emplace_back([&, i]
                               {
            for (int j = 0; j < ENTRIES_PER_PRODUCER; j++) {
                int id = i * ENTRIES_PER_PRODUCER + j;
                LogEntry entry = createTestEntry(id);

                // Try until enqueue succeeds
                while (!queue->enqueueBlocking(entry, std::chrono::milliseconds(100)))
                {
                    std::this_thread::yield();
                }

                totalEnqueued++;
            } });
    }

    // Wait for producers to finish
    for (auto &t : producers)
    {
        t.join();
    }

    // Wait for consumers
    for (auto &t : consumers)
    {
        t.join();
    }

    // Verify counts
    EXPECT_EQ(totalEnqueued.load(), TOTAL_ENTRIES);
    EXPECT_EQ(totalDequeued.load(), TOTAL_ENTRIES);
    EXPECT_EQ(queue->size(), 0);
}

// Stress test with random operations
TEST_F(LockFreeQueueThreadTest, RandomizedStressTest)
{
    const int NUM_THREADS = 8;
    const int OPS_PER_THREAD = 5000;

    std::atomic<int> totalEnqueued(0);
    std::atomic<int> totalDequeued(0);

    // Start worker threads
    std::vector<std::thread> threads;
    for (int i = 0; i < NUM_THREADS; i++)
    {
        threads.emplace_back([&, i]
                             {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(0, 1);  // 0 for enqueue, 1 for dequeue

            for (int j = 0; j < OPS_PER_THREAD; j++) {
                int id = i * OPS_PER_THREAD + j;

                if (dis(gen) == 0 || totalDequeued.load() >= totalEnqueued.load()) {
                    // Enqueue
                    LogEntry entry = createTestEntry(id);
                    if (queue->enqueueBlocking(entry, std::chrono::milliseconds(100)))
                    {
                        totalEnqueued++;
                    }
                } else {
                    // Dequeue
                    LogEntry entry;
                    if (queue->dequeue(entry)) {
                        totalDequeued++;
                    }
                }
            } });
    }

    // Wait for all threads
    for (auto &t : threads)
    {
        t.join();
    }

    // Verify that enqueued >= dequeued and size matches the difference
    EXPECT_GE(totalEnqueued.load(), totalDequeued.load());
    EXPECT_EQ(queue->size(), totalEnqueued.load() - totalDequeued.load());

    // Dequeue remaining entries
    LogEntry entry;
    while (queue->dequeue(entry))
    {
        totalDequeued++;
    }

    // Final verification
    EXPECT_EQ(totalEnqueued.load(), totalDequeued.load());
    EXPECT_EQ(queue->size(), 0);
}

// Test queue capacity enforcement
TEST_F(LockFreeQueueThreadTest, CapacityEnforcement)
{
    const size_t SMALL_CAPACITY = 128;
    auto smallQueue = std::make_unique<LockFreeQueue>(SMALL_CAPACITY);

    // Fill the queue
    size_t enqueued = 0;
    while (enqueued < SMALL_CAPACITY - 1)
    {
        if (smallQueue->enqueueBlocking(createTestEntry(enqueued), std::chrono::milliseconds(100)))
        {
            enqueued++;
        }
    }

    // One more should fail
    EXPECT_FALSE(smallQueue->enqueueBlocking(createTestEntry(enqueued), std::chrono::milliseconds(100)));
    enqueued++;

    // Dequeue one and try again
    LogEntry entry;
    EXPECT_TRUE(smallQueue->dequeue(entry));
    EXPECT_TRUE(smallQueue->enqueueBlocking(createTestEntry(enqueued)));
}

// Test timed operations
class LockFreeQueueTimingTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Create a new queue for each test
        queue = std::make_unique<LockFreeQueue>(QUEUE_CAPACITY);
    }

    void TearDown() override
    {
        queue.reset();
    }

    const size_t QUEUE_CAPACITY = 1024;
    std::unique_ptr<LockFreeQueue> queue;
};

// Test flush timeout
TEST_F(LockFreeQueueTimingTest, FlushWithTimeout)
{
    // Enqueue some items
    for (int i = 0; i < 10; i++)
    {
        queue->enqueueBlocking(LogEntry(
                                   LogEntry::ActionType::READ,
                                   "data/location/" + std::to_string(i),
                                   "user",
                                   "subject"),
                               std::chrono::milliseconds(100));
    }

    // Start a future to call flush
    auto future = std::async(std::launch::async, [&]
                             { return queue->flush(); });

    // Start a thread to dequeue after a delay
    std::thread consumer([&]
                         {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        std::vector<LogEntry> entries;
        queue->dequeueBatch(entries, 10); });

    // Flush should complete when queue is emptied
    auto status = future.wait_for(std::chrono::milliseconds(500));
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_TRUE(future.get());

    consumer.join();
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}