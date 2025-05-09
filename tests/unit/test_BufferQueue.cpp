#include <gtest/gtest.h>
#include "BufferQueue.hpp"
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <future>
#include <random>

// Basic functionality tests
class BufferQueueBasicTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Create a new queue for each test
        queue = std::make_unique<BufferQueue>(TEST_QUEUE_SIZE);
    }

    void TearDown() override
    {
        queue.reset();
    }

    // Helper to create a test queue item with log entry
    QueueItem createTestItem(int id)
    {
        QueueItem item;
        item.entry = LogEntry(
            LogEntry::ActionType::READ,
            "data/location/" + std::to_string(id),
            "user" + std::to_string(id),
            "subject" + std::to_string(id % 10));
        return item;
    }

    // Helper to create a test queue item with log entry and target filename
    QueueItem createTestItemWithTarget(int id, const std::string &filename)
    {
        QueueItem item = createTestItem(id);
        item.targetFilename = filename;
        return item;
    }

    const size_t TEST_QUEUE_SIZE = 16; // Small size for testing
    std::unique_ptr<BufferQueue> queue;
};

// Test enqueue and dequeue operations
TEST_F(BufferQueueBasicTest, EnqueueDequeue)
{
    QueueItem item = createTestItem(1);
    QueueItem retrievedItem;

    // Queue should be empty initially
    EXPECT_EQ(queue->size(), 0);

    // Enqueue one item
    EXPECT_TRUE(queue->enqueueBlocking(item, std::chrono::milliseconds(100)));
    EXPECT_EQ(queue->size(), 1);

    // Dequeue the item
    EXPECT_TRUE(queue->dequeue(retrievedItem));
    EXPECT_EQ(queue->size(), 0);

    // Verify the item matches
    EXPECT_EQ(retrievedItem.entry.getUserId(), item.entry.getUserId());
    EXPECT_EQ(retrievedItem.entry.getDataLocation(), item.entry.getDataLocation());
    EXPECT_EQ(retrievedItem.entry.getDataSubjectId(), item.entry.getDataSubjectId());
    EXPECT_EQ(retrievedItem.entry.getActionType(), item.entry.getActionType());
    EXPECT_EQ(retrievedItem.targetFilename, item.targetFilename);
}

// Test enqueue until full - Modified for dynamic queue
TEST_F(BufferQueueBasicTest, EnqueueUntilFull)
{
    // New test focuses on verifying queue doesn't fail under load
    const size_t MANY_ITEMS = TEST_QUEUE_SIZE * 3;

    // Fill the queue with many more items than initial capacity
    for (size_t i = 0; i < MANY_ITEMS; i++)
    {
        EXPECT_TRUE(queue->enqueueBlocking(createTestItem(i), std::chrono::milliseconds(100)));
    }

    EXPECT_EQ(queue->size(), MANY_ITEMS);
}

// Test enqueue with consumer thread - Modified for dynamic queue
TEST_F(BufferQueueBasicTest, EnqueueWithConsumer)
{
    // Fill the queue with items
    for (size_t i = 0; i < TEST_QUEUE_SIZE * 2; i++) // More than initial capacity
    {
        EXPECT_TRUE(queue->enqueueBlocking(createTestItem(i), std::chrono::milliseconds(100)));
    }

    EXPECT_EQ(queue->size(), TEST_QUEUE_SIZE * 2);

    // Test concurrent enqueue with consumer
    std::atomic<bool> producerSucceeded{false};
    std::thread producerThread([this, &producerSucceeded]()
                               {
        if (queue->enqueueBlocking(createTestItem(99999), std::chrono::seconds(3))) {
            producerSucceeded.store(true);
        } });

    // Producer should succeed immediately since queue can grow
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(producerSucceeded.load());

    QueueItem retrievedItem;
    std::thread consumerThread([this, &retrievedItem]()
                               { EXPECT_TRUE(queue->dequeue(retrievedItem)); });

    consumerThread.join();
    producerThread.join();

    EXPECT_TRUE(producerSucceeded.load());
    EXPECT_EQ(queue->size(), TEST_QUEUE_SIZE * 2);

    EXPECT_EQ(retrievedItem.entry.getActionType(), LogEntry::ActionType::READ);
    EXPECT_EQ(retrievedItem.entry.getDataLocation(), "data/location/0");
}

// Test dequeue from empty queue
TEST_F(BufferQueueBasicTest, DequeueFromEmpty)
{
    QueueItem item;
    EXPECT_FALSE(queue->dequeue(item));
}

// Test batch dequeue
TEST_F(BufferQueueBasicTest, BatchDequeue)
{
    const size_t numEntries = 5;

    // Enqueue several items
    for (size_t i = 0; i < numEntries; i++)
    {
        EXPECT_TRUE(queue->enqueueBlocking(createTestItem(i), std::chrono::milliseconds(100)));
    }

    // Batch dequeue
    std::vector<QueueItem> items;
    size_t count = queue->dequeueBatch(items, numEntries);

    // Verify we got all items
    EXPECT_EQ(count, numEntries);
    EXPECT_EQ(items.size(), numEntries);
    EXPECT_EQ(queue->size(), 0);

    // Verify entries match what we enqueued
    for (size_t i = 0; i < numEntries; i++)
    {
        EXPECT_EQ(items[i].entry.getDataLocation(), "data/location/" + std::to_string(i));
    }
}

// Test batch dequeue with more items requested than available
TEST_F(BufferQueueBasicTest, BatchDequeuePartial)
{
    const size_t numEntries = 3;
    const size_t requestSize = 5;

    // Enqueue a few items
    for (size_t i = 0; i < numEntries; i++)
    {
        EXPECT_TRUE(queue->enqueueBlocking(createTestItem(i), std::chrono::milliseconds(100)));
    }

    // Try to dequeue more than available
    std::vector<QueueItem> items;
    size_t count = queue->dequeueBatch(items, requestSize);

    // Verify we got what was available
    EXPECT_EQ(count, numEntries);
    EXPECT_EQ(items.size(), numEntries);
    EXPECT_EQ(queue->size(), 0);
}

// Test batch enqueue functionality
TEST_F(BufferQueueBasicTest, BatchEnqueue)
{
    const size_t numEntries = 5;
    std::vector<QueueItem> itemsToEnqueue;

    for (size_t i = 0; i < numEntries; i++)
    {
        itemsToEnqueue.push_back(createTestItem(i));
    }

    EXPECT_TRUE(queue->enqueueBatchBlocking(itemsToEnqueue));
    EXPECT_EQ(queue->size(), numEntries);

    std::vector<QueueItem> retrievedItems;
    size_t dequeued = queue->dequeueBatch(retrievedItems, numEntries);

    EXPECT_EQ(dequeued, numEntries);
    EXPECT_EQ(retrievedItems.size(), numEntries);

    for (size_t i = 0; i < numEntries; i++)
    {
        EXPECT_EQ(retrievedItems[i].entry.getDataLocation(), itemsToEnqueue[i].entry.getDataLocation());
        EXPECT_EQ(retrievedItems[i].entry.getUserId(), itemsToEnqueue[i].entry.getUserId());
    }

    EXPECT_EQ(queue->size(), 0);
}

// Test batch enqueue with growing queue - Modified for dynamic queue
TEST_F(BufferQueueBasicTest, BatchEnqueueWhenAlmostFull)
{
    // Fill the queue beyond initial capacity
    for (size_t i = 0; i < TEST_QUEUE_SIZE * 2; i++)
    {
        EXPECT_TRUE(queue->enqueueBlocking(createTestItem(i), std::chrono::milliseconds(100)));
    }

    std::vector<QueueItem> smallBatch;
    for (size_t i = 0; i < 3; i++)
    {
        smallBatch.push_back(createTestItem(100 + i));
    }
    EXPECT_TRUE(queue->enqueueBatchBlocking(smallBatch));

    std::vector<QueueItem> largeBatch;
    for (size_t i = 0; i < 4; i++)
    {
        largeBatch.push_back(createTestItem(200 + i));
    }
    // This should still succeed with dynamic queue
    EXPECT_TRUE(queue->enqueueBatchBlocking(largeBatch, std::chrono::milliseconds(500)));

    EXPECT_EQ(queue->size(), TEST_QUEUE_SIZE * 2 + 7);
}

// Test batch enqueue with dynamic growth - Modified for dynamic queue
TEST_F(BufferQueueBasicTest, BatchEnqueueBlocking)
{
    // Fill the queue with items
    for (size_t i = 0; i < TEST_QUEUE_SIZE + 10; i++)
    {
        EXPECT_TRUE(queue->enqueueBlocking(createTestItem(i), std::chrono::milliseconds(100)));
    }

    std::vector<QueueItem> batch;
    for (size_t i = 0; i < 3; i++)
    {
        batch.push_back(createTestItem(100 + i));
    }

    // Enqueue will succeed immediately since queue grows dynamically
    std::atomic<bool> producerSucceeded{false};
    std::thread producerThread([this, &batch, &producerSucceeded]()
                               {
        if (queue->enqueueBatchBlocking(batch, std::chrono::seconds(3))) {
            producerSucceeded.store(true);
        } });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(producerSucceeded.load());

    producerThread.join();

    EXPECT_TRUE(producerSucceeded.load());
    EXPECT_EQ(queue->size(), TEST_QUEUE_SIZE + 13);
}

// Test flush method
TEST_F(BufferQueueBasicTest, Flush)
{
    const size_t numEntries = 5;

    // Enqueue several items
    for (size_t i = 0; i < numEntries; i++)
    {
        EXPECT_TRUE(queue->enqueueBlocking(createTestItem(i), std::chrono::milliseconds(100)));
    }

    // Start a thread to dequeue all items
    std::thread consumer([&]
                         {
        std::vector<QueueItem> items;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        queue->dequeueBatch(items, numEntries); });

    // Flush should wait until queue is empty
    EXPECT_TRUE(queue->flush());
    EXPECT_EQ(queue->size(), 0);

    consumer.join();
}

// Test for QueueItem with targetFilename
TEST_F(BufferQueueBasicTest, QueueItemWithTargetFilename)
{
    // Create items with target filenames
    QueueItem item1 = createTestItemWithTarget(1, "file1.log");
    QueueItem item2 = createTestItemWithTarget(2, "file2.log");
    QueueItem item3 = createTestItem(3); // No target filename

    // Enqueue items
    EXPECT_TRUE(queue->enqueueBlocking(item1, std::chrono::milliseconds(100)));
    EXPECT_TRUE(queue->enqueueBlocking(item2, std::chrono::milliseconds(100)));
    EXPECT_TRUE(queue->enqueueBlocking(item3, std::chrono::milliseconds(100)));

    // Dequeue and verify
    QueueItem retrievedItem1, retrievedItem2, retrievedItem3;

    EXPECT_TRUE(queue->dequeue(retrievedItem1));
    EXPECT_TRUE(queue->dequeue(retrievedItem2));
    EXPECT_TRUE(queue->dequeue(retrievedItem3));

    // Check targetFilename is preserved correctly
    EXPECT_TRUE(retrievedItem1.targetFilename.has_value());
    EXPECT_EQ(*retrievedItem1.targetFilename, "file1.log");

    EXPECT_TRUE(retrievedItem2.targetFilename.has_value());
    EXPECT_EQ(*retrievedItem2.targetFilename, "file2.log");

    EXPECT_FALSE(retrievedItem3.targetFilename.has_value());
}

// Thread safety tests
class BufferQueueThreadTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Create a new queue for each test with larger capacity
        queue = std::make_unique<BufferQueue>(QUEUE_CAPACITY);
    }

    void TearDown() override
    {
        queue.reset();
    }

    // Helper to create a test queue item with log entry
    QueueItem createTestItem(int id)
    {
        QueueItem item;
        item.entry = LogEntry(
            LogEntry::ActionType::READ,
            "data/location/" + std::to_string(id),
            "user" + std::to_string(id),
            "subject" + std::to_string(id % 10));
        return item;
    }

    const size_t QUEUE_CAPACITY = 4096;
    std::unique_ptr<BufferQueue> queue;
};

// Test multiple producers, single consumer
TEST_F(BufferQueueThreadTest, MultipleProducersSingleConsumer)
{
    const int NUM_PRODUCERS = 4;
    const int ENTRIES_PER_PRODUCER = 1000;
    const int TOTAL_ENTRIES = NUM_PRODUCERS * ENTRIES_PER_PRODUCER;

    std::atomic<int> totalEnqueued(0);
    std::atomic<int> totalDequeued(0);

    // Start consumer thread
    std::thread consumer([&]
                         {
        QueueItem item;
        while (totalDequeued.load() < TOTAL_ENTRIES) {
            if (queue->dequeue(item)) {
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
                QueueItem item = createTestItem(id);

                // Try until enqueue succeeds
                while (!queue->enqueueBlocking(item, std::chrono::milliseconds(100)))
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
TEST_F(BufferQueueThreadTest, SingleProducerMultipleConsumers)
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
            QueueItem item;
            while (totalDequeued.load() < TOTAL_ENTRIES) {
                if (queue->dequeue(item)) {
                    totalDequeued++;
                } else {
                    std::this_thread::yield();
                }
            } });
    }

    // Producer thread
    for (int i = 0; i < TOTAL_ENTRIES; i++)
    {
        QueueItem item = createTestItem(i);

        // Try until enqueue succeeds
        while (!queue->enqueueBlocking(item, std::chrono::milliseconds(100)))
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

// Test multiple producers with batch enqueue
TEST_F(BufferQueueThreadTest, MultipleBatchProducers)
{
    const int NUM_PRODUCERS = 4;
    const int BATCHES_PER_PRODUCER = 50;
    const int ENTRIES_PER_BATCH = 20;
    const int TOTAL_ENTRIES = NUM_PRODUCERS * BATCHES_PER_PRODUCER * ENTRIES_PER_BATCH;

    std::atomic<int> totalEnqueued(0);
    std::atomic<int> totalDequeued(0);

    std::thread consumer([&]()
                         {
        std::vector<QueueItem> items;
        while (totalDequeued.load() < TOTAL_ENTRIES) {
            size_t count = queue->dequeueBatch(items, 50);
            if (count > 0) {
                totalDequeued += count;
            } else {
                std::this_thread::yield();
            }
        } });

    std::vector<std::thread> producers;
    for (int i = 0; i < NUM_PRODUCERS; i++)
    {
        producers.emplace_back([&, i]()
                               {
            std::vector<QueueItem> batchToEnqueue;

            for (int b = 0; b < BATCHES_PER_PRODUCER; b++) {
                batchToEnqueue.clear();
                for (int j = 0; j < ENTRIES_PER_BATCH; j++) {
                    int id = (i * BATCHES_PER_PRODUCER * ENTRIES_PER_BATCH) +
                             (b * ENTRIES_PER_BATCH) + j;
                    batchToEnqueue.push_back(createTestItem(id));
                }

                queue->enqueueBatchBlocking(batchToEnqueue, std::chrono::milliseconds(500));

                totalEnqueued += ENTRIES_PER_BATCH;
            } });
    }

    for (auto &t : producers)
    {
        t.join();
    }

    consumer.join();

    EXPECT_EQ(totalEnqueued.load(), TOTAL_ENTRIES);
    EXPECT_EQ(totalDequeued.load(), TOTAL_ENTRIES);
    EXPECT_EQ(queue->size(), 0);
}

// Test mixed batch and single item operations
TEST_F(BufferQueueThreadTest, MixedBatchOperations)
{
    const int NUM_THREADS = 6;
    const int OPS_PER_THREAD = 200;
    const int MAX_BATCH_SIZE = 10;

    std::atomic<int> totalEnqueued(0);
    std::atomic<int> totalDequeued(0);

    std::vector<std::thread> threads;
    for (int i = 0; i < NUM_THREADS; i++)
    {
        threads.emplace_back([&, i]()
                             {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> opType(0, 3);  // 0-1: single enqueue, 2: batch enqueue, 3: dequeue
            std::uniform_int_distribution<> batchSize(2, MAX_BATCH_SIZE);

            for (int j = 0; j < OPS_PER_THREAD; j++) {
                int id = i * OPS_PER_THREAD + j;
                int op = opType(gen);

                if (op <= 1 || totalDequeued.load() >= totalEnqueued.load()) {
                    // Single enqueue
                    QueueItem item = createTestItem(id);
                    if (queue->enqueueBlocking(item, std::chrono::milliseconds(50))) {
                        totalEnqueued++;
                    }
                } else if (op == 2) {
                    // Batch enqueue
                    int size = batchSize(gen);
                    std::vector<QueueItem> batch;
                    for (int k = 0; k < size; k++) {
                        batch.push_back(createTestItem(id * 1000 + k));
                    }

                    if (queue->enqueueBatchBlocking(batch, std::chrono::milliseconds(50))) {
                        totalEnqueued += size;
                    }
                } else {
                    if (gen() % 2 == 0) {
                        // Single dequeue
                        QueueItem item;
                        if (queue->dequeue(item)) {
                            totalDequeued++;
                        }
                    } else {
                        // Batch dequeue
                        std::vector<QueueItem> items;
                        size_t count = queue->dequeueBatch(items, batchSize(gen));
                        if (count > 0) {
                            totalDequeued += count;
                        }
                    }
                }
            } });
    }

    for (auto &t : threads)
    {
        t.join();
    }

    // Verify that enqueued >= dequeued and size matches the difference
    EXPECT_GE(totalEnqueued.load(), totalDequeued.load());
    EXPECT_EQ(queue->size(), totalEnqueued.load() - totalDequeued.load());

    // Dequeue remaining entries
    std::vector<QueueItem> items;
    while (queue->dequeueBatch(items, MAX_BATCH_SIZE) > 0)
    {
        totalDequeued += items.size();
    }

    EXPECT_EQ(totalEnqueued.load(), totalDequeued.load());
    EXPECT_EQ(queue->size(), 0);
}

// Test batch dequeue with multiple threads
TEST_F(BufferQueueThreadTest, BatchDequeueMultipleThreads)
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
            std::vector<QueueItem> items;
            while (totalDequeued.load() < TOTAL_ENTRIES) {
                size_t count = queue->dequeueBatch(items, BATCH_SIZE);
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
                QueueItem item = createTestItem(id);

                // Try until enqueue succeeds
                while (!queue->enqueueBlocking(item, std::chrono::milliseconds(100)))
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
TEST_F(BufferQueueThreadTest, RandomizedStressTest)
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
                    QueueItem item = createTestItem(id);
                    if (queue->enqueueBlocking(item, std::chrono::milliseconds(100)))
                    {
                        totalEnqueued++;
                    }
                } else {
                    // Dequeue
                    QueueItem item;
                    if (queue->dequeue(item)) {
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
    QueueItem item;
    while (queue->dequeue(item))
    {
        totalDequeued++;
    }

    // Final verification
    EXPECT_EQ(totalEnqueued.load(), totalDequeued.load());
    EXPECT_EQ(queue->size(), 0);
}

// Test dynamic queue growth - Modified for dynamic queue
TEST_F(BufferQueueThreadTest, DynamicQueueGrowth)
{
    const size_t SMALL_CAPACITY = 128;
    auto smallQueue = std::make_unique<BufferQueue>(SMALL_CAPACITY);

    // Fill the queue well beyond initial capacity
    size_t enqueued = 0;
    const size_t OVER_CAPACITY = SMALL_CAPACITY * 3;

    while (enqueued < OVER_CAPACITY)
    {
        EXPECT_TRUE(smallQueue->enqueueBlocking(createTestItem(enqueued), std::chrono::milliseconds(100)));
        enqueued++;
    }

    EXPECT_EQ(smallQueue->size(), OVER_CAPACITY);

    // Verify we can still enqueue more
    EXPECT_TRUE(smallQueue->enqueueBlocking(createTestItem(enqueued), std::chrono::milliseconds(100)));
    EXPECT_EQ(smallQueue->size(), OVER_CAPACITY + 1);
}

// Test timed operations
class BufferQueueTimingTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Create a new queue for each test
        queue = std::make_unique<BufferQueue>(QUEUE_CAPACITY);
    }

    void TearDown() override
    {
        queue.reset();
    }

    // Helper to create a test queue item with log entry
    QueueItem createTestItem(int id)
    {
        QueueItem item;
        item.entry = LogEntry(
            LogEntry::ActionType::READ,
            "data/location/" + std::to_string(id),
            "user" + std::to_string(id),
            "subject" + std::to_string(id % 10));
        return item;
    }

    const size_t QUEUE_CAPACITY = 1024;
    std::unique_ptr<BufferQueue> queue;
};

// Test flush timeout
TEST_F(BufferQueueTimingTest, FlushWithTimeout)
{
    // Enqueue some items
    for (int i = 0; i < 10; i++)
    {
        QueueItem item;
        item.entry = LogEntry(
            LogEntry::ActionType::READ,
            "data/location/" + std::to_string(i),
            "user",
            "subject");
        queue->enqueueBlocking(item, std::chrono::milliseconds(100));
    }

    // Start a future to call flush
    auto future = std::async(std::launch::async, [&]
                             { return queue->flush(); });

    // Start a thread to dequeue after a delay
    std::thread consumer([&]
                         {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        std::vector<QueueItem> items;
        queue->dequeueBatch(items, 10); });

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