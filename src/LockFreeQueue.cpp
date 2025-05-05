#include "LockFreeQueue.hpp"
#include <algorithm>
#include <thread>
#include <iostream>
#include <chrono>
#include <cmath>

// Find the next power of 2 greater than or equal to n
static size_t nextPowerOf2(size_t n)
{
    if (n == 0)
        return 1;
    size_t power = 1;
    while (power < n)
    {
        power *= 2;
    }
    return power;
}

LockFreeQueue::LockFreeQueue(size_t capacity)
    : m_capacity(nextPowerOf2(std::max(size_t(2), capacity))), m_mask(m_capacity - 1)
{
    m_buffer = std::make_unique<Node[]>(m_capacity);
}

LockFreeQueue::~LockFreeQueue()
{
    m_shuttingDown.store(true, std::memory_order_release);
    m_flushCondition.notify_all();
}

bool LockFreeQueue::enqueue(const QueueItem &item)
{
    size_t currentHead = m_head.load(std::memory_order_relaxed);

    while (true)
    {
        // Calculate the next head position
        size_t nextHead = (currentHead + 1) & m_mask;

        // Check if the queue is full
        if (nextHead == m_tail.load(std::memory_order_acquire))
        {
            return false; // Queue is full
        }

        // Try to reserve the slot at currentHead
        if (m_head.compare_exchange_weak(currentHead, nextHead,
                                         std::memory_order_release,
                                         std::memory_order_relaxed))
        {
            // Successfully reserved the slot, now write the data
            m_buffer[currentHead].data = item;

            // Mark the node as ready for consumption
            m_buffer[currentHead].ready.store(true, std::memory_order_release);

            // Increment the atomic size
            m_size.fetch_add(1, std::memory_order_relaxed);

            return true;
        }
    }
}

bool LockFreeQueue::enqueueBlocking(const QueueItem &item, std::chrono::milliseconds timeout)
{
    auto start = std::chrono::steady_clock::now();
    int backoffMs = 1;
    const int maxBackoffMs = 100;
    const double jitterFactor = 0.2;

    while (true)
    {
        if (enqueue(item))
        {
            return true;
        }

        if (m_shuttingDown.load(std::memory_order_acquire))
        {
            return false;
        }

        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed >= timeout)
        {
            return false;
        }

        // Add some random jitter to prevent synchronized retries (thundering herd problems)
        int jitter = static_cast<int>(backoffMs * jitterFactor * (static_cast<double>(rand()) / RAND_MAX));
        int sleepTime = backoffMs + jitter;

        // Make sure we don't sleep longer than our remaining timeout
        if (timeout != std::chrono::milliseconds::max())
        {
            auto remainingTime = timeout - elapsed;
            if (remainingTime <= std::chrono::milliseconds(sleepTime))
            {
                sleepTime = std::max(1, static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(remainingTime).count()));
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
        backoffMs = std::min(backoffMs * 2, maxBackoffMs);
    }
}

bool LockFreeQueue::enqueueBatch(const std::vector<QueueItem> &items)
{
    const size_t itemCount = items.size();

    size_t currentHead = m_head.load(std::memory_order_relaxed);
    size_t currentTail = m_tail.load(std::memory_order_acquire);

    size_t availableSpace;
    if (currentHead >= currentTail)
    {
        availableSpace = m_capacity - (currentHead - currentTail) - 1;
    }
    else
    {
        availableSpace = currentTail - currentHead - 1;
    }

    if (availableSpace < itemCount)
    {
        return false;
    }

    size_t nextHead = (currentHead + itemCount) & m_mask;

    if (!m_head.compare_exchange_strong(currentHead, nextHead,
                                        std::memory_order_release,
                                        std::memory_order_relaxed))
    {
        return false; // Someone else modified the head
    }

    // Successfully reserved slots, now add items
    size_t index = currentHead;
    for (size_t i = 0; i < itemCount; ++i)
    {
        m_buffer[index].data = items[i];
        m_buffer[index].ready.store(true, std::memory_order_release);
        index = (index + 1) & m_mask;
    }

    // Increment the atomic size by the number of items added
    m_size.fetch_add(itemCount, std::memory_order_relaxed);

    return true;
}

bool LockFreeQueue::enqueueBatchBlocking(const std::vector<QueueItem> &items,
                                         std::chrono::milliseconds timeout)
{
    auto start = std::chrono::steady_clock::now();
    int backoffMs = 1;
    const int maxBackoffMs = 100;
    const double jitterFactor = 0.2;

    while (true)
    {
        if (enqueueBatch(items))
        {
            return true;
        }

        if (m_shuttingDown.load(std::memory_order_acquire))
        {
            return false;
        }

        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed >= timeout)
        {
            return false;
        }

        // Add some random jitter to prevent synchronized retries
        int jitter = static_cast<int>(backoffMs * jitterFactor * (static_cast<double>(rand()) / RAND_MAX));
        int sleepTime = backoffMs + jitter;

        // Make sure we don't sleep longer than our remaining timeout
        if (timeout != std::chrono::milliseconds::max())
        {
            auto remainingTime = timeout - elapsed;
            if (remainingTime <= std::chrono::milliseconds(sleepTime))
            {
                sleepTime = std::max(1, static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(remainingTime).count()));
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
        backoffMs = std::min(backoffMs * 2, maxBackoffMs);
    }
}

bool LockFreeQueue::dequeue(QueueItem &item)
{
    while (true)
    {
        // Get the current tail position
        size_t currentTail = m_tail.load(std::memory_order_relaxed);

        // Check if the queue is empty
        if (currentTail == m_head.load(std::memory_order_acquire))
        {
            return false;
        }

        // Check if the node at currentTail is ready to be consumed
        if (!m_buffer[currentTail].ready.load(std::memory_order_acquire))
        {
            // The node is not ready yet, this shouldn't happen in a properly
            // synchronized queue but let's handle it just in case
            std::this_thread::yield();
            continue;
        }

        // Calculate the next tail position
        size_t nextTail = (currentTail + 1) & m_mask;

        // Try to update the tail
        if (m_tail.compare_exchange_weak(currentTail, nextTail,
                                         std::memory_order_release,
                                         std::memory_order_relaxed))
        {
            // Successfully dequeued, now read the data
            item = m_buffer[currentTail].data;

            // Mark the node as not ready
            m_buffer[currentTail].ready.store(false, std::memory_order_release);

            // Decrement the atomic size
            m_size.fetch_sub(1, std::memory_order_relaxed);

            m_flushCondition.notify_one();
            return true;
        }
    }
}

size_t LockFreeQueue::dequeueBatch(std::vector<QueueItem> &items, size_t maxItems)
{
    items.clear();
    items.reserve(maxItems);

    size_t dequeued = 0;
    QueueItem item;

    while (dequeued < maxItems && dequeue(item))
    {
        items.push_back(std::move(item));
        dequeued++;
    }

    return dequeued;
}

bool LockFreeQueue::flush()
{
    std::unique_lock<std::mutex> lock(m_flushMutex);

    // Wait until the queue is empty or shutting down
    m_flushCondition.wait(lock, [this]
                          { return (m_head.load(std::memory_order_acquire) ==
                                    m_tail.load(std::memory_order_acquire)) ||
                                   m_shuttingDown.load(std::memory_order_acquire); });

    return !m_shuttingDown.load(std::memory_order_acquire);
}

size_t LockFreeQueue::size() const
{
    return m_size.load(std::memory_order_relaxed);
}