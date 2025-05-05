#include "LockFreeQueue.hpp"
#include <algorithm>
#include <thread>
#include <iostream>
#include <chrono>
#include <cmath>

LockFreeQueue::LockFreeQueue(size_t capacity)
{
    // Moodycamel queue has dynamic capacity, but hint the initial size
    m_queue = moodycamel::ConcurrentQueue<QueueItem>(capacity);
}

LockFreeQueue::~LockFreeQueue()
{
    m_flushCondition.notify_one();
}

bool LockFreeQueue::enqueue(const QueueItem &item)
{
    bool result = m_queue.enqueue(item);
    if (result)
    {
        m_size.fetch_add(1, std::memory_order_relaxed);
    }
    return result;
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
    bool result = m_queue.enqueue_bulk(items.begin(), items.size());
    if (result)
    {
        m_size.fetch_add(items.size(), std::memory_order_relaxed);
    }
    return result;
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
    if (m_queue.try_dequeue(item))
    {
        m_size.fetch_sub(1, std::memory_order_relaxed);
        m_flushCondition.notify_one();
        return true;
    }
    return false;
}

size_t LockFreeQueue::dequeueBatch(std::vector<QueueItem> &items, size_t maxItems)
{
    items.clear();
    items.resize(maxItems);

    size_t dequeued = m_queue.try_dequeue_bulk(items.begin(), maxItems);
    items.resize(dequeued);

    if (dequeued > 0)
    {
        m_size.fetch_sub(dequeued, std::memory_order_relaxed);
        m_flushCondition.notify_one();
    }

    return dequeued;
}

bool LockFreeQueue::flush()
{
    std::unique_lock<std::mutex> lock(m_flushMutex);

    // Wait until the queue is empty
    m_flushCondition.wait(lock, [this]
                          { return m_size.load(std::memory_order_acquire) == 0; });

    return true;
}

size_t LockFreeQueue::size() const
{
    return m_size.load(std::memory_order_relaxed);
}