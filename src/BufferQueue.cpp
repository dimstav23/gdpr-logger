#include "BufferQueue.hpp"
#include <algorithm>
#include <thread>
#include <iostream>
#include <chrono>
#include <cmath>

BufferQueue::BufferQueue(size_t capacity, size_t maxExplicitProducers)
{
    m_queue = moodycamel::ConcurrentQueue<QueueItem>(capacity, maxExplicitProducers, 0);
}

bool BufferQueue::enqueue(const QueueItem &item, ProducerToken &token)
{
    return m_queue.try_enqueue(token, item);
}

bool BufferQueue::enqueueBlocking(const QueueItem &item, ProducerToken &token, std::chrono::milliseconds timeout)
{
    auto start = std::chrono::steady_clock::now();
    int backoffMs = 1;
    const int maxBackoffMs = 100;
    const double jitterFactor = 0.2;

    while (true)
    {
        if (enqueue(item, token))
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

bool BufferQueue::enqueueBatch(const std::vector<QueueItem> &items, ProducerToken &token)
{
    return m_queue.try_enqueue_bulk(token, items.begin(), items.size());
}

bool BufferQueue::enqueueBatchBlocking(const std::vector<QueueItem> &items, ProducerToken &token,
                                       std::chrono::milliseconds timeout)
{
    auto start = std::chrono::steady_clock::now();
    int backoffMs = 1;
    const int maxBackoffMs = 100;
    const double jitterFactor = 0.2;

    while (true)
    {
        if (enqueueBatch(items, token))
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

bool BufferQueue::dequeue(QueueItem &item, ConsumerToken &token)
{
    if (m_queue.try_dequeue(token, item))
    {
        return true;
    }
    return false;
}

size_t BufferQueue::dequeueBatch(std::vector<QueueItem> &items, size_t maxItems, ConsumerToken &token)
{
    items.clear();
    items.resize(maxItems);

    size_t dequeued = m_queue.try_dequeue_bulk(token, items.begin(), maxItems);
    items.resize(dequeued);

    return dequeued;
}

bool BufferQueue::flush()
{
    std::unique_lock<std::mutex> lock(m_flushMutex);

    do
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    } while (m_queue.size_approx() != 0);

    return true;
}

size_t BufferQueue::size() const
{
    return m_queue.size_approx();
}