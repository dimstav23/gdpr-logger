#ifndef BUFFER_QUEUE_HPP
#define BUFFER_QUEUE_HPP

#include "QueueItem.hpp"
#include "concurrentqueue.h"
#include <atomic>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <chrono>

class BufferQueue
{
private:
    moodycamel::ConcurrentQueue<QueueItem> m_queue;

    // Atomic size counter (Moodycamel queue size() is approximate)
    std::atomic<size_t> m_size{0};

    mutable std::mutex m_flushMutex;
    std::condition_variable m_flushCondition;

public:
    explicit BufferQueue(size_t capacity = 8192);
    ~BufferQueue();

    bool enqueueBlocking(const QueueItem &item, std::chrono::milliseconds timeout = std::chrono::milliseconds::max());
    bool enqueueBatchBlocking(const std::vector<QueueItem> &items,
                              std::chrono::milliseconds timeout = std::chrono::milliseconds::max());
    bool dequeue(QueueItem &item);
    size_t dequeueBatch(std::vector<QueueItem> &items, size_t maxItems);
    bool flush();
    size_t size() const;
    bool isEmpty() const { return size() == 0; }

    // delete copy/move
    BufferQueue(const BufferQueue &) = delete;
    BufferQueue &operator=(const BufferQueue &) = delete;
    BufferQueue(BufferQueue &&) = delete;
    BufferQueue &operator=(BufferQueue &&) = delete;

private:
    bool enqueue(const QueueItem &item);
    bool enqueueBatch(const std::vector<QueueItem> &items);
};

#endif