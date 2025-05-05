#ifndef LOCK_FREE_QUEUE_HPP
#define LOCK_FREE_QUEUE_HPP

#include "QueueItem.hpp"
#include <atomic>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>

class LockFreeQueue
{
private:
    struct Node
    {
        std::atomic<bool> ready{false};
        QueueItem data;
    };

    // Circular buffer of nodes
    std::unique_ptr<Node[]> m_buffer;
    const size_t m_capacity;
    const size_t m_mask;

    // Head and tail indices
    std::atomic<size_t> m_head{0}; // enqueue position
    std::atomic<size_t> m_tail{0}; // dequeue position

    // Atomic size counter
    std::atomic<size_t> m_size{0};

    // Used for flush() operation
    mutable std::mutex m_flushMutex;
    std::condition_variable m_flushCondition;

public:
    // Construct a lock-free queue with specified capacity rounded up to power of 2
    explicit LockFreeQueue(size_t capacity = 8192);
    ~LockFreeQueue();

    bool enqueueBlocking(const QueueItem &item, std::chrono::milliseconds timeout = std::chrono::milliseconds::max());
    bool enqueueBatchBlocking(const std::vector<QueueItem> &items,
                              std::chrono::milliseconds timeout = std::chrono::milliseconds::max());
    bool dequeue(QueueItem &item);
    size_t dequeueBatch(std::vector<QueueItem> &items, size_t maxItems);
    bool flush();
    size_t size() const;
    bool isEmpty() const { return size() == 0; }

    // delete copy/move
    LockFreeQueue(const LockFreeQueue &) = delete;
    LockFreeQueue &operator=(const LockFreeQueue &) = delete;
    LockFreeQueue(LockFreeQueue &&) = delete;
    LockFreeQueue &operator=(LockFreeQueue &&) = delete;

private:
    bool enqueue(const QueueItem &item);
    bool enqueueBatch(const std::vector<QueueItem> &items);
};

#endif