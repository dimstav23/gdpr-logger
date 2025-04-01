#ifndef LOCK_FREE_BUFFER_HPP
#define LOCK_FREE_BUFFER_HPP

#include "LogEntry.hpp"
#include <atomic>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>

class ILogQueue
{
public:
    virtual ~ILogQueue() = default;

    virtual bool enqueue(const LogEntry &entry) = 0;

    virtual bool dequeue(LogEntry &entry) = 0;

    // returns number of entries actually dequeued
    virtual size_t dequeueBatch(std::vector<LogEntry> &entries, size_t maxEntries) = 0;

    // Waits until all queued entries are processed
    virtual bool flush() = 0;

    virtual size_t size() const = 0;

    virtual bool isEmpty() const { return size() == 0; }
};

// Lock-free implementation of ILogQueue using a circular buffer
class LockFreeQueue : public ILogQueue
{
private:
    struct Node
    {
        std::atomic<bool> ready{false};
        LogEntry data;
    };

    // Circular buffer of nodes
    std::unique_ptr<Node[]> m_buffer;
    const size_t m_capacity;
    const size_t m_mask;

    // Head and tail indices
    std::atomic<size_t> m_head{0}; // Points to where producers can enqueue
    std::atomic<size_t> m_tail{0}; // Points to where consumers can dequeue

    // Used for flush() operation
    mutable std::mutex m_flushMutex;
    std::condition_variable m_flushCondition;
    std::atomic<bool> m_shuttingDown{false};

public:
    // Construct a lock-free queue with specified capacity rounded up to power of 2
    explicit LockFreeQueue(size_t capacity = 8192);

    ~LockFreeQueue() override;

    bool enqueue(const LogEntry &entry) override;
    bool dequeue(LogEntry &entry) override;
    size_t dequeueBatch(std::vector<LogEntry> &entries, size_t maxEntries) override;
    bool flush() override;
    size_t size() const override;

    // Delete copy/move constructors and assignment operators
    LockFreeQueue(const LockFreeQueue &) = delete;
    LockFreeQueue &operator=(const LockFreeQueue &) = delete;
    LockFreeQueue(LockFreeQueue &&) = delete;
    LockFreeQueue &operator=(LockFreeQueue &&) = delete;
};

#endif