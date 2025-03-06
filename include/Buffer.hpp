#pragma once

#include "LogEntry.hpp"
#include <memory>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <atomic>

/**
 * @brief Thread-safe buffer for log entries
 *
 * Implements a lock-free or minimally locked queue to collect log entries
 * before they are processed by writer threads.
 */
class Buffer
{
public:
    /**
     * @brief Constructs a buffer with specified capacity
     *
     * @param capacity Maximum number of entries in the buffer before blocking
     */
    explicit Buffer(size_t capacity);

    /**
     * @brief Destructor ensures proper cleanup
     */
    ~Buffer();

    /**
     * @brief Delete copy constructor and assignment operator
     */
    Buffer(const Buffer &) = delete;
    Buffer &operator=(const Buffer &) = delete;

    /**
     * @brief Adds a log entry to the buffer
     *
     * This operation is thread-safe and minimizes locking.
     *
     * @param entry Log entry to add
     * @return true if entry was successfully added, false otherwise
     */
    bool enqueue(std::unique_ptr<LogEntry> entry);

    /**
     * @brief Retrieves a batch of log entries from the buffer
     *
     * This operation is thread-safe and will block until either:
     * 1. The requested batch size is available
     * 2. A timeout occurs
     * 3. The system is shutting down
     *
     * @param maxBatchSize Maximum number of entries to retrieve
     * @param timeoutMs Timeout in milliseconds (0 means no timeout)
     * @return Vector of log entries retrieved from the buffer
     */
    std::vector<std::unique_ptr<LogEntry>> dequeueBatch(size_t maxBatchSize, unsigned timeoutMs = 0);

    /**
     * @brief Gets the current number of entries in the buffer
     *
     * @return Current buffer size
     */
    size_t size() const;

    /**
     * @brief Checks if the buffer is empty
     *
     * @return true if empty, false otherwise
     */
    bool empty() const;

    /**
     * @brief Signals that shutdown is requested
     *
     * This will unblock any waiting dequeueBatch calls
     */
    void signalShutdown();

private:
    const size_t m_capacity;
    std::atomic<bool> m_shutdownRequested;

    // Implementation can be a simple mutex-protected queue or a more advanced
    // lock-free queue depending on performance requirements

    // Option 1: Simple mutex-protected queue
    std::vector<std::unique_ptr<LogEntry>> m_queue;
    mutable std::mutex m_mutex;
    std::condition_variable m_condVar;

    // Option 2: Lock-free queue
    // Replace the above with a lock-free queue implementation
    // or use a third-party library for lock-free queues
};