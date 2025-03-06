#pragma once
#include <queue>
#include <mutex>
#include <condition_variable>
#include <optional>

template <typename T>
class ThreadSafeQueue
{
private:
    std::queue<T> m_queue;
    mutable std::mutex m_mutex;
    std::condition_variable m_condVar;
    bool m_shutdown{false};

public:
    // default constructor (when instance of this object is created)
    ThreadSafeQueue() = default;
    // default destructor (when instance of this object is destroyed)
    ~ThreadSafeQueue() = default;

    // Disable copy semantics
    ThreadSafeQueue(const ThreadSafeQueue &) = delete;
    ThreadSafeQueue &operator=(const ThreadSafeQueue &) = delete;

    // Enqueue an item into the queue
    void enqueue(const T &item)
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_queue.push(item);
        }
        m_condVar.notify_one();
    }

    // Optionally allow move semantics
    void enqueue(T &&item)
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_queue.push(std::move(item));
        }
        m_condVar.notify_one();
    }

    // Try to dequeue an item. If the queue is empty, block until an item is available
    T dequeue()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        // Wait until the queue is not empty.
        m_condVar.wait(lock, [this]
                       { return !m_queue.empty() || m_shutdown; });

        // In case of shutdown, we could either throw, return a sentinel, or handle it appropriately.
        if (m_queue.empty())
        {
            // Return a default constructed T if needed or throw an exception.
            return T{};
        }

        T item = std::move(m_queue.front());
        m_queue.pop();
        return item;
    }

    // Non-blocking attempt to dequeue an item.
    std::optional<T> tryDequeue()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_queue.empty())
            return std::nullopt;
        T item = std::move(m_queue.front());
        m_queue.pop();
        return item;
    }

    bool empty() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_queue.empty();
    }

    // Signal shutdown to unblock waiting threads
    void shutdown()
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_shutdown = true;
        }
        m_condVar.notify_all();
    }
};
