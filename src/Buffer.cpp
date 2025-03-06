#include "Buffer.hpp"
#include <chrono>

Buffer::Buffer(size_t capacity)
    : m_capacity(capacity), m_shutdownRequested(false) {}

Buffer::~Buffer()
{
    signalShutdown();
}

bool Buffer::enqueue(std::unique_ptr<LogEntry> entry)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_queue.size() >= m_capacity)
    {
        return false; // Buffer is full
    }
    m_queue.push_back(std::move(entry));
    m_condVar.notify_one();
    return true;
}

std::vector<std::unique_ptr<LogEntry>> Buffer::dequeueBatch(size_t maxBatchSize, unsigned timeoutMs)
{
    std::unique_lock<std::mutex> lock(m_mutex);

    if (timeoutMs > 0)
    {
        m_condVar.wait_for(lock, std::chrono::milliseconds(timeoutMs), [this]
                           { return !m_queue.empty() || m_shutdownRequested.load(); });
    }
    else
    {
        m_condVar.wait(lock, [this]
                       { return !m_queue.empty() || m_shutdownRequested.load(); });
    }

    std::vector<std::unique_ptr<LogEntry>> batch;
    while (!m_queue.empty() && batch.size() < maxBatchSize)
    {
        batch.push_back(std::move(m_queue.front()));
        m_queue.erase(m_queue.begin());
    }
    return batch;
}

size_t Buffer::size() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_queue.size();
}

bool Buffer::empty() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_queue.empty();
}

void Buffer::signalShutdown()
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_shutdownRequested.store(true);
    }
    m_condVar.notify_all();
}
