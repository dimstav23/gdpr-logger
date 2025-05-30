#include "LoggingAPI.hpp"
#include "QueueItem.hpp"
#include <iostream>

// Initialize static members
std::unique_ptr<LoggingAPI> LoggingAPI::s_instance = nullptr;
std::mutex LoggingAPI::s_instanceMutex;

LoggingAPI &LoggingAPI::getInstance()
{
    std::lock_guard<std::mutex> lock(s_instanceMutex);
    if (s_instance == nullptr)
    {
        s_instance.reset(new LoggingAPI());
    }
    return *s_instance;
}

LoggingAPI::LoggingAPI()
    : m_logQueue(nullptr),
      m_appendTimeout(std::chrono::milliseconds::max()),
      m_initialized(false)
{
}

LoggingAPI::~LoggingAPI()
{
    if (m_initialized)
    {
        reset();
    }
}

bool LoggingAPI::initialize(std::shared_ptr<BufferQueue> queue,
                            std::chrono::milliseconds appendTimeout)
{
    if (m_initialized)
    {
        reportError("LoggingAPI already initialized");
        return false;
    }

    if (!queue)
    {
        reportError("Cannot initialize with a null queue");
        return false;
    }

    m_logQueue = std::move(queue);
    m_appendTimeout = appendTimeout;
    m_initialized = true;

    return true;
}

BufferQueue::ProducerToken LoggingAPI::createProducerToken()
{
    if (!m_initialized)
    {
        reportError("LoggingAPI not initialized");
        throw std::runtime_error("LoggingAPI not initialized");
    }

    return m_logQueue->createProducerToken();
}

bool LoggingAPI::append(LogEntry entry,
                        BufferQueue::ProducerToken &token,
                        const std::optional<std::string> &filename)
{
    if (!m_initialized)
    {
        reportError("LoggingAPI not initialized");
        return false;
    }

    QueueItem item{std::move(entry), filename};
    return m_logQueue->enqueueBlocking(std::move(item), token, m_appendTimeout);
}

bool LoggingAPI::appendBatch(std::vector<LogEntry> entries,
                             BufferQueue::ProducerToken &token,
                             const std::optional<std::string> &filename)
{
    if (!m_initialized)
    {
        reportError("LoggingAPI not initialized");
        return false;
    }

    if (entries.empty())
    {
        return true;
    }

    std::vector<QueueItem> batch;
    batch.reserve(entries.size());
    for (auto &entry : entries)
    {
        batch.emplace_back(std::move(entry), filename);
    }
    return m_logQueue->enqueueBatchBlocking(std::move(batch), token, m_appendTimeout);
}

bool LoggingAPI::reset()
{
    if (!m_initialized)
    {
        return false;
    }

    // Reset state
    m_initialized = false;
    m_logQueue.reset();

    return true;
}

bool LoggingAPI::exportLogs(
    const std::string &outputPath,
    std::chrono::system_clock::time_point fromTimestamp,
    std::chrono::system_clock::time_point toTimestamp)
{
    if (!m_initialized)
    {
        reportError("LoggingAPI not initialized");
        return false;
    }

    // This functionality would typically be handled by a separate component,
    // such as a log storage or retrieval system
    reportError("Export logs functionality not implemented in LoggingAPI");
    return false;
}

void LoggingAPI::reportError(const std::string &message)
{
    std::cerr << "LoggingAPI Error: " << message << std::endl;
}