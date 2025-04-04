#include "LoggingAPI.hpp"
#include "LockFreeBuffer.hpp"
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
    : m_initialized(false)
{
}

LoggingAPI::~LoggingAPI()
{
    if (m_initialized)
    {
        shutdown(false);
    }
}

bool LoggingAPI::initialize(std::shared_ptr<LockFreeQueue> queue)
{
    std::lock_guard<std::mutex> lock(m_apiMutex);

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

    m_logQueue = queue;
    m_initialized = true;

    return true;
}

bool LoggingAPI::append(const LogEntry &entry)
{
    std::lock_guard<std::mutex> lock(m_apiMutex);

    if (!m_initialized)
    {
        reportError("LoggingAPI not initialized");
        return false;
    }

    LogEntry entryCopy = entry;
    return m_logQueue->enqueueBlocking(entryCopy);
}

bool LoggingAPI::append(
    LogEntry::ActionType actionType,
    const std::string &dataLocation,
    const std::string &userId,
    const std::string &dataSubjectId)
{
    LogEntry entry(actionType, dataLocation, userId, dataSubjectId);
    return append(entry);
}

bool LoggingAPI::shutdown(bool waitForCompletion)
{
    std::lock_guard<std::mutex> lock(m_apiMutex);

    if (!m_initialized)
    {
        reportError("LoggingAPI not initialized");
        return false;
    }

    bool result = true;

    if (waitForCompletion)
    {
        result = m_logQueue->flush();
    }

    // Reset state
    m_initialized = false;
    m_logQueue.reset();

    return result;
}

bool LoggingAPI::exportLogs(
    const std::string &outputPath,
    std::chrono::system_clock::time_point fromTimestamp,
    std::chrono::system_clock::time_point toTimestamp)
{
    std::lock_guard<std::mutex> lock(m_apiMutex);

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
