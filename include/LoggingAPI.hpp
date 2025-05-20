#ifndef LOGGING_API_HPP
#define LOGGING_API_HPP

#include "LogEntry.hpp"
#include "BufferQueue.hpp"
#include "QueueItem.hpp"
#include <string>
#include <chrono>
#include <memory>
#include <vector>
#include <shared_mutex>
#include <functional>
#include <optional>

class LoggingAPI
{
    friend class LoggingAPITest;

public:
    static LoggingAPI &getInstance();

    bool initialize(std::shared_ptr<BufferQueue> queue,
                    std::chrono::milliseconds appendTimeout = std::chrono::milliseconds::max());

    BufferQueue::ProducerToken createProducerToken();
    bool append(const LogEntry &entry,
                BufferQueue::ProducerToken &token,
                const std::optional<std::string> &filename = std::nullopt);
    bool appendBatch(const std::vector<LogEntry> &entries,
                     BufferQueue::ProducerToken &token,
                     const std::optional<std::string> &filename = std::nullopt);

    bool exportLogs(const std::string &outputPath,
                    std::chrono::system_clock::time_point fromTimestamp = std::chrono::system_clock::time_point(),
                    std::chrono::system_clock::time_point toTimestamp = std::chrono::system_clock::time_point());

    bool reset();

    ~LoggingAPI();

private:
    LoggingAPI();
    LoggingAPI(const LoggingAPI &) = delete;
    LoggingAPI &operator=(const LoggingAPI &) = delete;
    // Singleton instance
    static std::unique_ptr<LoggingAPI> s_instance;
    static std::mutex s_instanceMutex;

    std::shared_ptr<BufferQueue> m_logQueue;
    std::chrono::milliseconds m_appendTimeout;

    // State tracking
    bool m_initialized;

    // Helper to report errors
    void reportError(const std::string &message);
};

#endif