#ifndef LOGGING_API_HPP
#define LOGGING_API_HPP

#include "LogEntry.hpp"
#include "LockFreeQueue.hpp"
#include <string>
#include <chrono>
#include <memory>
#include <vector>
#include <shared_mutex>
#include <functional>

class LoggingAPI
{
    friend class LoggingAPITest;

public:
    static LoggingAPI &getInstance();

    bool initialize(std::shared_ptr<LockFreeQueue> queue,
                    std::chrono::milliseconds appendTimeout = std::chrono::milliseconds::max());

    bool append(const LogEntry &entry);
    bool appendBatch(const std::vector<LogEntry> &entries);

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

    std::shared_ptr<LockFreeQueue> m_logQueue;
    std::chrono::milliseconds m_appendTimeout;

    // State tracking
    bool m_initialized;
    std::shared_mutex m_apiMutex;

    // Helper to report errors
    void reportError(const std::string &message);
};

#endif