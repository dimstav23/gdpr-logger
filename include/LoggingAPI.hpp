#ifndef LOGGING_API_HPP
#define LOGGING_API_HPP

#include "LogEntry.hpp"
#include <string>
#include <chrono>
#include <memory>
#include <vector>
#include <mutex>
#include <functional>

// Forward declaration of the ILogQueue interface
// The full definition is in Buffer.hpp
class ILogQueue;

// Main Logging API class that provides the interface for logging
class LoggingAPI
{
    friend class LoggingAPITest;

public:
    // Get the singleton instance of the LoggingAPI
    static LoggingAPI &getInstance();

    // Initialize the logging system with a queue implementation
    bool initialize(std::shared_ptr<ILogQueue> queue);

    // Append a log entry to the logging system
    bool append(const LogEntry &entry);

    // Create and append a log entry with the given parameters
    bool append(LogEntry::ActionType actionType,
                const std::string &dataLocation,
                const std::string &userId,
                const std::string &dataSubjectId);

    // Shutdown the logging system gracefully
    // waitForCompletion: Whether to wait for all pending entries to be written
    bool shutdown(bool waitForCompletion = true);

    // Export logs for auditing purposes
    // outputPath: path where exported logs should be written
    // fromTimestamp: optional start timestamp for exported logs
    // param toTimestamp Optional end timestamp for exported logs
    bool exportLogs(const std::string &outputPath,
                    std::chrono::system_clock::time_point fromTimestamp = std::chrono::system_clock::time_point(),
                    std::chrono::system_clock::time_point toTimestamp = std::chrono::system_clock::time_point());

    // Destructor
    ~LoggingAPI();

private:
    // Private constructor for singleton pattern
    LoggingAPI();

    // Disallow copying and assignment
    LoggingAPI(const LoggingAPI &) = delete;
    LoggingAPI &operator=(const LoggingAPI &) = delete;

    // Singleton instance
    static std::unique_ptr<LoggingAPI> s_instance;
    static std::mutex s_instanceMutex;

    // The queue where log entries will be placed
    std::shared_ptr<ILogQueue> m_logQueue;

    // State tracking
    bool m_initialized;
    std::mutex m_apiMutex;

    // Helper to report errors
    void reportError(const std::string &message);
};

#endif