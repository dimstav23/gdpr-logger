#ifndef LOGGING_SYSTEM_HPP
#define LOGGING_SYSTEM_HPP

#include "Config.hpp"
#include "LoggingAPI.hpp"
#include "LockFreeBuffer.hpp"
#include "SegmentedStorage.hpp"
#include "Writer.hpp"
#include "LogEntry.hpp"
#include <memory>
#include <vector>
#include <atomic>
#include <mutex>
#include <chrono>
#include <string>

class LoggingSystem
{
public:
    LoggingSystem(const LoggingConfig &config);
    ~LoggingSystem();

    bool start();

    // Stops the logging system, if waitForCompletion waits until the queue is empty
    bool stop(bool waitForCompletion = true);

    bool isRunning() const;

    bool append(const LogEntry &entry);

    bool exportLogs(const std::string &outputPath,
                    std::chrono::system_clock::time_point fromTimestamp = std::chrono::system_clock::time_point(),
                    std::chrono::system_clock::time_point toTimestamp = std::chrono::system_clock::time_point());

private:
    std::shared_ptr<LockFreeQueue> m_queue;         // Thread-safe queue for log entries
    std::shared_ptr<SegmentedStorage> m_storage;    // Manages append-only log segments
    std::vector<std::unique_ptr<Writer>> m_writers; // Multiple writer threads
    std::atomic<bool> m_running{false};             // System running state
    std::atomic<bool> m_acceptingEntries{false};    // Controls whether new entries are accepted
    std::mutex m_systemMutex;                       // For system-wide operations

    size_t m_numWriterThreads; // Number of writer threads
    size_t m_batchSize;        // Batch size for writers
};

#endif