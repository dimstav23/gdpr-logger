#ifndef LOGGING_SYSTEM_HPP
#define LOGGING_SYSTEM_HPP

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
    /**
     * Constructor initializes the logging system with configuration parameters.
     * @param basePath Directory for log segments
     * @param baseFilename Base name for segment files
     * @param maxSegmentSize Max size per segment before rotation (bytes)
     * @param bufferSize Write buffer size for storage (bytes)
     * @param queueCapacity Capacity of the lock-free queue
     * @param batchSize Number of entries per batch for the writer
     * @param numWriterThreads Number of writer threads to process the queue (default: 2)
     */
    LoggingSystem(const std::string &basePath,
                  const std::string &baseFilename,
                  size_t maxSegmentSize = 100 * 1024 * 1024, // 100 MB default
                  size_t bufferSize = 64 * 1024,             // 64 KB buffer default
                  size_t queueCapacity = 8192,               // Default queue capacity
                  size_t batchSize = 100,                    // Default batch size
                  size_t numWriterThreads = 2);              // Default number of writer threads

    /** Destructor ensures proper shutdown */
    ~LoggingSystem();

    /** Starts the writer threads to begin processing log entries */
    bool start();

    /**
     * Stops the logging system, optionally waiting for all entries to be written
     * @param waitForCompletion If true, waits until the queue is empty
     * @return True if shutdown succeeds
     */
    bool stop(bool waitForCompletion = true);

    /** Returns true if the logging system is currently running */
    bool isRunning() const;

    /** Appends a pre-constructed log entry to the queue */
    bool append(const LogEntry &entry);

    /** Creates and appends a log entry with the given parameters */
    bool append(LogEntry::ActionType actionType,
                const std::string &dataLocation,
                const std::string &userId,
                const std::string &dataSubjectId);

    /**
     * Exports logs to a file, optionally filtered by timestamp range
     * @param outputPath Path to write exported logs
     * @param fromTimestamp Start of timestamp range (default: epoch start)
     * @param toTimestamp End of timestamp range (default: now)
     * @return True if export succeeds
     */
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