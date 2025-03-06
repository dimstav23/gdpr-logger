#pragma once

#include "LogEntry.hpp"
#include "Config.hpp"
#include "Buffer.hpp"
#include "Writer.hpp"
#include <memory>
#include <string>
#include <vector>

/**
 * @brief Main interface for the GDPR logging system
 *
 * Provides a simple API for logging GDPR-related operations with high throughput.
 * Log entries are queued and written asynchronously by dedicated writer threads.
 */
class LoggingAPI
{
public:
    /**
     * @brief Constructs a LoggingAPI instance with provided configuration
     * @param config Configuration settings for the logging system
     * @throws ConfigException if configuration is invalid
     */
    explicit LoggingAPI(const Config &config);

    /**
     * @brief Destructor ensures all pending logs are written before shutdown
     */
    ~LoggingAPI();

    /**
     * @brief Delete copy constructor and assignment operator
     */
    LoggingAPI(const LoggingAPI &) = delete;
    LoggingAPI &operator=(const LoggingAPI &) = delete;

    /**
     * @brief Logs a data operation in the GDPR context
     *
     * @param action Type of action performed (e.g., "READ", "UPDATE", "DELETE")
     * @param dataSubject Identifier for the data subject (e.g., user ID)
     * @param dataCategory Category of data being accessed (e.g., "PERSONAL", "SENSITIVE")
     * @param dataLocation Reference to where the data is stored
     * @param requestReason Reason for the data operation
     * @param additionalInfo Optional additional information
     * @return true if the log entry was successfully queued, false otherwise
     */
    bool append(const std::string &action,
                const std::string &dataSubject,
                const std::string &dataCategory,
                const std::string &dataLocation,
                const std::string &requestReason,
                const std::string &additionalInfo = "");

    /**
     * @brief Exports logs from a specific time range
     *
     * @param startTime Beginning of time range
     * @param endTime End of time range
     * @param outputPath File path where exported logs will be written
     * @return Number of log entries exported
     * @throws ExportException if export fails
     */
    size_t exportLogs(std::time_t startTime, std::time_t endTime, const std::string &outputPath);

    /**
     * @brief Verifies integrity of logs in storage
     *
     * Checks all log segments for tampering by verifying hash chains.
     *
     * @return true if all logs are intact, false if tampering detected
     */
    bool verifyIntegrity();

    /**
     * @brief Get the current status of the logging system
     *
     * @return Status object containing queue size, active writers, etc.
     */
    LoggingStatus getStatus() const;

private:
    std::unique_ptr<Config> m_config;
    std::unique_ptr<Buffer> m_buffer;
    std::vector<std::unique_ptr<Writer>> m_writers;

    bool m_initialized;
    bool m_shutdownRequested;

    /**
     * @brief Initializes the logging system components
     * @return true if initialization succeeds, false otherwise
     */
    bool initialize();

    /**
     * @brief Requests graceful shutdown of all system components
     */
    void shutdown();
};