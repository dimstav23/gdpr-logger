#pragma once

#include "Buffer.hpp"
#include "Segment.hpp"
#include "Crypto.hpp"
#include "Config.hpp"
#include <atomic>
#include <thread>
#include <memory>
#include <string>

/**
 * @brief Writer thread that processes log entries from the buffer
 *
 * Responsible for batching, compressing, encrypting, and writing log entries
 * to the appropriate segment files.
 */
class Writer
{
public:
    /**
     * @brief Constructs a writer thread
     *
     * @param buffer Shared buffer to pull log entries from
     * @param crypto Crypto tools for hashing and encryption
     * @param config System configuration
     */
    Writer(std::shared_ptr<Buffer> buffer, std::shared_ptr<Crypto> crypto, const Config &config);

    /**
     * @brief Destructor stops the writer thread
     */
    ~Writer();

    /**
     * @brief Delete copy constructor and assignment operator
     */
    Writer(const Writer &) = delete;
    Writer &operator=(const Writer &) = delete;

    /**
     * @brief Starts the writer thread
     *
     * @return true if thread started successfully, false otherwise
     */
    bool start();

    /**
     * @brief Requests the writer thread to stop
     *
     * This is a non-blocking call. The thread will finish processing
     * its current batch before stopping.
     */
    void stop();

    /**
     * @brief Gets the status of the writer thread
     *
     * @return true if running, false if stopped
     */
    bool isRunning() const;

    /**
     * @brief Gets statistics about the writer thread
     *
     * @return Structure containing writer statistics
     */
    WriterStats getStats() const;

private:
    std::shared_ptr<Buffer> m_buffer;
    std::shared_ptr<Crypto> m_crypto;
    Config m_config;

    std::thread m_thread;
    std::atomic<bool> m_running;
    std::atomic<bool> m_stopRequested;

    std::unique_ptr<Segment> m_currentSegment;
    std::vector<uint8_t> m_lastEntryHash;

    // Statistics
    std::atomic<size_t> m_entriesProcessed;
    std::atomic<size_t> m_bytesWritten;
    std::atomic<size_t> m_batchesProcessed;

    /**
     * @brief Main processing function for the writer thread
     */
    void processingLoop();

    /**
     * @brief Processes a batch of log entries
     *
     * @param batch Vector of log entries to process
     * @return Number of entries successfully written
     */
    size_t processBatch(std::vector<std::unique_ptr<LogEntry>> &batch);

    /**
     * @brief Opens a new segment file if needed
     *
     * @return true if successful, false otherwise
     */
    bool ensureSegmentAvailable();
};

/**
 * @brief Statistics for a writer thread
 */
struct WriterStats
{
    size_t entriesProcessed;  // Total entries processed by this writer
    size_t bytesWritten;      // Total bytes written to disk
    size_t batchesProcessed;  // Total batches processed
    double avgBatchSize;      // Average batch size
    double avgProcessingTime; // Average time to process a batch (ms)
};