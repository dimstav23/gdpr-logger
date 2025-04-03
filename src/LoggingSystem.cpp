#include "LoggingSystem.hpp"
#include <iostream>
#include <fstream>
#include <filesystem>
#include "Crypto.hpp"
#include "Compression.hpp"

LoggingSystem::LoggingSystem(const std::string &basePath,
                             const std::string &baseFilename,
                             size_t maxSegmentSize,
                             size_t bufferSize,
                             size_t queueCapacity,
                             size_t batchSize,
                             size_t numWriterThreads)
    : m_numWriterThreads(numWriterThreads),
      m_batchSize(batchSize)
{
    // Create the directory if it doesn't exist
    std::filesystem::create_directories(basePath);

    // Initialize components
    m_queue = std::make_shared<LockFreeQueue>(queueCapacity);
    m_storage = std::make_shared<SegmentedStorage>(basePath, baseFilename, maxSegmentSize, bufferSize);

    // Initialize the LoggingAPI with our queue
    LoggingAPI::getInstance().initialize(m_queue);

    // Reserve space for writers
    m_writers.reserve(numWriterThreads);
}

LoggingSystem::~LoggingSystem()
{
    // Ensure system is stopped before destroying
    stop(false);
}

bool LoggingSystem::start()
{
    std::lock_guard<std::mutex> lock(m_systemMutex);

    if (m_running.load(std::memory_order_acquire))
    {
        std::cerr << "LoggingSystem: Already running" << std::endl;
        return false;
    }

    // Set flags
    m_running.store(true, std::memory_order_release);
    m_acceptingEntries.store(true, std::memory_order_release);

    // Create and start writer threads
    for (size_t i = 0; i < m_numWriterThreads; i++)
    {
        auto writer = std::make_unique<Writer>(*m_queue, m_storage, m_batchSize);
        writer->start();
        m_writers.push_back(std::move(writer));
    }

    std::cout << "LoggingSystem: Started " << m_numWriterThreads << " writer threads" << std::endl;
    return true;
}

bool LoggingSystem::stop(bool waitForCompletion)
{
    std::lock_guard<std::mutex> lock(m_systemMutex);

    if (!m_running.load(std::memory_order_acquire))
    {
        std::cerr << "LoggingSystem: Not running" << std::endl;
        return false;
    }

    // Stop accepting new entries
    m_acceptingEntries.store(false, std::memory_order_release);

    // If requested, wait for queue to empty
    if (waitForCompletion && m_queue)
    {
        std::cout << "LoggingSystem: Waiting for queue to empty..." << std::endl;
        m_queue->flush();
    }

    // Stop all writers
    for (auto &writer : m_writers)
    {
        if (writer)
        {
            writer->stop();
        }
    }

    // Clear the writer container
    m_writers.clear();

    // Flush storage to ensure all data is written
    if (m_storage)
    {
        m_storage->flush();
    }

    // Set running flag to false
    m_running.store(false, std::memory_order_release);

    // Reset LoggingAPI
    LoggingAPI::getInstance().shutdown(false);

    std::cout << "LoggingSystem: Stopped" << std::endl;
    return true;
}

bool LoggingSystem::isRunning() const
{
    return m_running.load(std::memory_order_acquire);
}

bool LoggingSystem::append(const LogEntry &entry)
{
    if (!m_acceptingEntries.load(std::memory_order_acquire))
    {
        std::cerr << "LoggingSystem: Not accepting entries" << std::endl;
        return false;
    }

    return LoggingAPI::getInstance().append(entry);
}

bool LoggingSystem::append(LogEntry::ActionType actionType,
                           const std::string &dataLocation,
                           const std::string &userId,
                           const std::string &dataSubjectId)
{
    if (!m_acceptingEntries.load(std::memory_order_acquire))
    {
        std::cerr << "LoggingSystem: Not accepting entries" << std::endl;
        return false;
    }

    return LoggingAPI::getInstance().append(actionType, dataLocation, userId, dataSubjectId);
}

bool LoggingSystem::exportLogs(const std::string &outputPath,
                               std::chrono::system_clock::time_point fromTimestamp,
                               std::chrono::system_clock::time_point toTimestamp)
{
    // This is a placeholder implementation for log export
    // A complete solution would:
    // 1. Read the encrypted segments from storage
    // 2. Decrypt and decompress them
    // 3. Filter by timestamp if requested
    // 4. Write to the output path

    std::cerr << "LoggingSystem: Export logs not fully implemented" << std::endl;
}