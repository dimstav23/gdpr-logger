#include "LoggingSystem.hpp"
#include <iostream>
#include <fstream>
#include <filesystem>
#include "Crypto.hpp"
#include "Compression.hpp"

LoggingSystem::LoggingSystem(const LoggingConfig &config)
    : m_numWriterThreads(config.numWriterThreads),
      m_batchSize(config.batchSize)
{
    if (!std::filesystem::create_directories(config.basePath))
    {
        if (!std::filesystem::exists(config.basePath))
        {
            throw std::runtime_error("Failed to create log directory: " + config.basePath);
        }
        // If directory exists, no error; proceed silently
    }

    m_queue = std::make_shared<LockFreeQueue>(config.queueCapacity);
    m_storage = std::make_shared<SegmentedStorage>(
        config.basePath, config.baseFilename,
        config.maxSegmentSize, config.bufferSize);

    LoggingAPI::getInstance().initialize(m_queue, config.appendTimeout);

    m_writers.reserve(m_numWriterThreads);
}

LoggingSystem::~LoggingSystem()
{
    // Ensure system is stopped before destroying
    stop(true);
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

    m_acceptingEntries.store(false, std::memory_order_release);

    if (waitForCompletion && m_queue)
    {
        std::cout << "LoggingSystem: Waiting for queue to empty..." << std::endl;
        m_queue->flush();
    }

    for (auto &writer : m_writers)
    {
        if (writer)
        {
            writer->stop();
        }
    }
    m_writers.clear();

    // Flush storage to ensure all data is written
    if (m_storage)
    {
        m_storage->flush();
    }

    m_running.store(false, std::memory_order_release);

    LoggingAPI::getInstance().reset();

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

bool LoggingSystem::appendBatch(const std::vector<LogEntry> &entries)
{
    if (!m_acceptingEntries.load(std::memory_order_acquire))
    {
        std::cerr << "LoggingSystem: Not accepting entries" << std::endl;
        return false;
    }

    return LoggingAPI::getInstance().appendBatch(entries);
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