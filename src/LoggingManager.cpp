#include "LoggingManager.hpp"
#include "Crypto.hpp"
#include "Compression.hpp"
#include <iostream>
#include <filesystem>

LoggingManager::LoggingManager(const LoggingConfig &config)
    : m_numWriterThreads(config.numWriterThreads),
      m_batchSize(config.batchSize),
      m_useEncryption(config.useEncryption),
      m_compressionLevel(config.compressionLevel)
{
    if (!std::filesystem::create_directories(config.basePath) &&
        !std::filesystem::exists(config.basePath))
    {
        throw std::runtime_error("Failed to create log directory: " + config.basePath);
    }

    m_queue = std::make_shared<BufferQueue>(config.queueCapacity, config.maxExplicitProducers);
    m_storage = std::make_shared<SegmentedStorage>(
        config.basePath, config.baseFilename,
        config.maxSegmentSize,
        config.maxAttempts,
        config.baseRetryDelay,
        config.maxOpenFiles);

    Logger::getInstance().initialize(m_queue, config.appendTimeout);

    m_writers.reserve(m_numWriterThreads);
}

LoggingManager::~LoggingManager()
{
    stop();
}

bool LoggingManager::start()
{
    std::lock_guard<std::mutex> lock(m_systemMutex);

    if (m_running.load(std::memory_order_acquire))
    {
        std::cerr << "LoggingSystem: Already running" << std::endl;
        return false;
    }

    m_running.store(true, std::memory_order_release);
    m_acceptingEntries.store(true, std::memory_order_release);

    for (size_t i = 0; i < m_numWriterThreads; ++i)
    {
        auto writer = std::make_unique<Writer>(*m_queue, m_storage, m_batchSize, m_useEncryption, m_compressionLevel);
        writer->start();
        m_writers.push_back(std::move(writer));
    }

    std::cout << "LoggingSystem: Started " << m_numWriterThreads << " writer threads" << std::endl;
    std::cout << "Encryption: " << (m_useEncryption ? "Enabled" : "Disabled") << std::endl;
    std::cout << "Compression: " << (m_compressionLevel != 0 ? "Enabled" : "Disabled") << std::endl;
    return true;
}

bool LoggingManager::startGDPR()
{
    std::lock_guard<std::mutex> lock(m_systemMutex);

    if (m_running.load(std::memory_order_acquire))
    {
        std::cerr << "LoggingSystem: Already running" << std::endl;
        return false;
    }

    m_running.store(true, std::memory_order_release);
    m_acceptingEntries.store(true, std::memory_order_release);

    for (size_t i = 0; i < m_numWriterThreads; ++i)
    {
        auto writer = std::make_unique<Writer>(*m_queue, m_storage, m_batchSize, m_useEncryption, m_compressionLevel);
        writer->startGDPR();
        m_writers.push_back(std::move(writer));
    }

    std::cout << "LoggingSystem: Started " << m_numWriterThreads << " writer threads" << std::endl;
    std::cout << "Encryption: " << (m_useEncryption ? "Enabled" : "Disabled") << std::endl;
    std::cout << "Compression: " << (m_compressionLevel != 0 ? "Enabled" : "Disabled") << std::endl;
    return true;
}

bool LoggingManager::stop()
{
    std::lock_guard<std::mutex> lock(m_systemMutex);

    if (!m_running.load(std::memory_order_acquire))
    {
        return false;
    }

    m_acceptingEntries.store(false, std::memory_order_release);

    if (m_queue)
    {
        std::cout << "LoggingSystem: Waiting for queue to empty..." << std::endl;
        m_queue->flush();
    }

    for (auto &writer : m_writers)
    {
        writer->stop();
    }
    m_writers.clear();

    // Flush storage to ensure all data is written
    if (m_storage)
    {
        m_storage->flush();
    }

    m_running.store(false, std::memory_order_release);

    Logger::getInstance().reset();

    std::cout << "LoggingSystem: Stopped" << std::endl;
    return true;
}

BufferQueue::ProducerToken LoggingManager::createProducerToken()
{
    return Logger::getInstance().createProducerToken();
}

bool LoggingManager::append(LogEntry entry,
                            BufferQueue::ProducerToken &token,
                            const std::optional<std::string> &filename)
{
    if (!m_acceptingEntries.load(std::memory_order_acquire))
    {
        std::cerr << "LoggingSystem: Not accepting entries" << std::endl;
        return false;
    }

    return Logger::getInstance().append(std::move(entry), token, filename);
}

bool LoggingManager::appendBatch(std::vector<LogEntry> entries,
                                 BufferQueue::ProducerToken &token,
                                 const std::optional<std::string> &filename)
{
    if (!m_acceptingEntries.load(std::memory_order_acquire))
    {
        std::cerr << "LoggingSystem: Not accepting entries" << std::endl;
        return false;
    }

    return Logger::getInstance().appendBatch(std::move(entries), token, filename);
}

bool LoggingManager::exportLogs(
    const std::string &outputPath,
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
    return false;
}