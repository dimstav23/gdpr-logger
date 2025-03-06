#include "Writer.hpp"
#include "Compression.hpp"
#include <iostream>
#include <chrono>

Writer::Writer(std::shared_ptr<Buffer> buffer, std::shared_ptr<Crypto> crypto, const Config &config)
    : m_buffer(std::move(buffer)), m_crypto(std::move(crypto)), m_config(config),
      m_running(false), m_stopRequested(false), m_entriesProcessed(0),
      m_bytesWritten(0), m_batchesProcessed(0) {}

Writer::~Writer()
{
    stop();
    if (m_thread.joinable())
    {
        m_thread.join();
    }
}

bool Writer::start()
{
    if (m_running.load())
    {
        return false; // Already running
    }
    m_running.store(true);
    m_thread = std::thread(&Writer::processingLoop, this);
    return true;
}

void Writer::stop()
{
    m_stopRequested.store(true);
    if (m_thread.joinable())
    {
        m_thread.join();
    }
    m_running.store(false);
}

bool Writer::isRunning() const
{
    return m_running.load();
}

WriterStats Writer::getStats() const
{
    return {m_entriesProcessed.load(), m_bytesWritten.load(), m_batchesProcessed.load(),
            m_batchesProcessed.load() ? static_cast<double>(m_entriesProcessed.load()) / m_batchesProcessed.load() : 0.0,
            0.0}; // Placeholder for avgProcessingTime
}

void Writer::processingLoop()
{
    while (!m_stopRequested.load())
    {
        auto batch = m_buffer->dequeueBatch(m_config.batchSize, m_config.batchTimeoutMs);
        if (!batch.empty())
        {
            processBatch(batch);
        }
    }
}

size_t Writer::processBatch(std::vector<std::unique_ptr<LogEntry>> &batch)
{
    if (batch.empty())
    {
        return 0;
    }

    if (!ensureSegmentAvailable())
    {
        return 0;
    }

    std::vector<uint8_t> rawData;
    for (auto &entry : batch)
    {
        auto serialized = entry->serialize();
        rawData.insert(rawData.end(), serialized.begin(), serialized.end());
    }

    auto compressedData = Compression::compress(rawData);
    auto encryptedData = m_crypto->encrypt(compressedData);
    auto hash = m_crypto->computeHash(encryptedData);

    if (!m_currentSegment->write(encryptedData, hash))
    {
        return 0;
    }

    m_lastEntryHash = hash;
    m_entriesProcessed += batch.size();
    m_bytesWritten += encryptedData.size();
    m_batchesProcessed++;
    return batch.size();
}

bool Writer::ensureSegmentAvailable()
{
    if (!m_currentSegment || m_currentSegment->isFull())
    {
        m_currentSegment = std::make_unique<Segment>(m_config);
        return m_currentSegment->open();
    }
    return true;
}
