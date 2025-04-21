#include "Writer.hpp"
#include "Crypto.hpp"
#include "Compression.hpp"
#include <iostream>
#include <chrono>
#include <map>

Writer::Writer(LockFreeQueue &queue,
               std::shared_ptr<SegmentedStorage> storage,
               size_t batchSize)
    : m_queue(queue),
      m_storage(std::move(storage)),
      m_batchSize(batchSize) {}

Writer::~Writer()
{
    stop();
}

void Writer::start()
{
    if (m_running.exchange(true))
    {
        return;
    }

    m_writerThread.reset(new std::thread(&Writer::processLogEntries, this));
}

void Writer::stop()
{
    if (m_running.exchange(false))
    {
        if (m_writerThread && m_writerThread->joinable())
        {
            m_writerThread->join();
        }
    }
}

bool Writer::isRunning() const
{
    return m_running.load();
}

void Writer::processLogEntries()
{
    std::vector<QueueItem> batch;
    batch.reserve(m_batchSize);

    Crypto crypto;
    std::vector<uint8_t> encryptionKey(32, 0x42); // dummy key

    while (m_running)
    {
        // Try to dequeue a batch of log entries
        size_t entriesDequeued = m_queue.dequeueBatch(batch, m_batchSize);
        if (entriesDequeued == 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        // Group log entries by destination (either default storage or specific file)
        std::map<std::optional<std::string>, std::vector<LogEntry>> groupedEntries;

        for (const auto &item : batch)
        {
            groupedEntries[item.targetFilename].push_back(item.entry);
        }

        // Process each group separately
        for (const auto &[targetFilename, entries] : groupedEntries)
        {
            // Compress and encrypt the batch of log entries
            std::vector<uint8_t> compressedData = Compression::compressBatch(entries);
            std::vector<uint8_t> encryptedData = crypto.encrypt(compressedData, encryptionKey);

            if (targetFilename)
            {
                // Write to the specified file
                m_storage->writeToFile(*targetFilename, encryptedData);
            }
            else
            {
                // Write to default segmented storage
                m_storage->write(encryptedData);
            }
        }

        batch.clear();
    }

    // Ensure any remaining data is flushed when stopping
    m_storage->flush();
}