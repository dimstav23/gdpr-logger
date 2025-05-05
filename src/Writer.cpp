#include "Writer.hpp"
#include "Crypto.hpp"
#include "Compression.hpp"
#include <iostream>
#include <chrono>
#include <map>

Writer::Writer(LockFreeQueue &queue,
               std::shared_ptr<SegmentedStorage> storage,
               size_t batchSize,
               bool useEncryption)
    : m_queue(queue),
      m_storage(std::move(storage)),
      m_batchSize(batchSize),
      m_useEncryption(useEncryption) {}

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

    Crypto crypto;
    std::vector<uint8_t> encryptionKey(32, 0x42); // dummy key

    while (m_running)
    {
        // Try to dequeue a batch of log entries
        size_t entriesDequeued = m_queue.dequeueBatch(batch, m_batchSize);
        if (entriesDequeued == 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }

        // Group log entries by destination (either default storage or specific file)
        std::map<std::optional<std::string>, std::vector<std::vector<uint8_t>>> groupedSerializedEntries;

        // Serialize all entries first, then group by destination
        for (const auto &item : batch)
        {
            std::vector<uint8_t> serializedEntry = item.entry.serialize();
            groupedSerializedEntries[item.targetFilename].push_back(serializedEntry);
        }

        // Process each group separately
        for (const auto &[targetFilename, serializedEntries] : groupedSerializedEntries)
        {
            // Compress the batch of serialized entries
            std::vector<uint8_t> compressedData = Compression::compressBatch(serializedEntries);

            // encrypt if encryption is enabled
            std::vector<uint8_t> dataToWrite;
            if (m_useEncryption)
            {
                dataToWrite = crypto.encrypt(compressedData, encryptionKey);
            }
            else
            {
                dataToWrite = compressedData;
            }

            if (targetFilename)
            {
                m_storage->writeToFile(*targetFilename, dataToWrite);
            }
            else
            {
                // Write to default segmented storage
                m_storage->write(dataToWrite);
            }
        }

        batch.clear();
    }
}