#include "Writer.hpp"
#include "Crypto.hpp"
#include "Compression.hpp"
#include <iostream>
#include <chrono>
#include <map>

Writer::Writer(BufferQueue &queue,
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
    std::vector<uint8_t> encryptionKey(crypto.KEY_SIZE, 0x42); // dummy key
    std::vector<uint8_t> dummyIV(crypto.GCM_IV_SIZE, 0x24);    // dummy IV

    while (m_running)
    {
        size_t entriesDequeued = m_queue.dequeueBatch(batch, m_batchSize);
        if (entriesDequeued == 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }

        std::map<std::optional<std::string>, std::vector<std::vector<uint8_t>>> groupedSerializedEntries;

        // Serialize all entries first, then group by destination
        for (const auto &item : batch)
        {
            std::vector<uint8_t> serializedEntry = item.entry.serialize();
            groupedSerializedEntries[item.targetFilename].push_back(serializedEntry);
        }

        for (const auto &[targetFilename, serializedEntries] : groupedSerializedEntries)
        {
            std::vector<uint8_t> compressedData = Compression::compressBatch(serializedEntries);

            std::vector<uint8_t> dataToWrite = m_useEncryption ? crypto.encrypt(compressedData, encryptionKey, dummyIV) : compressedData;

            if (targetFilename)
            {
                m_storage->writeToFile(*targetFilename, dataToWrite);
            }
            else
            {
                m_storage->write(dataToWrite);
            }
        }

        batch.clear();
    }
}