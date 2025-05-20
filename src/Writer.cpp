#include "Writer.hpp"
#include "Crypto.hpp"
#include "Compression.hpp"
#include <iostream>
#include <chrono>
#include <map>

Writer::Writer(BufferQueue &queue,
               std::shared_ptr<SegmentedStorage> storage,
               size_t batchSize,
               bool useEncryption,
               bool useCompression)
    : m_queue(queue),
      m_storage(std::move(storage)),
      m_batchSize(batchSize),
      m_useEncryption(useEncryption),
      m_useCompression(useCompression),
      m_consumerToken(queue.createConsumerToken())
{
}

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
        size_t entriesDequeued = m_queue.dequeueBatch(batch, m_batchSize, m_consumerToken);
        if (entriesDequeued == 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }

        std::map<std::optional<std::string>, std::vector<LogEntry>> groupedEntries;
        for (const auto &item : batch)
        {
            groupedEntries[item.targetFilename].push_back(item.entry);
        }

        for (const auto &[targetFilename, entries] : groupedEntries)
        {
            std::vector<uint8_t> processedData = LogEntry::serializeBatch(entries);

            // Apply compression only if enabled
            processedData = m_useCompression ? Compression::compress(processedData) : processedData;
            // Apply encryption if enabled
            processedData = m_useEncryption ? crypto.encrypt(std::move(processedData), encryptionKey, dummyIV) : processedData;

            if (targetFilename)
            {
                m_storage->writeToFile(*targetFilename, processedData);
            }
            else
            {
                m_storage->write(processedData);
            }
        }

        batch.clear();
    }
}