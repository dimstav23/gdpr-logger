#include "Writer.hpp"
#include <iostream>
#include <chrono>

Writer::Writer(ILogQueue &logQueue, size_t batchSize)
    : m_logQueue(logQueue), m_batchSize(batchSize) {}

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

    m_writerThread = std::make_unique<std::thread>(&Writer::processLogEntries, this);
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
    std::vector<LogEntry> batch;
    batch.reserve(m_batchSize);

    while (m_running)
    {
        // Try to dequeue a batch of log entries
        size_t entriesDequeued = m_logQueue.dequeueBatch(batch, m_batchSize);

        if (entriesDequeued > 0)
        {
            // Simulate writing to disk by logging to console
            std::cout << "Writing batch of " << entriesDequeued << " log entries:" << std::endl;
            for (const auto &entry : batch)
            {
                std::cout << "Log Entry - Data Location: " << entry.getDataLocation()
                          << ", Action: " << actionTypeToString(entry.getActionType())
                          << ", Data Location: " << entry.getDataSubjectId() << std::endl;
            }

            // Clear the batch for next iteration
            batch.clear();
        }
        else
        {
            // If no entries, wait a bit to avoid busy-waiting
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}