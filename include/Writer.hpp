#ifndef WRITER_HPP
#define WRITER_HPP

#include <thread>
#include <atomic>
#include <memory>
#include <vector>
#include "LogEntry.hpp"
#include "LockFreeQueue.hpp"
#include "SegmentedStorage.hpp"

class Writer
{
public:
    explicit Writer(LockFreeQueue &logQueue,
                    std::shared_ptr<SegmentedStorage> storage,
                    size_t batchSize = 100);

    ~Writer();

    void start();

    void stop();

    bool isRunning() const;

private:
    void processLogEntries();

    LockFreeQueue &m_logQueue;
    std::shared_ptr<SegmentedStorage> m_storage;

    std::unique_ptr<std::thread> m_writerThread;

    std::atomic<bool> m_running{false};

    const size_t m_batchSize;
};

#endif