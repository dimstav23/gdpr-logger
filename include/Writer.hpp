#ifndef WRITER_HPP
#define WRITER_HPP

#include <thread>
#include <atomic>
#include <memory>
#include <vector>
#include "LogEntry.hpp"
#include "LockFreeBuffer.hpp"

class Writer
{
public:
    explicit Writer(ILogQueue &logQueue, size_t batchSize = 100);

    ~Writer();

    void start();

    void stop();

    bool isRunning() const;

private:
    void processLogEntries();

    ILogQueue &m_logQueue;

    std::unique_ptr<std::thread> m_writerThread;

    std::atomic<bool> m_running{false};

    const size_t m_batchSize;
};

#endif