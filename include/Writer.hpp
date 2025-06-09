#ifndef WRITER_HPP
#define WRITER_HPP

#include <thread>
#include <atomic>
#include <memory>
#include <vector>
#include "QueueItem.hpp"
#include "BufferQueue.hpp"
#include "SegmentedStorage.hpp"

class Writer
{
public:
    explicit Writer(BufferQueue &queue,
                    std::shared_ptr<SegmentedStorage> storage,
                    size_t batchSize = 100,
                    bool useEncryption = true,
                    int m_compressionLevel = 9);

    ~Writer();

    void start();
    void stop();
    bool isRunning() const;

private:
    void processLogEntries();

    BufferQueue &m_queue;
    std::shared_ptr<SegmentedStorage> m_storage;
    std::unique_ptr<std::thread> m_writerThread;
    std::atomic<bool> m_running{false};
    const size_t m_batchSize;
    const bool m_useEncryption;
    const int m_compressionLevel;

    BufferQueue::ConsumerToken m_consumerToken;
};
#endif