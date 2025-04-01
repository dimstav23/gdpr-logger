#include "SegmentedStorage.hpp"
#include <iostream>
#include <iomanip>
#include <sstream>
#include <chrono>

SegmentedStorage::SegmentedStorage(const std::string &basePath,
                                   const std::string &baseFilename,
                                   size_t maxSegmentSize,
                                   size_t bufferSize)
    : m_basePath(basePath), m_baseFilename(baseFilename), m_maxSegmentSize(maxSegmentSize), m_bufferSize(bufferSize), m_currentSegmentIndex(0), m_currentOffset(0)
{
    // Ensure the directory exists
    std::filesystem::create_directories(m_basePath);

    // Create the initial segment
    createNewSegment();
}

SegmentedStorage::~SegmentedStorage()
{
    // Make sure any buffered data is flushed and file is properly closed
    if (m_currentFile && m_currentFile->is_open())
    {
        m_currentFile->flush();
        m_currentFile->close();
    }
}

size_t SegmentedStorage::write(const uint8_t *data, size_t size)
{
    if (size == 0)
        return 0;

    std::unique_lock<std::mutex> lock(m_fileMutex);

    // Check if this write would exceed the max segment size
    if (m_currentOffset.load(std::memory_order_acquire) + size > m_maxSegmentSize)
    {
        // We need to rotate the segment
        rotateSegment();
    }

    // Reserve space for our write by updating the atomic offset
    size_t writeOffset = m_currentOffset.fetch_add(size, std::memory_order_acq_rel);

    // Seek to the correct position
    m_currentFile->seekp(writeOffset, std::ios::beg);

    // Write the data
    m_currentFile->write(reinterpret_cast<const char *>(data), size);

    // Flush if we hit the buffer threshold or we're close to max segment size
    if ((writeOffset + size) % m_bufferSize == 0 ||
        (writeOffset + size > m_maxSegmentSize - m_bufferSize))
    {
        m_currentFile->flush();
    }

    lock.unlock();
    return size;
}

size_t SegmentedStorage::write(const std::vector<uint8_t> &data)
{
    // Delegate to the raw pointer version
    return write(data.data(), data.size());
}

void SegmentedStorage::flush()
{
    std::lock_guard<std::mutex> lock(m_fileMutex);
    if (m_currentFile && m_currentFile->is_open())
    {
        m_currentFile->flush();
    }
}

size_t SegmentedStorage::getCurrentSegmentIndex() const
{
    return m_currentSegmentIndex.load(std::memory_order_acquire);
}

size_t SegmentedStorage::getCurrentSegmentSize() const
{
    return m_currentOffset.load(std::memory_order_acquire);
}

std::string SegmentedStorage::getCurrentSegmentPath() const
{
    return generateSegmentPath(m_currentSegmentIndex.load(std::memory_order_acquire));
}

std::string SegmentedStorage::rotateSegment()
{
    // Flush and close the current segment
    if (m_currentFile && m_currentFile->is_open())
    {
        m_currentFile->flush();
        m_currentFile->close();
    }

    // Increment segment index and reset offset
    m_currentSegmentIndex.fetch_add(1, std::memory_order_acq_rel);
    m_currentOffset.store(0, std::memory_order_release);

    // Create a new segment
    createNewSegment();

    return getCurrentSegmentPath();
}

void SegmentedStorage::createNewSegment()
{
    std::string segmentPath = generateSegmentPath(m_currentSegmentIndex.load(std::memory_order_acquire));

    // Create a new file stream
    m_currentFile = std::make_unique<std::ofstream>(
        segmentPath,
        std::ios::binary | std::ios::trunc);

    if (!m_currentFile->is_open())
    {
        throw std::runtime_error("Failed to open segment file: " + segmentPath);
    }

    // Disable internal buffering for direct I/O
    m_currentFile->rdbuf()->pubsetbuf(nullptr, 0);
}

std::string SegmentedStorage::generateSegmentPath(size_t segmentIndex) const
{
    // Generate a timestamp for the filename
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);

    std::stringstream ss;
    ss << m_basePath << "/";
    ss << m_baseFilename << "_";
    ss << std::put_time(std::localtime(&now_time_t), "%Y%m%d_%H%M%S") << "_";
    ss << std::setw(6) << std::setfill('0') << segmentIndex << ".log";

    return ss.str();
}