#include "SegmentedStorage.hpp"
#include <chrono>
#include <iomanip>
#include <sstream>

SegmentedStorage::SegmentedStorage(const std::string &basePath,
                                   const std::string &baseFilename,
                                   size_t maxSegmentSize,
                                   size_t bufferSize)
    : m_basePath(basePath), m_baseFilename(baseFilename), m_maxSegmentSize(maxSegmentSize), m_bufferSize(bufferSize)
{
    std::filesystem::create_directories(m_basePath);
    m_currentFile = std::make_unique<std::ofstream>(
        generateSegmentPath(m_baseFilename, 0),
        std::ios::binary | std::ios::trunc);
    if (!m_currentFile->is_open())
    {
        throw std::runtime_error("Failed to open default segment file.");
    }
    m_currentFile->rdbuf()->pubsetbuf(nullptr, 0);
}

SegmentedStorage::~SegmentedStorage()
{
    flush();
    std::unique_lock<std::shared_mutex> lock(m_mapMutex);
    for (auto &pair : m_fileSegments)
    {
        if (pair.second->fileStream && pair.second->fileStream->is_open())
        {
            pair.second->fileStream->flush();
            pair.second->fileStream->close();
        }
    }
}

size_t SegmentedStorage::write(const std::vector<uint8_t> &data)
{
    size_t size = data.size();
    if (size == 0)
        return 0;

    std::unique_lock<std::mutex> lock(m_defaultFileMutex);
    if (m_currentOffset.load(std::memory_order_acquire) + size > m_maxSegmentSize)
    {
        rotateSegment();
    }
    size_t writeOffset = m_currentOffset.fetch_add(size, std::memory_order_acq_rel);
    m_currentFile->seekp(writeOffset, std::ios::beg);
    m_currentFile->write(reinterpret_cast<const char *>(data.data()), size);
    if ((writeOffset + size) % m_bufferSize == 0 ||
        (writeOffset + size > m_maxSegmentSize - m_bufferSize))
    {
        m_currentFile->flush();
    }
    return size;
}

size_t SegmentedStorage::writeToFile(const std::string &filename, const std::vector<uint8_t> &data)
{
    SegmentInfo *segment = getOrCreateSegment(filename);
    size_t size = data.size();
    if (size == 0)
        return 0;

    std::unique_lock<std::mutex> lock(segment->fileMutex);
    if (segment->currentOffset.load(std::memory_order_acquire) + size > m_maxSegmentSize)
    {
        rotateSegment(filename);
    }
    size_t writeOffset = segment->currentOffset.fetch_add(size, std::memory_order_acq_rel);
    segment->fileStream->seekp(writeOffset, std::ios::beg);
    segment->fileStream->write(reinterpret_cast<const char *>(data.data()), size);
    if ((writeOffset + size) % m_bufferSize == 0 ||
        (writeOffset + size > m_maxSegmentSize - m_bufferSize))
    {
        segment->fileStream->flush();
    }
    return size;
}

void SegmentedStorage::flush()
{
    std::unique_lock<std::mutex> lock(m_defaultFileMutex);
    if (m_currentFile && m_currentFile->is_open())
    {
        m_currentFile->flush();
    }
    lock.unlock();

    std::shared_lock<std::shared_mutex> mapLock(m_mapMutex);
    for (auto &pair : m_fileSegments)
    {
        std::unique_lock<std::mutex> fileLock(pair.second->fileMutex);
        if (pair.second->fileStream && pair.second->fileStream->is_open())
        {
            pair.second->fileStream->flush();
        }
    }
}

void SegmentedStorage::flushFile(const std::string &filename)
{
    SegmentInfo *segment = getOrCreateSegment(filename);
    std::unique_lock<std::mutex> lock(segment->fileMutex);
    if (segment->fileStream && segment->fileStream->is_open())
    {
        segment->fileStream->flush();
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
    return generateSegmentPath(m_baseFilename, m_currentSegmentIndex.load(std::memory_order_acquire));
}

std::string SegmentedStorage::rotateSegment()
{
    std::unique_lock<std::mutex> lock(m_defaultFileMutex);
    if (m_currentFile && m_currentFile->is_open())
    {
        m_currentFile->flush();
        m_currentFile->close();
    }
    size_t newIndex = m_currentSegmentIndex.fetch_add(1, std::memory_order_acq_rel) + 1;
    m_currentOffset.store(0, std::memory_order_release);
    std::string newPath = generateSegmentPath(m_baseFilename, newIndex);
    m_currentFile = std::make_unique<std::ofstream>(newPath, std::ios::binary | std::ios::trunc);
    if (!m_currentFile->is_open())
    {
        throw std::runtime_error("Failed to open new default segment file.");
    }
    m_currentFile->rdbuf()->pubsetbuf(nullptr, 0);
    return newPath;
}

std::string SegmentedStorage::rotateSegment(const std::string &filename)
{
    SegmentInfo *segment = getOrCreateSegment(filename);
    std::unique_lock<std::mutex> lock(segment->fileMutex);
    if (segment->fileStream && segment->fileStream->is_open())
    {
        segment->fileStream->flush();
        segment->fileStream->close();
    }
    size_t newIndex = segment->segmentIndex.fetch_add(1, std::memory_order_acq_rel) + 1;
    segment->currentOffset.store(0, std::memory_order_release);
    std::string newPath = generateSegmentPath(filename, newIndex);
    segment->fileStream = std::make_unique<std::ofstream>(newPath, std::ios::binary | std::ios::trunc);
    if (!segment->fileStream->is_open())
    {
        throw std::runtime_error("Failed to open new segment file for " + filename);
    }
    segment->fileStream->rdbuf()->pubsetbuf(nullptr, 0);
    return newPath;
}

SegmentedStorage::SegmentInfo *SegmentedStorage::getOrCreateSegment(const std::string &filename)
{
    {
        std::shared_lock<std::shared_mutex> readLock(m_mapMutex);
        auto it = m_fileSegments.find(filename);
        if (it != m_fileSegments.end())
        {
            return it->second.get();
        }
    }
    std::unique_lock<std::shared_mutex> writeLock(m_mapMutex);
    auto it = m_fileSegments.find(filename);
    if (it != m_fileSegments.end())
    {
        return it->second.get();
    }
    auto segmentInfo = std::make_unique<SegmentInfo>();
    std::string segmentPath = generateSegmentPath(filename, 0);
    segmentInfo->fileStream = std::make_unique<std::ofstream>(segmentPath, std::ios::binary | std::ios::trunc);
    if (!segmentInfo->fileStream->is_open())
    {
        throw std::runtime_error("Failed to open segment file: " + segmentPath);
    }
    segmentInfo->fileStream->rdbuf()->pubsetbuf(nullptr, 0);
    m_fileSegments[filename] = std::move(segmentInfo);
    return m_fileSegments[filename].get();
}

std::string SegmentedStorage::generateSegmentPath(const std::string &filename, size_t segmentIndex) const
{
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << m_basePath << "/";
    ss << filename << "_";
    ss << std::put_time(std::localtime(&now_time_t), "%Y%m%d_%H%M%S") << "_";
    ss << std::setw(6) << std::setfill('0') << segmentIndex << ".log";
    return ss.str();
}