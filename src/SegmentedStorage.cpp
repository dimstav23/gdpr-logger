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
    getOrCreateSegment(m_baseFilename);
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
    return writeToFile(m_baseFilename, data);
}

size_t SegmentedStorage::writeToFile(const std::string &filename, const std::vector<uint8_t> &data)
{
    size_t size = data.size();
    if (size == 0)
        return 0;

    SegmentInfo *segment = getOrCreateSegment(filename);

    // First, check if we need to rotate without holding the mutex
    bool needRotation = false;
    {
        size_t currentOffset = segment->currentOffset.load(std::memory_order_acquire);
        needRotation = (currentOffset + size > m_maxSegmentSize);
    }

    // If rotation is needed, handle it separately
    if (needRotation)
    {
        std::lock_guard<std::mutex> rotLock(segment->fileMutex);
        // Double-check if rotation is still needed after acquiring the lock
        if (segment->currentOffset.load(std::memory_order_acquire) + size > m_maxSegmentSize)
        {
            rotateSegment(filename);
        }
    }

    // Now proceed with writing
    std::lock_guard<std::mutex> writeLock(segment->fileMutex);

    // Check again if rotation is needed (another thread might have written data)
    if (segment->currentOffset.load(std::memory_order_acquire) + size > m_maxSegmentSize)
    {
        rotateSegment(filename);
    }

    // Write the data
    size_t writeOffset = segment->currentOffset.fetch_add(size, std::memory_order_acq_rel);
    segment->fileStream->seekp(writeOffset, std::ios::beg);
    segment->fileStream->write(reinterpret_cast<const char *>(data.data()), size);

    // Flush if needed
    if ((writeOffset + size) % m_bufferSize == 0 ||
        (writeOffset + size > m_maxSegmentSize - m_bufferSize))
    {
        segment->fileStream->flush();
    }

    return size;
}

void SegmentedStorage::flush()
{
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
    return getCurrentSegmentIndex(m_baseFilename);
}

size_t SegmentedStorage::getCurrentSegmentIndex(const std::string &filename) const
{
    std::shared_lock<std::shared_mutex> lock(m_mapMutex);
    auto it = m_fileSegments.find(filename);
    if (it != m_fileSegments.end())
    {
        return it->second->segmentIndex.load(std::memory_order_acquire);
    }
    throw std::runtime_error("Segment not found for filename: " + filename);
}

size_t SegmentedStorage::getCurrentSegmentSize() const
{
    return getCurrentSegmentSize(m_baseFilename);
}

size_t SegmentedStorage::getCurrentSegmentSize(const std::string &filename) const
{
    std::shared_lock<std::shared_mutex> lock(m_mapMutex);
    auto it = m_fileSegments.find(filename);
    if (it != m_fileSegments.end())
    {
        return it->second->currentOffset.load(std::memory_order_acquire);
    }
    throw std::runtime_error("Segment not found for filename: " + filename);
}

std::string SegmentedStorage::getCurrentSegmentPath() const
{
    return getCurrentSegmentPath(m_baseFilename);
}

std::string SegmentedStorage::getCurrentSegmentPath(const std::string &filename) const
{
    std::shared_lock<std::shared_mutex> lock(m_mapMutex);
    auto it = m_fileSegments.find(filename);
    if (it != m_fileSegments.end())
    {
        size_t index = it->second->segmentIndex.load(std::memory_order_acquire);
        return generateSegmentPath(filename, index);
    }
    throw std::runtime_error("Segment not found for filename: " + filename);
}

std::string SegmentedStorage::rotateSegment(const std::string &filename)
{
    // Important: This method assumes that segment->fileMutex is already locked by the caller
    SegmentInfo *segment = getOrCreateSegment(filename);

    // Perform rotation
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
    // Try read lock first for better performance
    {
        std::shared_lock<std::shared_mutex> readLock(m_mapMutex);
        auto it = m_fileSegments.find(filename);
        if (it != m_fileSegments.end())
        {
            return it->second.get();
        }
    }

    // If not found, acquire write lock and create
    std::unique_lock<std::shared_mutex> writeLock(m_mapMutex);

    // Double-check in case another thread created it
    auto it = m_fileSegments.find(filename);
    if (it != m_fileSegments.end())
    {
        return it->second.get();
    }

    // Create new segment
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