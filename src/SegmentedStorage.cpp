#include "SegmentedStorage.hpp"

SegmentedStorage::SegmentedStorage(const std::string &basePath,
                                   const std::string &baseFilename,
                                   size_t maxSegmentSize,
                                   size_t maxAttempts,
                                   std::chrono::milliseconds baseRetryDelay)
    : m_basePath(basePath),
      m_baseFilename(baseFilename),
      m_maxSegmentSize(maxSegmentSize),
      m_maxAttempts(maxAttempts),
      m_baseRetryDelay(baseRetryDelay)
{
    std::filesystem::create_directories(m_basePath);
    getOrCreateSegment(m_baseFilename);
}

SegmentedStorage::~SegmentedStorage()
{
    std::unique_lock<std::shared_mutex> mapLock(m_mapMutex);
    for (auto &pair : m_fileSegments)
    {
        // exclusive lock to prevent concurrent writes
        std::unique_lock<std::shared_mutex> lock(pair.second->fileMutex);
        if (pair.second->fd >= 0)
        {
            fsyncRetry(pair.second->fd);
            ::close(pair.second->fd);
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

    std::shared_ptr<SegmentInfo> segment = getOrCreateSegment(filename);
    size_t writeOffset;
    int currentFd;

    // This loop handles race conditions around rotation
    while (true)
    {
        // First check if we need to rotate WITHOUT reserving space
        size_t currentOffset = segment->currentOffset.load(std::memory_order_acquire);
        if (currentOffset + size > m_maxSegmentSize)
        {
            std::unique_lock<std::shared_mutex> rotLock(segment->fileMutex);
            // Double-check if rotation is still needed
            if (segment->currentOffset.load(std::memory_order_acquire) + size > m_maxSegmentSize)
            {
                rotateSegment(filename);
                continue;
            }
        }

        // Now safely reserve space
        writeOffset = segment->currentOffset.fetch_add(size, std::memory_order_acq_rel);

        // Double-check we didn't cross the boundary after reservation
        if (writeOffset + size > m_maxSegmentSize)
        {
            // Someone other thread increased the offset past our threshold, try again
            continue;
        }

        // Capture file descriptor to ensure we use the same one consistently
        std::shared_lock<std::shared_mutex> readLock(segment->fileMutex);
        currentFd = segment->fd;

        // If fd is invalid (rotation happened), try again
        if (currentFd < 0)
        {
            continue;
        }

        // We have a valid offset and fd, so we can proceed with the write
        break;
    }

    // Write under shared lock to prevent racing with rotate/close
    {
        std::shared_lock<std::shared_mutex> writeLock(segment->fileMutex);

        // Double-check that fd hasn't changed (which would indicate rotation happened)
        if (segment->fd != currentFd)
        {
            // If rotation happened during this time, retry the write
            return writeToFile(filename, data);
        }

        pwriteFull(currentFd, data.data(), size, static_cast<off_t>(writeOffset));
    }

    return size;
}

void SegmentedStorage::flush()
{
    std::shared_lock<std::shared_mutex> mapLock(m_mapMutex);
    for (auto &pair : m_fileSegments)
    {
        std::unique_lock<std::shared_mutex> lock(pair.second->fileMutex);
        if (pair.second->fd >= 0)
        {
            fsyncRetry(pair.second->fd);
        }
    }
}

std::string SegmentedStorage::rotateSegment(const std::string &filename)
{
    std::shared_ptr<SegmentInfo> segment = getOrCreateSegment(filename);

    // exclusive lock assumed
    if (segment->fd >= 0)
    {
        fsyncRetry(segment->fd);
        ::close(segment->fd);
    }

    size_t newIndex = segment->segmentIndex.fetch_add(1, std::memory_order_acq_rel) + 1;
    segment->currentOffset.store(0, std::memory_order_release);
    std::string newPath = generateSegmentPath(filename, newIndex);

    int fd = openWithRetry(newPath.c_str(), O_CREAT | O_RDWR, 0644);
    segment->fd = fd;

    return newPath;
}

std::shared_ptr<SegmentedStorage::SegmentInfo> SegmentedStorage::getOrCreateSegment(const std::string &filename)
{
    {
        std::shared_lock<std::shared_mutex> readLock(m_mapMutex);
        auto it = m_fileSegments.find(filename);
        if (it != m_fileSegments.end())
            return it->second;
    }

    std::unique_lock<std::shared_mutex> writeLock(m_mapMutex);
    auto it = m_fileSegments.find(filename);
    if (it != m_fileSegments.end())
        return it->second;

    auto segmentInfo = std::make_shared<SegmentInfo>();
    std::string segmentPath = generateSegmentPath(filename, 0);
    int fd = openWithRetry(segmentPath.c_str(), O_CREAT | O_RDWR, 0644);
    segmentInfo->fd = fd;
    m_fileSegments[filename] = segmentInfo;
    return segmentInfo;
}

std::string SegmentedStorage::generateSegmentPath(const std::string &filename, size_t segmentIndex) const
{
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    std::tm time_info;

    // Linux-specific thread-safe version of localtime
    localtime_r(&now_time_t, &time_info);

    std::stringstream ss;
    ss << m_basePath << "/";
    ss << filename << "_";
    ss << std::put_time(&time_info, "%Y%m%d_%H%M%S") << "_";
    ss << std::setw(6) << std::setfill('0') << segmentIndex << ".log";
    return ss.str();
}