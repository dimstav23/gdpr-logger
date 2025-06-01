#include "SegmentedStorage.hpp"
#include <iomanip>
#include <sstream>

SegmentedStorage::SegmentedStorage(const std::string &basePath,
                                   const std::string &baseFilename,
                                   size_t maxSegmentSize,
                                   size_t maxAttempts,
                                   std::chrono::milliseconds baseRetryDelay,
                                   size_t maxOpenFiles)
    : m_basePath(basePath),
      m_baseFilename(baseFilename),
      m_maxSegmentSize(maxSegmentSize),
      m_maxAttempts(maxAttempts),
      m_baseRetryDelay(baseRetryDelay),
      m_maxOpenFiles(maxOpenFiles),
      m_fdCache(maxOpenFiles, this) // Initialize FdCache with maxOpenFiles and pointer to SegmentedStorage
{
    std::filesystem::create_directories(m_basePath);
    getOrCreateSegment(m_baseFilename);
}

SegmentedStorage::~SegmentedStorage()
{
    m_fdCache.closeAll();
}

// FdCache methods
int SegmentedStorage::FdCache::get(const std::string &path)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    auto it = m_cacheMap.find(path);
    if (it != m_cacheMap.end())
    {
        // Found in cache, move to front (most recently used)
        m_lruList.splice(m_lruList.begin(), m_lruList, it->second);
        return it->second->second;
    }

    // Not in cache, need to open and add
    if (m_lruList.size() >= m_capacity)
    {
        // Cache is full, evict least recently used
        const auto &lru_pair = m_lruList.back();
        ::close(lru_pair.second); // Close the actual FD
        m_cacheMap.erase(lru_pair.first);
        m_lruList.pop_back();
    }

    // Open the file
    int fd = m_parent->openWithRetry(path.c_str(), O_CREAT | O_RDWR, 0644);
    m_lruList.emplace_front(path, fd);
    m_cacheMap[path] = m_lruList.begin();
    return fd;
}

void SegmentedStorage::FdCache::put(const std::string &path, int fd)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    // This `put` is mainly for when `rotateSegment` provides a new FD.
    // It should replace an existing entry or add a new one.
    auto it = m_cacheMap.find(path);
    if (it != m_cacheMap.end())
    {
        // Update existing entry
        it->second->second = fd;
        m_lruList.splice(m_lruList.begin(), m_lruList, it->second);
    }
    else
    {
        // Add new entry, check capacity
        if (m_lruList.size() >= m_capacity)
        {
            const auto &lru_pair = m_lruList.back();
            ::close(lru_pair.second);
            m_cacheMap.erase(lru_pair.first);
            m_lruList.pop_back();
        }
        m_lruList.emplace_front(path, fd);
        m_cacheMap[path] = m_lruList.begin();
    }
}

void SegmentedStorage::FdCache::closeFd(const std::string &path)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_cacheMap.find(path);
    if (it != m_cacheMap.end())
    {
        // Found, close it and remove from cache
        m_parent->fsyncRetry(it->second->second); // Ensure data is flushed before closing
        ::close(it->second->second);
        m_lruList.erase(it->second);
        m_cacheMap.erase(it);
    }
}

void SegmentedStorage::FdCache::closeAll()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    for (const auto &pair : m_lruList)
    {
        if (pair.second >= 0)
        {
            m_parent->fsyncRetry(pair.second); // Ensure data is flushed before closing
            ::close(pair.second);
        }
    }
    m_lruList.clear();
    m_cacheMap.clear();
}

// Helper to get FD for a given path using the cache
int SegmentedStorage::getFdForPath(const std::string &path)
{
    return m_fdCache.get(path);
}

size_t SegmentedStorage::write(std::vector<uint8_t> &&data)
{
    return writeToFile(m_baseFilename, std::move(data));
}

size_t SegmentedStorage::writeToFile(const std::string &filename, std::vector<uint8_t> &&data)
{
    size_t size = data.size();
    if (size == 0)
        return 0;

    std::shared_ptr<SegmentInfo> segment = getOrCreateSegment(filename);
    size_t writeOffset;
    int currentFd;
    std::string currentSegmentPath; // To hold the path of the file we are writing to

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
                // After rotation, segment->currentSegmentPath has been updated.
                // We need to re-evaluate the write.
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

        // Capture file path to ensure we use the same one consistently
        // We acquire a shared lock here to ensure that `currentSegmentPath`
        // doesn't change while we read it and use it to get the FD.
        std::shared_lock<std::shared_mutex> readLock(segment->fileMutex);
        currentSegmentPath = segment->currentSegmentPath;

        // Get the file descriptor for the current segment path
        currentFd = getFdForPath(currentSegmentPath);

        // If fd is invalid (e.g., race condition where file was closed or rotated
        // immediately after we got its path but before we got the FD, this is unlikely
        // with the LRU but good to keep the check), try again
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

        // Double-check that the file path hasn't changed (which would indicate rotation happened)
        // AND that the fd we are using is still valid for that path.
        // The LRU cache ensures we get a valid FD for currentSegmentPath.
        if (segment->currentSegmentPath != currentSegmentPath)
        {
            // If rotation happened during this time, retry the write with the new segment
            return writeToFile(filename, std::move(data));
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
        if (!pair.second->currentSegmentPath.empty())
        {
            int fd_to_flush = m_fdCache.get(pair.second->currentSegmentPath); // Get FD from cache (or open it)
            if (fd_to_flush >= 0)
            {
                fsyncRetry(fd_to_flush);
            }
        }
    }
}

std::string SegmentedStorage::rotateSegment(const std::string &filename)
{
    std::shared_ptr<SegmentInfo> segment = getOrCreateSegment(filename);

    // exclusive lock assumed by the caller (writeToFile)

    // Close the old file descriptor via the cache's mechanism
    if (!segment->currentSegmentPath.empty())
    {
        m_fdCache.closeFd(segment->currentSegmentPath);
    }

    size_t newIndex = segment->segmentIndex.fetch_add(1, std::memory_order_acq_rel) + 1;
    segment->currentOffset.store(0, std::memory_order_release);
    std::string newPath = generateSegmentPath(filename, newIndex);

    // Update the segment's path and add the new FD to the cache
    segment->currentSegmentPath = newPath;
    m_fdCache.get(newPath); // This will open and put into cache

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
    // Double-check after acquiring write lock
    auto it = m_fileSegments.find(filename);
    if (it != m_fileSegments.end())
        return it->second;

    auto segmentInfo = std::make_shared<SegmentInfo>();
    std::string segmentPath = generateSegmentPath(filename, 0);

    // Initialize the segment's path
    segmentInfo->currentSegmentPath = segmentPath;

    // The FD will be managed by the cache, we don't store it in SegmentInfo directly
    // Ensure the file is created and potentially opened and put into the cache
    m_fdCache.get(segmentPath); // This will open and put into cache if not present

    m_fileSegments[filename] = segmentInfo;
    return segmentInfo;
}

std::string SegmentedStorage::generateSegmentPath(const std::string &filename, size_t segmentIndex) const
{
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    std::tm time_info;

    // Linux-specific thread-safe version of localtime
    // This function can fail. For robustness in production, you might want to check its return value.
    localtime_r(&now_time_t, &time_info);

    std::stringstream ss;
    ss << m_basePath << "/";
    ss << filename << "_";
    ss << std::put_time(&time_info, "%Y%m%d_%H%M%S") << "_";
    ss << std::setw(6) << std::setfill('0') << segmentIndex << ".log";
    return ss.str();
}