#include "SegmentedStorage.hpp"
#include <chrono>
#include <iomanip>
#include <sstream>
#include <stdexcept>

SegmentedStorage::SegmentedStorage(const std::string &basePath,
                                   const std::string &baseFilename,
                                   size_t maxSegmentSize)
    : m_basePath(basePath),
      m_baseFilename(baseFilename),
      m_maxSegmentSize(maxSegmentSize)
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
            ::fsync(pair.second->fd);
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

    SegmentInfo *segment = getOrCreateSegment(filename);
    size_t writeOffset;
    int currentFd;

    // This loop handles the case where rotation happens between offset reservation and writing
    while (true)
    {
        // Reserve byte range atomically
        writeOffset = segment->currentOffset.fetch_add(size, std::memory_order_acq_rel);

        // Need to rotate?
        if (writeOffset + size > m_maxSegmentSize)
        {
            std::unique_lock<std::shared_mutex> rotLock(segment->fileMutex);

            // Double-check if we still need to rotate (another thread might have already rotated)
            if (segment->currentOffset.load(std::memory_order_acquire) > m_maxSegmentSize)
            {
                // Reset the offset counter for the new segment
                rotateSegment(filename);

                // Try again with the new segment
                continue;
            }
        }

        // Capture file descriptor to ensure we use the same one consistently
        std::shared_lock<std::shared_mutex> readLock(segment->fileMutex);
        currentFd = segment->fd;

        // If we can't fit the data or the fd changed (rotation happened), try again
        if (writeOffset + size > m_maxSegmentSize || currentFd < 0)
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

        ssize_t written = ::pwrite(segment->fd,
                                   reinterpret_cast<const void *>(data.data()),
                                   size,
                                   static_cast<off_t>(writeOffset));
        if (written < 0 || static_cast<size_t>(written) != size)
        {
            throw std::runtime_error("pwrite failed or partial write");
        }
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
            ::fsync(pair.second->fd);
        }
    }
}

std::string SegmentedStorage::rotateSegment(const std::string &filename)
{
    SegmentInfo *segment = getOrCreateSegment(filename);

    // exclusive lock assumed
    if (segment->fd >= 0)
    {
        ::fsync(segment->fd);
        ::close(segment->fd);
    }

    size_t newIndex = segment->segmentIndex.fetch_add(1, std::memory_order_acq_rel) + 1;
    segment->currentOffset.store(0, std::memory_order_release);
    std::string newPath = generateSegmentPath(filename, newIndex);

    int fd = ::open(newPath.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd < 0)
    {
        throw std::runtime_error("Failed to open new segment file: " + newPath);
    }
    segment->fd = fd;

    return newPath;
}

SegmentedStorage::SegmentInfo *SegmentedStorage::getOrCreateSegment(const std::string &filename)
{
    {
        std::shared_lock<std::shared_mutex> readLock(m_mapMutex);
        auto it = m_fileSegments.find(filename);
        if (it != m_fileSegments.end())
            return it->second.get();
    }

    std::unique_lock<std::shared_mutex> writeLock(m_mapMutex);
    auto it = m_fileSegments.find(filename);
    if (it != m_fileSegments.end())
        return it->second.get();

    auto segmentInfo = std::make_unique<SegmentInfo>();
    std::string segmentPath = generateSegmentPath(filename, 0);
    int fd = ::open(segmentPath.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd < 0)
        throw std::runtime_error("Failed to open segment file: " + segmentPath);

    segmentInfo->fd = fd;
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