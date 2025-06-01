#ifndef SEGMENTED_STORAGE_HPP
#define SEGMENTED_STORAGE_HPP

#include <string>
#include <vector>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <filesystem>
#include <cstdint>
#include <unordered_map>
#include <fcntl.h>  // for open flags
#include <unistd.h> // for close, pwrite, fsync
#include <chrono>
#include <thread>
#include <stdexcept>
#include <list> // For LRU cache

class SegmentedStorage
{
public:
    SegmentedStorage(const std::string &basePath,
                     const std::string &baseFilename,
                     size_t maxSegmentSize = 100 * 1024 * 1024, // 100 MB default
                     size_t maxAttempts = 5,
                     std::chrono::milliseconds baseRetryDelay = std::chrono::milliseconds(1),
                     size_t maxOpenFiles = 512); // New parameter for max open files

    ~SegmentedStorage();

    size_t write(std::vector<uint8_t> &&data);
    size_t writeToFile(const std::string &filename, std::vector<uint8_t> &&data);
    void flush();

private:
    std::string m_basePath;
    std::string m_baseFilename;
    size_t m_maxSegmentSize;
    size_t m_maxAttempts;
    std::chrono::milliseconds m_baseRetryDelay;
    size_t m_maxOpenFiles; // Max number of file descriptors to keep open

    struct SegmentInfo
    {
        std::atomic<size_t> segmentIndex{0};
        std::atomic<size_t> currentOffset{0};
        std::string currentSegmentPath;      // Full path to the current active segment file
        mutable std::shared_mutex fileMutex; // shared for writes, exclusive for rotate/flush
    };

    std::unordered_map<std::string, std::shared_ptr<SegmentInfo>> m_fileSegments;
    mutable std::shared_mutex m_mapMutex; // Protects m_fileSegments map

    // LRU Cache for file descriptors
    class FdCache
    {
    public:
        FdCache(size_t capacity, SegmentedStorage *parent) : m_capacity(capacity), m_parent(parent) {}

        int get(const std::string &path);
        void put(const std::string &path, int fd);
        void closeFd(const std::string &path);
        void closeAll();

    private:
        size_t m_capacity;
        // List of (path, fd) pairs, ordered from most recently used to least recently used
        std::list<std::pair<std::string, int>> m_lruList;
        // Map from path to iterator in m_lruList for O(1) lookup
        std::unordered_map<std::string, decltype(m_lruList.begin())> m_cacheMap;
        std::mutex m_mutex;         // Protects m_lruList and m_cacheMap
        SegmentedStorage *m_parent; // Pointer to the parent SegmentedStorage for retry helpers
    };

    FdCache m_fdCache;

    std::shared_ptr<SegmentInfo> getOrCreateSegment(const std::string &filename);
    std::string rotateSegment(const std::string &filename);
    std::string generateSegmentPath(const std::string &filename, size_t segmentIndex) const;

    // Helper to get FD for a given path using the cache
    int getFdForPath(const std::string &path);

    // Retry helpers use member-configured parameters
    template <typename Func>
    auto retryWithBackoff(Func &&f)
    {
        for (size_t attempt = 1;; ++attempt)
        {
            try
            {
                return f();
            }
            catch (const std::runtime_error &)
            {
                if (attempt >= m_maxAttempts)
                    throw;
                auto delay = m_baseRetryDelay * (1 << (attempt - 1));
                std::this_thread::sleep_for(delay);
            }
        }
    }

    int openWithRetry(const char *path, int flags, mode_t mode)
    {
        return retryWithBackoff([&]()
                                {
            int fd = ::open(path, flags, mode);
            if (fd < 0) throw std::runtime_error("open failed");
            return fd; });
    }

    size_t pwriteFull(int fd, const uint8_t *buf, size_t count, off_t offset)
    {
        size_t total = 0;
        while (total < count)
        {
            ssize_t written = ::pwrite(fd, buf + total, count - total, offset + total);
            if (written < 0)
            {
                if (errno == EINTR)
                    continue;
                throw std::runtime_error("pwrite failed");
            }
            total += written;
        }
        return total;
    }

    void fsyncRetry(int fd)
    {
        retryWithBackoff([&]()
                         {
            if (::fsync(fd) < 0) throw std::runtime_error("fsync failed");
            return 0; });
    }
};

#endif