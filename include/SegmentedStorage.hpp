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

class SegmentedStorage
{
public:
    SegmentedStorage(const std::string &basePath,
                     const std::string &baseFilename,
                     size_t maxSegmentSize = 100 * 1024 * 1024, // 100 MB default
                     size_t maxAttempts = 5,
                     std::chrono::milliseconds baseRetryDelay = std::chrono::milliseconds(1));

    ~SegmentedStorage();

    size_t write(const std::vector<uint8_t> &data);
    size_t writeToFile(const std::string &filename, const std::vector<uint8_t> &data);
    void flush();

private:
    std::string m_basePath;
    std::string m_baseFilename;
    size_t m_maxSegmentSize;
    size_t m_maxAttempts;
    std::chrono::milliseconds m_baseRetryDelay;

    struct SegmentInfo
    {
        std::atomic<size_t> segmentIndex{0};
        std::atomic<size_t> currentOffset{0};
        int fd{-1};
        mutable std::shared_mutex fileMutex; // shared for writes, exclusive for rotate/flush
    };

    std::unordered_map<std::string, std::shared_ptr<SegmentInfo>> m_fileSegments;
    mutable std::shared_mutex m_mapMutex;

    std::shared_ptr<SegmentInfo> getOrCreateSegment(const std::string &filename);
    std::string rotateSegment(const std::string &filename);
    std::string generateSegmentPath(const std::string &filename, size_t segmentIndex) const;

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