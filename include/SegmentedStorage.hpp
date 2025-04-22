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

class SegmentedStorage
{
public:
    SegmentedStorage(const std::string &basePath,
                     const std::string &baseFilename,
                     size_t maxSegmentSize = 100 * 1024 * 1024 // 100 MB default
    );

    ~SegmentedStorage();

    size_t write(const std::vector<uint8_t> &data);
    size_t writeToFile(const std::string &filename, const std::vector<uint8_t> &data);
    void flush();

private:
    std::string m_basePath;
    std::string m_baseFilename;
    size_t m_maxSegmentSize;

    struct SegmentInfo
    {
        std::atomic<size_t> segmentIndex{0};
        std::atomic<size_t> currentOffset{0};
        int fd{-1};
        mutable std::shared_mutex fileMutex; // shared for writes, exclusive for rotate/flush
    };

    std::unordered_map<std::string, std::unique_ptr<SegmentInfo>> m_fileSegments;
    mutable std::shared_mutex m_mapMutex;

    SegmentInfo *getOrCreateSegment(const std::string &filename);
    std::string rotateSegment(const std::string &filename);
    std::string generateSegmentPath(const std::string &filename, size_t segmentIndex) const;
};

#endif