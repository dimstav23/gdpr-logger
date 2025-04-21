#ifndef SEGMENTED_STORAGE_HPP
#define SEGMENTED_STORAGE_HPP

#include <string>
#include <vector>
#include <atomic>
#include <mutex>
#include <filesystem>
#include <cstdint>
#include <memory>
#include <fstream>
#include <unordered_map>
#include <shared_mutex>

class SegmentedStorage
{
public:
    SegmentedStorage(const std::string &basePath,
                     const std::string &baseFilename,
                     size_t maxSegmentSize = 100 * 1024 * 1024, // 100 MB default
                     size_t bufferSize = 64 * 1024);            // 64 KB buffer default

    ~SegmentedStorage();

    size_t write(const std::vector<uint8_t> &data);

    size_t writeToFile(const std::string &filename, const std::vector<uint8_t> &data);

    void flush();

    void flushFile(const std::string &filename);

    size_t getCurrentSegmentIndex() const;
    size_t getCurrentSegmentIndex(const std::string &filename) const;

    size_t getCurrentSegmentSize() const;
    size_t getCurrentSegmentSize(const std::string &filename) const;

    std::string getCurrentSegmentPath() const;
    std::string getCurrentSegmentPath(const std::string &filename) const;

    std::string rotateSegment();
    std::string rotateSegment(const std::string &filename);

private:
    std::string m_basePath;
    std::string m_baseFilename;
    size_t m_maxSegmentSize;
    size_t m_bufferSize;

    struct SegmentInfo
    {
        std::atomic<size_t> segmentIndex{0};
        std::atomic<size_t> currentOffset{0};
        std::unique_ptr<std::ofstream> fileStream;
        std::mutex fileMutex;
    };

    std::unordered_map<std::string, std::unique_ptr<SegmentInfo>> m_fileSegments;
    mutable std::shared_mutex m_mapMutex;

    SegmentInfo *getOrCreateSegment(const std::string &filename);

    std::string generateSegmentPath(const std::string &filename, size_t segmentIndex) const;
};

#endif