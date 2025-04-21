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
    /**
     * Constructor
     * @param basePath Base directory for storing log segments
     * @param baseFilename Base filename for default log segments
     * @param maxSegmentSize Maximum size for each segment in bytes before rolling to a new segment
     * @param bufferSize Size of the write buffer in bytes
     */
    SegmentedStorage(const std::string &basePath,
                     const std::string &baseFilename,
                     size_t maxSegmentSize = 100 * 1024 * 1024, // 100 MB default
                     size_t bufferSize = 64 * 1024);            // 64 KB buffer default

    ~SegmentedStorage();

    /**
     * Write data to the default segment, thread-safe, allowing concurrent writes.
     * @param data Vector containing the data to write
     * @return Actual number of bytes written
     */
    size_t write(const std::vector<uint8_t> &data);

    /**
     * Write data to a specified file, thread-safe, allowing concurrent writes.
     * @param filename Client-specified filename (without path or extension)
     * @param data Vector containing the data to write
     * @return Actual number of bytes written
     */
    size_t writeToFile(const std::string &filename, const std::vector<uint8_t> &data);

    // Flush any buffered data to disk for all active segments
    void flush();

    // Flush data for a specific file segment
    void flushFile(const std::string &filename);

    // Returns the current segment index for the default file
    size_t getCurrentSegmentIndex() const;
    // Returns current segment size in bytes for the default file
    size_t getCurrentSegmentSize() const;
    // Returns path to the current default segment file
    std::string getCurrentSegmentPath() const;

    // Force creation of a new segment file for the default file
    // Returns path to the newly created segment file
    std::string rotateSegment();
    // Force creation of a new segment file for a specific file
    std::string rotateSegment(const std::string &filename);

private:
    std::string m_basePath;
    std::string m_baseFilename;
    size_t m_maxSegmentSize;
    size_t m_bufferSize;

    // Default segment info
    std::atomic<size_t> m_currentSegmentIndex{0};
    std::atomic<size_t> m_currentOffset{0};
    std::unique_ptr<std::ofstream> m_currentFile;
    std::mutex m_defaultFileMutex;

    // Structure to track segment information for each custom file
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