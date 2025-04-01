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

class SegmentedStorage
{
public:
    /**
     * Constructor for SegmentedStorage.
     *
     * @param basePath Base directory for storing log segments
     * @param baseFilename Base filename for log segments
     * @param maxSegmentSize Maximum size for each segment in bytes before rolling to a new segment
     * @param bufferSize Size of the write buffer in bytes
     */
    SegmentedStorage(const std::string &basePath,
                     const std::string &baseFilename,
                     size_t maxSegmentSize = 100 * 1024 * 1024, // 100 MB default
                     size_t bufferSize = 64 * 1024);            // 64 KB buffer default

    ~SegmentedStorage();

    /**
     * Write data to the current segment.
     * Thread-safe method allowing concurrent writes.
     *
     * @param data Pointer to the data to write
     * @param size Size of the data in bytes
     * @return Actual number of bytes written
     */
    size_t write(const uint8_t *data, size_t size);

    /**
     * Write vector data to the current segment.
     * Thread-safe method allowing concurrent writes.
     *
     * @param data Vector containing the data to write
     * @return Actual number of bytes written
     */
    size_t write(const std::vector<uint8_t> &data);

    // Flush any buffered data to disk.
    void flush();

    /**
     * Get the current segment index.
     *
     * @return Current segment index
     */
    size_t getCurrentSegmentIndex() const;

    /**
     * Get the current size of the active segment.
     *
     * @return Current segment size in bytes
     */
    size_t getCurrentSegmentSize() const;

    /**
     * Get the path to the current segment file.
     *
     * @return Path to the current segment file
     */
    std::string getCurrentSegmentPath() const;

    // Force creation of a new segment file, returns Path to the newly created segment file
    std::string rotateSegment();

private:
    std::string m_basePath;
    std::string m_baseFilename;
    size_t m_maxSegmentSize;
    size_t m_bufferSize;

    // Current segment info
    std::atomic<size_t> m_currentSegmentIndex;
    std::atomic<size_t> m_currentOffset;

    // File handling
    std::unique_ptr<std::ofstream> m_currentFile;
    std::mutex m_fileMutex; // For file operations that can't be atomic

    // Creates a new segment file and updates internal state. Not thread-safe, should be called with lock held.
    void createNewSegment();

    /**
     * Generates the file path for a given segment index.
     *
     * @param segmentIndex The segment index
     * @return Full path to the segment file
     */
    std::string generateSegmentPath(size_t segmentIndex) const;
};

#endif