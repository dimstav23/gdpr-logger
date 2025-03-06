#pragma once

#include "LogEntry.hpp"
#include "Crypto.hpp"
#include <string>
#include <vector>
#include <memory>
#include <fstream>
#include <ctime>

/**
 * @brief Manages a single log segment file
 *
 * A segment is an append-only file that contains log entries.
 * When a segment reaches a specified size or age, it is sealed
 * and a new segment is created.
 */
class Segment
{
public:
    /**
     * @brief Creates or opens a log segment
     *
     * @param path File path for the segment
     * @param crypto Crypto tools for hashing and encryption
     * @param maxSize Maximum size of the segment in bytes
     * @param maxAge Maximum age of the segment in seconds
     * @param compress Whether to compress log entries
     * @param encrypt Whether to encrypt log entries
     * @throws SegmentException if the segment cannot be created/opened
     */
    Segment(const std::string &path,
            std::shared_ptr<Crypto> crypto,
            size_t maxSize,
            std::time_t maxAge,
            bool compress,
            bool encrypt);

    /**
     * @brief Destructor seals the segment if not already sealed
     */
    ~Segment();

    /**
     * @brief Delete copy constructor and assignment operator
     */
    Segment(const Segment &) = delete;
    Segment &operator=(const Segment &) = delete;

    /**
     * @brief Appends a log entry to the segment
     *
     * @param entry Log entry to append
     * @param previousHash Hash of the previous entry for chaining
     * @return Hash of the appended entry
     * @throws SegmentException if the segment is sealed or write fails
     */
    std::vector<uint8_t> append(const LogEntry &entry, const std::vector<uint8_t> &previousHash);

    /**
     * @brief Checks if the segment should be rotated
     *
     * @return true if the segment should be rotated, false otherwise
     */
    bool shouldRotate() const;

    /**
     * @brief Seals the segment, making it read-only
     *
     * Computes a final segment hash and writes it to the segment.
     *
     * @return true if sealing was successful, false otherwise
     */
    bool seal();

    /**
     * @brief Checks if the segment is sealed
     *
     * @return true if sealed, false otherwise
     */
    bool isSealed() const;

    /**
     * @brief Gets the current size of the segment
     *
     * @return Size in bytes
     */
    size_t size() const;

    /**
     * @brief Gets the age of the segment
     *
     * @return Age in seconds
     */
    std::time_t age() const;

    /**
     * @brief Gets the path of the segment
     *
     * @return File path
     */
    std::string getPath() const;

    /**
     * @brief Verifies the integrity of the segment
     *
     * Checks that all hash chains are valid.
     *
     * @return true if integrity verified, false if tampering detected
     */
    bool verifyIntegrity();

    /**
     * @brief Exports the segment to a readable format
     *
     * @param outputPath Path to write the exported data
     * @return Number of entries exported
     * @throws ExportException if export fails
     */
    size_t exportEntries(const std::string &outputPath);

private:
    std::string m_path;
    std::shared_ptr<Crypto> m_crypto;
    size_t m_maxSize;
    std::time_t m_maxAge;
    bool m_compress;
    bool m_encrypt;

    std::ofstream m_file;
    std::time_t m_creationTime;
    size_t m_currentSize;
    bool m_sealed;

    std::vector<uint8_t> m_segmentHash;
    std::vector<uint8_t> m_lastEntryHash;

    /**
     * @brief Opens the segment file
     *
     * @return true if successful, false otherwise
     */
    bool openFile();

    /**
     * @brief Writes the segment header
     *
     * @return true if successful, false otherwise
     */
    bool writeHeader();

    /**
     * @brief Writes the segment footer when sealing
     *
     * @return true if successful, false otherwise
     */
    bool writeFooter();
};