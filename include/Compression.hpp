#ifndef COMPRESSION_HPP
#define COMPRESSION_HPP

#include <vector>
#include <string>
#include <memory>
#include "LogEntry.hpp"

// Provides compression functionality for log entries using zlib before they are encrypted and written to disk
class Compression
{
public:
    // Compress a single log entry
    static std::vector<uint8_t> compressEntry(const LogEntry &entry);

    // Compress multiple log entries in a batch
    static std::vector<uint8_t> compressBatch(const std::vector<LogEntry> &entries);

    // Decompress data into a single log entry or nullptr if decompression fails
    static std::unique_ptr<LogEntry> decompressEntry(const std::vector<uint8_t> &compressedData);

    // Decompress data into multiple log entries
    static std::vector<LogEntry> decompressBatch(const std::vector<uint8_t> &compressedData);

private:
    // helper function to compress raw data
    static std::vector<uint8_t> compress(const std::vector<uint8_t> &data);

    // Helper function to decompress raw data
    static std::vector<uint8_t> decompress(const std::vector<uint8_t> &compressedData);
};

#endif