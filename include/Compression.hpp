#ifndef COMPRESSION_HPP
#define COMPRESSION_HPP

#include <vector>
#include <string>
#include <memory>
#include "LogEntry.hpp"

class Compression
{
public:
    static std::vector<uint8_t> compressEntry(const LogEntry &entry);

    static std::vector<uint8_t> compressBatch(const std::vector<LogEntry> &entries);

    static std::unique_ptr<LogEntry> decompressEntry(const std::vector<uint8_t> &compressedData);

    static std::vector<LogEntry> decompressBatch(const std::vector<uint8_t> &compressedData);

private:
    static std::vector<uint8_t> compress(const std::vector<uint8_t> &data);

    static std::vector<uint8_t> decompress(const std::vector<uint8_t> &compressedData);
};

#endif