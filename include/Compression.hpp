#ifndef COMPRESSION_HPP
#define COMPRESSION_HPP

#include "LogEntry.hpp"
#include <vector>
#include <cstdint>

class Compression
{
public:
    static std::vector<uint8_t> compressBatch(const std::vector<std::vector<uint8_t>> &serializedEntries);

    static std::vector<LogEntry> decompressBatch(const std::vector<uint8_t> &compressedData);

private:
    static std::vector<uint8_t> compress(const std::vector<uint8_t> &data);

    static std::vector<uint8_t> decompress(const std::vector<uint8_t> &compressedData);
};

#endif