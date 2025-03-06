#include "Compression.hpp"
#include <zlib.h>
#include <stdexcept>

std::vector<uint8_t> Compression::compress(const std::vector<uint8_t> &data, int level)
{
    if (data.empty())
    {
        return {};
    }

    uLongf compressedSize = compressBound(data.size());
    std::vector<uint8_t> compressedData(compressedSize);

    int res = ::compress2(compressedData.data(), &compressedSize, data.data(), data.size(), level);
    if (res != Z_OK)
    {
        throw std::runtime_error("Compression failed");
    }

    compressedData.resize(compressedSize);
    return compressedData;
}

std::vector<uint8_t> Compression::decompress(const std::vector<uint8_t> &compressedData)
{
    if (compressedData.empty())
    {
        return {};
    }

    uLongf decompressedSize = compressedData.size() * 4; // Initial guess, may need adjustment
    std::vector<uint8_t> decompressedData(decompressedSize);

    int res = ::uncompress(decompressedData.data(), &decompressedSize, compressedData.data(), compressedData.size());
    if (res == Z_BUF_ERROR)
    {
        decompressedSize *= 2; // Try increasing the buffer size
        decompressedData.resize(decompressedSize);
        res = ::uncompress(decompressedData.data(), &decompressedSize, compressedData.data(), compressedData.size());
    }

    if (res != Z_OK)
    {
        throw std::runtime_error("Decompression failed");
    }

    decompressedData.resize(decompressedSize);
    return decompressedData;
}

bool Compression::isCompressed(const std::vector<uint8_t> &data)
{
    return data.size() > 2 && data[0] == 0x78 && (data[1] == 0x01 || data[1] == 0x9C || data[1] == 0xDA);
}

float Compression::getEstimatedCompressionRatio(const std::string &dataType)
{
    if (dataType == "json" || dataType == "text")
    {
        return 0.4f; // Text compresses well
    }
    else if (dataType == "binary")
    {
        return 0.8f; // Binary data compresses less effectively
    }
    return 1.0f; // Default: assume no compression
}
