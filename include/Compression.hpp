#pragma once

#include <vector>
#include <cstdint>
#include <string>

/**
 * @brief Utility class for data compression operations
 */
class Compression
{
public:
    /**
     * @brief Compress data using zlib/deflate algorithm
     *
     * @param data Data to compress
     * @param level Compression level (0-9, where 0 is no compression and 9 is maximum compression)
     * @return std::vector<uint8_t> Compressed data
     * @throws std::runtime_error if compression fails
     */
    static std::vector<uint8_t> compress(
        const std::vector<uint8_t> &data,
        int level = 6);

    /**
     * @brief Decompress data using zlib/deflate algorithm
     *
     * @param compressedData Compressed data
     * @return std::vector<uint8_t> Decompressed data
     * @throws std::runtime_error if decompression fails
     */
    static std::vector<uint8_t> decompress(
        const std::vector<uint8_t> &compressedData);

    /**
     * @brief Check if a vector of data is compressed
     *
     * @param data Data to check
     * @return true if it appears to be zlib compressed, false otherwise
     */
    static bool isCompressed(const std::vector<uint8_t> &data);

    /**
     * @brief Get the estimated compression ratio for a specific data type
     *
     * @param dataType Type of data (e.g., "json", "text", "binary")
     * @return float Estimated compression ratio (1.0 means no compression, 0.5 means half size)
     */
    static float getEstimatedCompressionRatio(const std::string &dataType);
};