#include "Compression.hpp"
#include <zlib.h>
#include <stdexcept>
#include <cstring>
#include <iostream>

// Compress a single log entry
std::vector<uint8_t> Compression::compressEntry(const LogEntry &entry)
{
    std::vector<uint8_t> serializedData = entry.serialize();
    return compress(serializedData);
}

// Compress multiple log entries in a batch
std::vector<uint8_t> Compression::compressBatch(const std::vector<LogEntry> &entries)
{
    // Serialize all entries into a single binary blob
    std::vector<uint8_t> batchData;

    // First, store the number of entries
    uint32_t numEntries = entries.size();
    batchData.resize(sizeof(numEntries));
    std::memcpy(batchData.data(), &numEntries, sizeof(numEntries));

    // Then serialize each entry
    for (const auto &entry : entries)
    {
        std::vector<uint8_t> entryData = entry.serialize();

        // Store the size of the serialized entry
        uint32_t entrySize = entryData.size();
        size_t currentSize = batchData.size();
        batchData.resize(currentSize + sizeof(entrySize));
        std::memcpy(batchData.data() + currentSize, &entrySize, sizeof(entrySize));

        // Store the serialized entry
        currentSize = batchData.size();
        batchData.resize(currentSize + entryData.size());
        std::memcpy(batchData.data() + currentSize, entryData.data(), entryData.size());
    }

    return compress(batchData);
}

// Decompress data into a single log entry
std::unique_ptr<LogEntry> Compression::decompressEntry(const std::vector<uint8_t> &compressedData)
{
    try
    {
        std::vector<uint8_t> decompressedData = decompress(compressedData);
        auto entry = std::make_unique<LogEntry>();
        if (entry->deserialize(decompressedData))
        {
            return entry;
        }
        else
        {
            return nullptr;
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error decompressing log entry: " << e.what() << std::endl;
        return nullptr;
    }
}

// Decompress data into multiple log entries
std::vector<LogEntry> Compression::decompressBatch(const std::vector<uint8_t> &compressedData)
{
    std::vector<LogEntry> entries;

    try
    {
        std::vector<uint8_t> decompressedData = decompress(compressedData);

        // Read the number of entries
        if (decompressedData.size() < sizeof(uint32_t))
        {
            throw std::runtime_error("Decompressed data too small to contain entry count");
        }

        uint32_t numEntries;
        std::memcpy(&numEntries, decompressedData.data(), sizeof(numEntries));

        // Position in the decompressed data
        size_t position = sizeof(numEntries);

        // Extract each entry
        for (uint32_t i = 0; i < numEntries; ++i)
        {
            // Check if we have enough data left to read the entry size
            if (position + sizeof(uint32_t) > decompressedData.size())
            {
                throw std::runtime_error("Unexpected end of decompressed data");
            }

            // Read the size of the entry
            uint32_t entrySize;
            std::memcpy(&entrySize, decompressedData.data() + position, sizeof(entrySize));
            position += sizeof(entrySize);

            // Check if we have enough data left to read the entry
            if (position + entrySize > decompressedData.size())
            {
                throw std::runtime_error("Unexpected end of decompressed data");
            }

            // Extract the entry data
            std::vector<uint8_t> entryData(decompressedData.begin() + position,
                                           decompressedData.begin() + position + entrySize);
            position += entrySize;

            // Deserialize the entry
            LogEntry entry;
            if (entry.deserialize(entryData))
            {
                entries.push_back(entry);
            }
            else
            {
                throw std::runtime_error("Failed to deserialize log entry");
            }
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error decompressing log batch: " << e.what() << std::endl;
    }

    return entries;
}

// Helper function to compress raw data using zlib
std::vector<uint8_t> Compression::compress(const std::vector<uint8_t> &data)
{
    if (data.empty())
    {
        return std::vector<uint8_t>();
    }

    z_stream zs;
    std::memset(&zs, 0, sizeof(zs));

    if (deflateInit(&zs, Z_BEST_COMPRESSION) != Z_OK)
    {
        throw std::runtime_error("Failed to initialize zlib deflate");
    }

    zs.next_in = const_cast<Bytef *>(data.data());
    zs.avail_in = data.size();

    int ret;
    char outbuffer[32768];
    std::vector<uint8_t> compressedData;

    // Compress data in chunks
    do
    {
        zs.next_out = reinterpret_cast<Bytef *>(outbuffer);
        zs.avail_out = sizeof(outbuffer);

        ret = deflate(&zs, Z_FINISH);

        if (compressedData.size() < zs.total_out)
        {
            compressedData.insert(compressedData.end(),
                                  outbuffer,
                                  outbuffer + (zs.total_out - compressedData.size()));
        }
    } while (ret == Z_OK);

    deflateEnd(&zs);

    if (ret != Z_STREAM_END)
    {
        throw std::runtime_error("Exception during zlib compression");
    }

    return compressedData;
}

// Helper function to decompress raw data using zlib
std::vector<uint8_t> Compression::decompress(const std::vector<uint8_t> &compressedData)
{
    if (compressedData.empty())
    {
        return std::vector<uint8_t>();
    }

    z_stream zs;
    std::memset(&zs, 0, sizeof(zs));

    if (inflateInit(&zs) != Z_OK)
    {
        throw std::runtime_error("Failed to initialize zlib inflate");
    }

    zs.next_in = const_cast<Bytef *>(compressedData.data());
    zs.avail_in = compressedData.size();

    int ret;
    char outbuffer[32768];
    std::vector<uint8_t> decompressedData;

    // Decompress data in chunks
    do
    {
        zs.next_out = reinterpret_cast<Bytef *>(outbuffer);
        zs.avail_out = sizeof(outbuffer);

        ret = inflate(&zs, Z_NO_FLUSH);

        if (ret == Z_NEED_DICT || ret == Z_DATA_ERROR || ret == Z_MEM_ERROR)
        {
            inflateEnd(&zs);
            throw std::runtime_error("Exception during zlib decompression");
        }

        if (decompressedData.size() < zs.total_out)
        {
            decompressedData.insert(decompressedData.end(),
                                    outbuffer,
                                    outbuffer + (zs.total_out - decompressedData.size()));
        }
    } while (ret == Z_OK);

    inflateEnd(&zs);

    if (ret != Z_STREAM_END)
    {
        throw std::runtime_error("Exception during zlib decompression");
    }

    return decompressedData;
}