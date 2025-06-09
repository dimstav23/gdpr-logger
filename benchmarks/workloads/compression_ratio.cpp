#include "Compression.hpp"
#include "LogEntry.hpp"

#include <random>
#include <iostream>
#include <chrono>
#include <iomanip>
#include <vector>

// Helper to generate a random alphanumeric string
std::string randomString(size_t minLen, size_t maxLen, std::mt19937 &rng)
{
    static const std::string chars =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::uniform_int_distribution<size_t> lenDist(minLen, maxLen);
    std::uniform_int_distribution<size_t> charDist(0, chars.size() - 1);

    size_t length = lenDist(rng);
    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i)
    {
        result += chars[charDist(rng)];
    }
    return result;
}

// Generate synthetic LogEntries
std::vector<LogEntry> generateSyntheticEntries(size_t count)
{
    std::vector<LogEntry> entries;
    entries.reserve(count);

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> actionDist(0, 3);

    for (size_t i = 0; i < count; ++i)
    {
        auto action = static_cast<LogEntry::ActionType>(actionDist(rng));
        std::string dataLocation = randomString(20, 50, rng);
        std::string dataControllerId = randomString(8, 12, rng);
        std::string dataProcessorId = randomString(8, 12, rng);
        std::string dataSubjectId = randomString(8, 12, rng);
        std::vector<uint8_t> payload;

        LogEntry entry(action, dataLocation, dataControllerId, dataProcessorId, dataSubjectId);
        entries.push_back(entry);
    }

    return entries;
}

struct Result
{
    int compressionLevel;
    size_t uncompressedSize;
    size_t compressedSize;
    double compressionRatio;
    long long durationMs;
};

int main()
{
    size_t batchSize = 5000;
    const std::vector<int> compressionLevels = {0, 1, 3, 6, 9};
    std::vector<Result> results;

    for (auto compressionLevel : compressionLevels)
    {
        std::vector<LogEntry> entries = generateSyntheticEntries(batchSize);
        std::vector<uint8_t> serializedEntries = LogEntry::serializeBatch(std::move(entries));
        size_t uncompressedSize = serializedEntries.size();

        // Compress and time
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<uint8_t> compressed = Compression::compress(std::move(serializedEntries), compressionLevel);
        auto end = std::chrono::high_resolution_clock::now();

        size_t compressedSize = compressed.size();
        double compressionRatio = static_cast<double>(uncompressedSize) / compressedSize;
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        results.push_back({compressionLevel, uncompressedSize, compressedSize, compressionRatio, duration});
    }

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "CompressionLevel  UncompressedB  CompressedB  Ratio    TimeMs\n";
    std::cout << "--------------------------------------------------------\n";
    for (auto &r : results)
    {
        std::cout << std::setw(8) << r.compressionLevel << "    "
                  << std::setw(13) << r.uncompressedSize << "    "
                  << std::setw(11) << r.compressedSize << "    "
                  << std::setw(6) << r.compressionRatio << "    "
                  << std::setw(6) << r.durationMs << "\n";
    }

    return 0;
}
