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

// Generate synthetic LogEntries and serialize them
std::vector<std::vector<uint8_t>> generateSyntheticEntries(size_t count)
{
    std::vector<std::vector<uint8_t>> serializedEntries;
    serializedEntries.reserve(count);

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> actionDist(0, 3);

    for (size_t i = 0; i < count; ++i)
    {
        auto action = static_cast<LogEntry::ActionType>(actionDist(rng));
        std::string dataLocation = randomString(20, 50, rng);
        std::string userId = randomString(8, 12, rng);
        std::string dataSubjectId = randomString(8, 12, rng);

        LogEntry entry(action, dataLocation, userId, dataSubjectId);
        serializedEntries.push_back(entry.serialize());
    }

    return serializedEntries;
}

struct Result
{
    size_t entryCount;
    size_t uncompressedSize;
    size_t compressedSize;
    double compressionRatio;
    long long durationMs;
};

int main()
{
    std::vector<size_t> batchSizes = {1000, 5000, 10000, 50000};
    std::vector<Result> results;

    for (auto n : batchSizes)
    {
        auto serializedEntries = generateSyntheticEntries(n);

        size_t uncompressedSize = 0;
        for (const auto &e : serializedEntries)
            uncompressedSize += e.size();

        // Compress and time
        auto start = std::chrono::high_resolution_clock::now();
        auto compressed = Compression::compressBatch(serializedEntries);
        auto end = std::chrono::high_resolution_clock::now();

        size_t compressedSize = compressed.size();
        double compressionRatio = static_cast<double>(uncompressedSize) / compressedSize;
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        results.push_back({n, uncompressedSize, compressedSize, compressionRatio, duration});
    }

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "BatchSize  UncompressedB  CompressedB  Ratio    TimeMs\n";
    std::cout << "--------------------------------------------------------\n";
    for (auto &r : results)
    {
        std::cout << std::setw(8) << r.entryCount << "    "
                  << std::setw(13) << r.uncompressedSize << "    "
                  << std::setw(11) << r.compressedSize << "    "
                  << std::setw(6) << r.compressionRatio << "    "
                  << std::setw(6) << r.durationMs << "\n";
    }

    return 0;
}
