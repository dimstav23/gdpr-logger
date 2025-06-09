#include "Compression.hpp"
#include "LogEntry.hpp"

#include <random>
#include <iostream>
#include <chrono>
#include <iomanip>
#include <vector>
#include <cstdint>

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

// generate random binary payload (power‑of‑two length 32‑1024 B)
std::vector<uint8_t> randomPayload(std::mt19937 &rng)
{
    std::uniform_int_distribution<int> expDist(5, 10); // 2^5..2^10
    const size_t len = static_cast<size_t>(1) << expDist(rng);

    std::uniform_int_distribution<int> byteDist(0, 255);
    std::vector<uint8_t> data(len);
    for (auto &b : data)
        b = static_cast<uint8_t>(byteDist(rng));
    return data;
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
        std::vector<uint8_t> payload = randomPayload(rng);

        LogEntry entry(action,
                       dataLocation,
                       dataControllerId,
                       dataProcessorId,
                       dataSubjectId,
                       std::move(payload));

        entries.push_back(std::move(entry));
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
    constexpr size_t batchSize = 5000;
    const std::vector<int> compressionLevels = {0, 1, 3, 6, 9};
    std::vector<Result> results;

    for (int level : compressionLevels)
    {
        std::vector<LogEntry> entries = generateSyntheticEntries(batchSize);
        std::vector<uint8_t> serializedEntries = LogEntry::serializeBatch(std::move(entries));
        size_t uncompressedSize = serializedEntries.size();

        // Compress and time
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<uint8_t> compressed = Compression::compress(std::move(serializedEntries), level);
        auto end = std::chrono::high_resolution_clock::now();

        size_t compressedSize = compressed.size();
        double compressionRatio = static_cast<double>(uncompressedSize) / compressedSize;
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        results.push_back({level, uncompressedSize, compressedSize, compressionRatio, duration});
    }

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "CompressionLevel  UncompressedB  CompressedB  Ratio    TimeMs\n";
    std::cout << "--------------------------------------------------------------\n";
    for (const auto &r : results)
    {
        std::cout << std::setw(8) << r.compressionLevel << "    "
                  << std::setw(13) << r.uncompressedSize << "    "
                  << std::setw(11) << r.compressedSize << "    "
                  << std::setw(6) << r.compressionRatio << "    "
                  << std::setw(6) << r.durationMs << '\n';
    }

    return 0;
}