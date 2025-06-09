#include "Compression.hpp"
#include "LogEntry.hpp"

#include <random>
#include <iostream>
#include <chrono>
#include <iomanip>
#include <vector>
#include <cstdint>
#include <algorithm>

// Generate synthetic LogEntries with realistic, compressible payloads
std::vector<LogEntry> generateSyntheticEntries(size_t count)
{
    std::vector<LogEntry> entries;
    entries.reserve(count);

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> actionDist(0, 3);

    // Create pools for repeated elements
    std::vector<std::string> userIds;
    for (int i = 1; i <= 1000; ++i)
    {
        userIds.push_back("user_" + std::to_string(i));
    }

    std::vector<std::string> attributes = {
        "profile", "settings", "history", "preferences", "contacts",
        "messages", "photos", "documents", "videos", "audio"};

    std::vector<std::string> controllerIds;
    for (int i = 1; i <= 10; ++i)
    {
        controllerIds.push_back("controller_" + std::to_string(i));
    }

    std::vector<std::string> processorIds;
    for (int i = 1; i <= 20; ++i)
    {
        processorIds.push_back("processor_" + std::to_string(i));
    }

    // Predefined list of common English words for realistic payloads
    std::vector<std::string> wordList = {
        "the", "data", "to", "and", "user", "is", "in", "for", "of", "access",
        "system", "time", "log", "with", "on", "from", "request", "error", "file", "server",
        "update", "status", "by", "at", "process", "information", "new", "this", "connection", "failed",
        "success", "operation", "id", "network", "event", "application", "check", "value", "into", "service",
        "query", "response", "get", "set", "action", "report", "now", "client", "device", "start"};

    // Create weights for Zipfian distribution (1/(rank+1))
    std::vector<double> weights;
    for (size_t k = 0; k < wordList.size(); ++k)
    {
        weights.push_back(1.0 / (k + 1.0));
    }
    std::discrete_distribution<size_t> wordDist(weights.begin(), weights.end());

    // Distributions for selecting from pools
    std::uniform_int_distribution<size_t> userDist(0, userIds.size() - 1);
    std::uniform_int_distribution<size_t> attrDist(0, attributes.size() - 1);
    std::uniform_int_distribution<size_t> controllerDist(0, controllerIds.size() - 1);
    std::uniform_int_distribution<size_t> processorDist(0, processorIds.size() - 1);
    std::uniform_int_distribution<int> wordCountDist(5, 170); // Payload word count

    for (size_t i = 0; i < count; ++i)
    {
        auto action = static_cast<LogEntry::ActionType>(actionDist(rng));

        // Structured dataLocation and related dataSubjectId
        std::string user_id = userIds[userDist(rng)];
        std::string attribute = attributes[attrDist(rng)];
        std::string dataLocation = "user/" + user_id + "/" + attribute;
        std::string dataSubjectId = user_id; // Matches user_id for realism

        // Repeated IDs from limited pools
        std::string dataControllerId = controllerIds[controllerDist(rng)];
        std::string dataProcessorId = processorIds[processorDist(rng)];

        // Generate payload with realistic words selected based on Zipfian distribution
        int wordCount = wordCountDist(rng);
        std::string payloadStr;
        for (int j = 0; j < wordCount; ++j)
        {
            if (j > 0)
                payloadStr += " ";
            size_t wordIndex = wordDist(rng);
            payloadStr += wordList[wordIndex];
        }
        // Ensure payload size is between 32 and 1024 bytes
        if (payloadStr.size() < 32)
        {
            payloadStr += std::string(32 - payloadStr.size(), ' ');
        }
        else if (payloadStr.size() > 1024)
        {
            payloadStr = payloadStr.substr(0, 1024);
        }
        std::vector<uint8_t> payload(payloadStr.begin(), payloadStr.end());

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