#include "BenchmarkUtils.hpp"

LatencyCollector appendLogEntries(LoggingManager &loggingManager, const std::vector<BatchWithDestination> &batches)
{
    LatencyCollector localCollector;
    // Pre-allocate to avoid reallocations during measurement
    localCollector.reserve(batches.size());

    auto token = loggingManager.createProducerToken();

    for (const auto &batchWithDest : batches)
    {
        // Measure latency for each appendBatch call
        auto startTime = std::chrono::high_resolution_clock::now();

        bool success = loggingManager.appendBatch(batchWithDest.first, token, batchWithDest.second);

        auto endTime = std::chrono::high_resolution_clock::now();
        auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);

        // Record the latency measurement in thread-local collector
        localCollector.addMeasurement(latency);

        if (!success)
        {
            std::cerr << "Failed to append batch of " << batchWithDest.first.size() << " entries to "
                      << (batchWithDest.second ? *batchWithDest.second : "default") << std::endl;
        }
    }

    return localCollector;
}

void cleanupLogDirectory(const std::string &logDir)
{
    try
    {
        if (std::filesystem::exists(logDir))
        {
            std::filesystem::remove_all(logDir);
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error cleaning log directory: " << e.what() << std::endl;
    }
}

size_t calculateTotalDataSize(const std::vector<BatchWithDestination> &batches, int numProducers)
{
    size_t totalSize = 0;

    for (const auto &batchWithDest : batches)
    {
        for (const auto &entry : batchWithDest.first)
        {
            totalSize += entry.serialize().size();
        }
    }

    return totalSize * numProducers;
}

size_t calculateDirectorySize(const std::string &dirPath)
{
    size_t totalSize = 0;
    for (const auto &entry : std::filesystem::recursive_directory_iterator(dirPath))
    {
        if (entry.is_regular_file())
        {
            totalSize += std::filesystem::file_size(entry.path());
        }
    }
    return totalSize;
}

std::vector<BatchWithDestination> generateBatches(
    int numEntries,
    int numSpecificFiles,
    int batchSize,
    int payloadSize)
{
    std::vector<BatchWithDestination> batches;

    // Generate specific filenames based on the parameter
    std::vector<std::string> specificFilenames;
    for (int i = 0; i < numSpecificFiles; i++)
    {
        specificFilenames.push_back("specific_log_file" + std::to_string(i + 1) + ".log");
    }

    int totalChoices = numSpecificFiles + 1; // +1 for default (std::nullopt)
    int generated = 0;
    int destinationIndex = 0;

    while (generated < numEntries)
    {
        int currentBatchSize = std::min(batchSize, numEntries - generated);

        // Deterministically assign a destination (cycling through options)
        std::optional<std::string> targetFilename = std::nullopt;
        if (destinationIndex % totalChoices > 0)
        {
            targetFilename = specificFilenames[(destinationIndex % totalChoices) - 1];
        }

        // Generate the batch
        std::vector<LogEntry> batch;
        batch.reserve(currentBatchSize);
        for (int i = 0; i < currentBatchSize; i++)
        {
            std::string dataLocation = "database/table/row" + std::to_string(generated + i);
            std::string dataSubjectId = "subject" + std::to_string((generated + i) % 10);
            std::string dataControllerId = "controller" + std::to_string(generated + i);
            std::string dataProcessorId = "processor" + std::to_string(generated + i);
            std::vector<uint8_t> payload(payloadSize, 0x22); // payloadSize number of bytes of 0x22
            LogEntry entry(LogEntry::ActionType::CREATE,
                           std::move(dataLocation),
                           std::move(dataControllerId),
                           std::move(dataProcessorId),
                           std::move(dataSubjectId),
                           std::move(payload));
            batch.push_back(entry);
        }

        batches.push_back({batch, targetFilename});
        generated += currentBatchSize;
        destinationIndex++; // Move to the next destination
    }

    return batches;
}

LatencyStats calculateLatencyStats(const LatencyCollector &collector)
{
    const auto &latencies = collector.getMeasurements();

    if (latencies.empty())
    {
        return {0.0, 0.0, 0.0, 0.0, 0, 0.0, 0.0};
    }

    // Convert to milliseconds for easier reading
    std::vector<double> latenciesMs;
    latenciesMs.reserve(latencies.size());
    for (const auto &lat : latencies)
    {
        latenciesMs.push_back(static_cast<double>(lat.count()) / 1e6); // ns to ms
    }

    // Sort for percentile calculations
    std::sort(latenciesMs.begin(), latenciesMs.end());

    LatencyStats stats;
    stats.count = latenciesMs.size();
    stats.minMs = latenciesMs.front();
    stats.maxMs = latenciesMs.back();
    stats.avgMs = std::accumulate(latenciesMs.begin(), latenciesMs.end(), 0.0) / latenciesMs.size();

    // Median
    size_t medianIdx = latenciesMs.size() / 2;
    if (latenciesMs.size() % 2 == 0)
    {
        stats.medianMs = (latenciesMs[medianIdx - 1] + latenciesMs[medianIdx]) / 2.0;
    }
    else
    {
        stats.medianMs = latenciesMs[medianIdx];
    }

    // Percentiles
    size_t p95Idx = static_cast<size_t>(latenciesMs.size() * 0.95);
    size_t p99Idx = static_cast<size_t>(latenciesMs.size() * 0.99);
    stats.p95Ms = latenciesMs[std::min(p95Idx, latenciesMs.size() - 1)];
    stats.p99Ms = latenciesMs[std::min(p99Idx, latenciesMs.size() - 1)];

    return stats;
}

void printLatencyStats(const LatencyStats &stats)
{
    std::cout << "============== Latency Statistics ==============" << std::endl;
    std::cout << "Total append operations: " << stats.count << std::endl;
    std::cout << "Min latency: " << stats.minMs << " ms" << std::endl;
    std::cout << "Max latency: " << stats.maxMs << " ms" << std::endl;
    std::cout << "Average latency: " << stats.avgMs << " ms" << std::endl;
    std::cout << "Median latency: " << stats.medianMs << " ms" << std::endl;
    std::cout << "95th percentile: " << stats.p95Ms << " ms" << std::endl;
    std::cout << "99th percentile: " << stats.p99Ms << " ms" << std::endl;
    std::cout << "===============================================" << std::endl;
}