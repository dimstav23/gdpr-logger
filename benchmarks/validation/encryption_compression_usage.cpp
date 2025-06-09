#include "BenchmarkUtils.hpp"
#include "LoggingManager.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <future>
#include <optional>
#include <iomanip>
#include <filesystem>

struct BenchmarkResult
{
    bool useEncryption;
    int compressionLevel;
    double executionTime;
    size_t totalEntries;
    double throughputEntries;
    size_t totalDataSizeBytes;
    size_t finalStorageSize;
    double logicalThroughputGiB;
    double physicalThroughputGiB;
    double writeAmplification;
    LatencyStats latencyStats;
};

BenchmarkResult runBenchmark(const LoggingConfig &baseConfig, bool useEncryption, int compressionLevel,
                             const std::vector<BatchWithDestination> &batches,
                             int numProducerThreads, int entriesPerProducer)
{
    LoggingConfig config = baseConfig;
    config.basePath = "./encryption_compression_usage";
    config.useEncryption = useEncryption;
    config.compressionLevel = compressionLevel;

    cleanupLogDirectory(config.basePath);

    size_t totalDataSizeBytes = calculateTotalDataSize(batches, numProducerThreads);
    double totalDataSizeGiB = static_cast<double>(totalDataSizeBytes) / (1024 * 1024 * 1024);
    std::cout << "Benchmark with Encryption: " << (useEncryption ? "Enabled" : "Disabled")
              << ", Compression: " << (compressionLevel != 0 ? "Enabled" : "Disabled")
              << " - Total data to be written: " << totalDataSizeBytes
              << " bytes (" << totalDataSizeGiB << " GiB)" << std::endl;

    LoggingManager loggingManager(config);
    loggingManager.start();
    auto startTime = std::chrono::high_resolution_clock::now();

    // Each future now returns a LatencyCollector with thread-local measurements
    std::vector<std::future<LatencyCollector>> futures;
    for (int i = 0; i < numProducerThreads; i++)
    {
        futures.push_back(std::async(
            std::launch::async,
            appendLogEntries,
            std::ref(loggingManager),
            std::ref(batches)));
    }

    // Collect latency measurements from all threads
    LatencyCollector masterCollector;
    for (auto &future : futures)
    {
        LatencyCollector threadCollector = future.get();
        masterCollector.merge(threadCollector);
    }

    loggingManager.stop();
    auto endTime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = endTime - startTime;

    size_t finalStorageSize = calculateDirectorySize(config.basePath);
    double writeAmplification = static_cast<double>(finalStorageSize) / totalDataSizeBytes;

    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = numProducerThreads * entriesPerProducer;
    double throughputEntries = totalEntries / elapsedSeconds;
    double logicalThroughputGiB = totalDataSizeGiB / elapsedSeconds;
    double physicalThroughputGiB = static_cast<double>(finalStorageSize) / (1024.0 * 1024.0 * 1024.0 * elapsedSeconds);

    // Calculate latency statistics from merged measurements
    LatencyStats latencyStats = calculateLatencyStats(masterCollector);

    cleanupLogDirectory(config.basePath);

    return BenchmarkResult{
        useEncryption,
        compressionLevel,
        elapsedSeconds,
        totalEntries,
        throughputEntries,
        totalDataSizeBytes,
        finalStorageSize,
        logicalThroughputGiB,
        physicalThroughputGiB,
        writeAmplification,
        latencyStats};
}

int main()
{
    // system parameters
    LoggingConfig baseConfig;
    baseConfig.baseFilename = "default";
    baseConfig.maxSegmentSize = 50 * 1024 * 1024; // 50 MB
    baseConfig.maxAttempts = 5;
    baseConfig.baseRetryDelay = std::chrono::milliseconds(1);
    baseConfig.queueCapacity = 3000000;
    baseConfig.maxExplicitProducers = 96;
    baseConfig.batchSize = 8192;
    baseConfig.numWriterThreads = 64;
    baseConfig.appendTimeout = std::chrono::minutes(2);
    // Benchmark parameters
    const int numSpecificFiles = 256;
    const int producerBatchSize = 512;
    const int numProducers = 96;
    const int entriesPerProducer = 360000;
    const int payloadSize = 4096;

    const std::vector<int> compressionLevels = {0, 1, 3, 6, 9};
    const std::vector<bool> encryptionSettings = {false, true};

    std::cout << "Generating batches with pre-determined destinations for all threads...";
    std::vector<BatchWithDestination> batches = generateBatches(entriesPerProducer, numSpecificFiles, producerBatchSize, payloadSize);
    std::cout << " Done." << std::endl;

    // Run benchmarks for all combinations of encryption and compression levels
    std::vector<BenchmarkResult> results;

    for (bool useEncryption : encryptionSettings)
    {
        for (int compressionLevel : compressionLevels)
        {
            BenchmarkResult result = runBenchmark(baseConfig, useEncryption, compressionLevel, batches, numProducers, entriesPerProducer);
            results.push_back(result);
        }
    }

    std::cout << "\n============== ENCRYPTION/COMPRESSION LEVEL BENCHMARK SUMMARY ==============" << std::endl;
    std::cout << std::left << std::setw(12) << "Encryption"
              << std::setw(15) << "Comp. Level"
              << std::setw(20) << "Execution Time (s)"
              << std::setw(25) << "Input Size (bytes)"
              << std::setw(25) << "Storage Size (bytes)"
              << std::setw(20) << "Write Amp."
              << std::setw(30) << "Throughput (entries/s)"
              << std::setw(20) << "Logical (GiB/s)"
              << std::setw(20) << "Physical (GiB/s)"
              << std::setw(12) << "Avg Lat(ms)"
              << std::setw(12) << "Med Lat(ms)"
              << std::setw(12) << "Max Lat(ms)" << std::endl;
    std::cout << "-------------------------------------------------------------------------------------------------------------------------------" << std::endl;

    // Helper function to format compression level description
    auto getCompressionDescription = [](int level) -> std::string
    {
        switch (level)
        {
        case 0:
            return "0 (None)";
        case 1:
            return "1 (Speed)";
        case 3:
            return "3 (Low-Med)";
        case 6:
            return "6 (Default)";
        case 9:
            return "9 (Best)";
        default:
            return std::to_string(level);
        }
    };

    // Display results for each configuration
    auto printResult = [&getCompressionDescription](const BenchmarkResult &result)
    {
        std::cout << std::left << std::setw(12) << (result.useEncryption ? "True" : "False")
                  << std::setw(15) << getCompressionDescription(result.compressionLevel)
                  << std::fixed << std::setprecision(3) << std::setw(20) << result.executionTime
                  << std::setw(25) << result.totalDataSizeBytes
                  << std::setw(25) << result.finalStorageSize
                  << std::fixed << std::setprecision(3) << std::setw(20) << result.writeAmplification
                  << std::fixed << std::setprecision(3) << std::setw(30) << result.throughputEntries
                  << std::fixed << std::setprecision(3) << std::setw(20) << result.logicalThroughputGiB
                  << std::fixed << std::setprecision(3) << std::setw(20) << result.physicalThroughputGiB
                  << std::fixed << std::setprecision(3) << std::setw(12) << result.latencyStats.avgMs
                  << std::fixed << std::setprecision(3) << std::setw(12) << result.latencyStats.medianMs
                  << std::fixed << std::setprecision(3) << std::setw(12) << result.latencyStats.maxMs << std::endl;
    };

    for (const auto &result : results)
    {
        printResult(result);
    }

    std::cout << "================================================================================================================================" << std::endl;

    return 0;
}