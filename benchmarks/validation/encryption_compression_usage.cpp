#include "BenchmarkUtils.hpp"
#include "LoggingSystem.hpp"
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
    bool useCompression;
    double executionTime;
    size_t totalEntries;
    double throughputEntries;
    size_t totalDataSizeBytes;
    double throughputGiB;
    double writeAmplification;
};

void appendLogEntries(LoggingSystem &loggingSystem, const std::vector<BatchWithDestination> &batches)
{
    for (const auto &batchWithDest : batches)
    {
        if (!loggingSystem.appendBatch(batchWithDest.first, batchWithDest.second))
        {
            std::cerr << "Failed to append batch of " << batchWithDest.first.size() << " entries to "
                      << (batchWithDest.second ? *batchWithDest.second : "default") << std::endl;
        }
    }
}

BenchmarkResult runBenchmark(const LoggingConfig &baseConfig, bool useEncryption, bool useCompression,
                             const std::vector<BatchWithDestination> &batches,
                             int numProducerThreads, int entriesPerProducer)
{
    LoggingConfig config = baseConfig;
    config.basePath = "./encryption_compression_usage";
    config.useEncryption = useEncryption;
    config.useCompression = useCompression;

    cleanupLogDirectory(config.basePath);

    size_t totalDataSizeBytes = calculateTotalDataSize(batches, numProducerThreads);
    double totalDataSizeGiB = static_cast<double>(totalDataSizeBytes) / (1024 * 1024 * 1024);
    std::cout << "Benchmark with Encryption: " << (useEncryption ? "Enabled" : "Disabled")
              << ", Compression: " << (useCompression ? "Enabled" : "Disabled")
              << " - Total data to be written: " << totalDataSizeBytes
              << " bytes (" << totalDataSizeGiB << " GiB)" << std::endl;

    LoggingSystem loggingSystem(config);
    loggingSystem.start();
    auto startTime = std::chrono::high_resolution_clock::now();

    std::vector<std::future<void>> futures;
    for (int i = 0; i < numProducerThreads; i++)
    {
        futures.push_back(std::async(
            std::launch::async,
            appendLogEntries,
            std::ref(loggingSystem),
            std::ref(batches)));
    }

    for (auto &future : futures)
    {
        future.wait();
    }

    loggingSystem.stop(true);
    auto endTime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = endTime - startTime;

    size_t finalStorageSize = calculateDirectorySize(config.basePath);
    double writeAmplification = static_cast<double>(finalStorageSize) / totalDataSizeBytes;

    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = numProducerThreads * entriesPerProducer;
    double throughputEntries = totalEntries / elapsedSeconds;
    double throughputGiB = totalDataSizeGiB / elapsedSeconds;

    return BenchmarkResult{
        useEncryption,
        useCompression,
        elapsedSeconds,
        totalEntries,
        throughputEntries,
        totalDataSizeBytes,
        throughputGiB,
        writeAmplification};
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
    baseConfig.batchSize = 8400;
    baseConfig.numWriterThreads = 12;
    baseConfig.appendTimeout = std::chrono::minutes(2);
    // Benchmark parameters
    const int numSpecificFiles = 100;
    const int producerBatchSize = 1000;
    const int numProducers = 32;
    const int entriesPerProducer = 3000000;
    const int payloadSize = 2048;

    std::cout << "Generating batches with pre-determined destinations for all threads...";
    std::vector<BatchWithDestination> batches = generateBatches(entriesPerProducer, numSpecificFiles, producerBatchSize, payloadSize);
    std::cout << " Done." << std::endl;

    // Run benchmarks for all four combinations
    BenchmarkResult resultNoEncryptionNoCompression = runBenchmark(baseConfig, false, false, batches, numProducers, entriesPerProducer);
    BenchmarkResult resultNoEncryptionWithCompression = runBenchmark(baseConfig, false, true, batches, numProducers, entriesPerProducer);
    BenchmarkResult resultWithEncryptionNoCompression = runBenchmark(baseConfig, true, false, batches, numProducers, entriesPerProducer);
    BenchmarkResult resultWithEncryptionWithCompression = runBenchmark(baseConfig, true, true, batches, numProducers, entriesPerProducer);

    std::cout << "\n============== BENCHMARK SUMMARY ==============" << std::endl;
    std::cout << std::left << std::setw(12) << "Encryption"
              << std::setw(12) << "Compression"
              << std::setw(20) << "Execution Time (s)"
              << std::setw(25) << "Throughput (entries/s)"
              << std::setw(20) << "Throughput (GiB/s)"
              << std::setw(20) << "Write Amplification" << std::endl;
    std::cout << "-----------------------------------------------------------------------------------------------------------" << std::endl;

    // Display results for each configuration
    auto printResult = [](const BenchmarkResult &result)
    {
        std::cout << std::left << std::setw(12) << (result.useEncryption ? "True" : "False")
                  << std::setw(12) << (result.useCompression ? "True" : "False")
                  << std::fixed << std::setprecision(3) << std::setw(20) << result.executionTime
                  << std::fixed << std::setprecision(3) << std::setw(25) << result.throughputEntries
                  << std::fixed << std::setprecision(3) << std::setw(20) << result.throughputGiB
                  << std::fixed << std::setprecision(3) << std::setw(20) << result.writeAmplification << std::endl;
    };

    printResult(resultNoEncryptionNoCompression);
    printResult(resultNoEncryptionWithCompression);
    printResult(resultWithEncryptionNoCompression);
    printResult(resultWithEncryptionWithCompression);

    std::cout << "=================================================================================" << std::endl;

    return 0;
}