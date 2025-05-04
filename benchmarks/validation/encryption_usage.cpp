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
        // Add a small delay after batch operations to simulate real-world patterns
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

BenchmarkResult runBenchmark(const LoggingConfig &baseConfig, bool useEncryption,
                             const std::vector<BatchWithDestination> &batches,
                             int numProducerThreads, int entriesPerProducer)
{
    LoggingConfig config = baseConfig;
    config.basePath = useEncryption ? "./logs_encrypted" : "./logs_unencrypted";
    config.useEncryption = useEncryption;

    cleanupLogDirectory(config.basePath);

    size_t totalDataSizeBytes = calculateTotalDataSize(batches, numProducerThreads);
    double totalDataSizeGiB = static_cast<double>(totalDataSizeBytes) / (1024 * 1024 * 1024);
    std::cout << (useEncryption ? "Encrypted" : "Unencrypted")
              << " benchmark - Total data to be written: " << totalDataSizeBytes
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

    auto endTime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = endTime - startTime;
    loggingSystem.stop(true);

    size_t finalStorageSize = calculateDirectorySize(config.basePath);
    double writeAmplification = static_cast<double>(finalStorageSize) / totalDataSizeBytes;

    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = numProducerThreads * entriesPerProducer;
    double throughputEntries = totalEntries / elapsedSeconds;
    double throughputGiB = totalDataSizeGiB / elapsedSeconds;

    return BenchmarkResult{
        useEncryption,
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
    baseConfig.baseFilename = "gdpr_audit";
    baseConfig.maxSegmentSize = 50 * 1024 * 1024; // 50 MB
    baseConfig.maxAttempts = 5;
    baseConfig.baseRetryDelay = std::chrono::milliseconds(1);
    baseConfig.queueCapacity = 1500000;
    baseConfig.batchSize = 25;
    baseConfig.numWriterThreads = 4;
    baseConfig.appendTimeout = std::chrono::minutes(1);
    // Benchmark parameters
    const int numProducerThreads = 20;
    const int entriesPerProducer = 500000;
    const int numSpecificFiles = 25;
    const int producerBatchSize = 100;

    std::cout << "Generating batches with pre-determined destinations for all threads...";
    std::vector<BatchWithDestination> batches = generateBatches(entriesPerProducer, numSpecificFiles, producerBatchSize);
    std::cout << " Done." << std::endl;

    BenchmarkResult resultEncrypted = runBenchmark(baseConfig, true, batches, numProducerThreads, entriesPerProducer);
    BenchmarkResult resultUnencrypted = runBenchmark(baseConfig, false, batches, numProducerThreads, entriesPerProducer);

    std::cout << "\n============== ENCRYPTION BENCHMARK SUMMARY ==============" << std::endl;
    std::cout << std::left << std::setw(15) << "Encryption"
              << std::setw(20) << "Execution Time (s)"
              << std::setw(25) << "Throughput (entries/s)"
              << std::setw(20) << "Throughput (GiB/s)"
              << std::setw(20) << "Relative Performance"
              << std::setw(20) << "Write Amplification" << std::endl;
    std::cout << "-------------------------------------------------------------------------------------------------------" << std::endl;

    std::cout << std::left << std::setw(15) << "Disabled"
              << std::fixed << std::setprecision(3) << std::setw(20) << resultUnencrypted.executionTime
              << std::fixed << std::setprecision(3) << std::setw(25) << resultUnencrypted.throughputEntries
              << std::fixed << std::setprecision(3) << std::setw(20) << resultUnencrypted.throughputGiB
              << std::fixed << std::setprecision(3) << std::setw(20) << 1.00
              << std::fixed << std::setprecision(3) << std::setw(20) << resultUnencrypted.writeAmplification << std::endl;

    double relativePerf = resultEncrypted.throughputEntries / resultUnencrypted.throughputEntries;
    std::cout << std::left << std::setw(15) << "Enabled"
              << std::fixed << std::setprecision(3) << std::setw(20) << resultEncrypted.executionTime
              << std::fixed << std::setprecision(3) << std::setw(25) << resultEncrypted.throughputEntries
              << std::fixed << std::setprecision(3) << std::setw(20) << resultEncrypted.throughputGiB
              << std::fixed << std::setprecision(3) << std::setw(20) << relativePerf
              << std::fixed << std::setprecision(3) << std::setw(20) << resultEncrypted.writeAmplification << std::endl;

    std::cout << "===========================================================================" << std::endl;

    return 0;
}