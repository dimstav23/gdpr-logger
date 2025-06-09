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
    double elapsedSeconds;
    double throughputEntries;
    double logicalThroughputGiB;
    double physicalThroughputGiB;
    double writeAmplification;
};

BenchmarkResult runQueueCapacityBenchmark(const LoggingConfig &config, int numProducerThreads,
                                          int entriesPerProducer, int numSpecificFiles, int producerBatchSize, int payloadSize)
{
    cleanupLogDirectory(config.basePath);

    std::cout << "Generating batches with pre-determined destinations for all threads...";
    std::vector<BatchWithDestination> batches = generateBatches(entriesPerProducer, numSpecificFiles, producerBatchSize, payloadSize);
    std::cout << " Done." << std::endl;

    size_t totalDataSizeBytes = calculateTotalDataSize(batches, numProducerThreads);
    double totalDataSizeGiB = static_cast<double>(totalDataSizeBytes) / (1024 * 1024 * 1024);

    std::cout << "Total data to be written: " << totalDataSizeBytes << " bytes ("
              << totalDataSizeGiB << " GiB)" << std::endl;

    LoggingManager loggingManager(config);
    loggingManager.start();
    auto startTime = std::chrono::high_resolution_clock::now();

    std::vector<std::future<void>> futures;
    for (int i = 0; i < numProducerThreads; i++)
    {
        futures.push_back(std::async(
            std::launch::async,
            appendLogEntries,
            std::ref(loggingManager),
            std::ref(batches)));
    }

    for (auto &future : futures)
    {
        future.wait();
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

    cleanupLogDirectory(config.basePath);

    return BenchmarkResult{
        elapsedSeconds,
        throughputEntries,
        logicalThroughputGiB,
        physicalThroughputGiB,
        writeAmplification};
}

void runQueueCapacityComparison(const LoggingConfig &baseConfig, const std::vector<int> &queueSizes,
                                int numProducerThreads,
                                int entriesPerProducer, int numSpecificFiles, int producerBatchSize, int payloadSize)
{
    std::vector<BenchmarkResult> results;

    for (int queueSize : queueSizes)
    {
        LoggingConfig runConfig = baseConfig;
        runConfig.queueCapacity = queueSize;
        runConfig.basePath = "./logs/queue_" + std::to_string(queueSize);

        BenchmarkResult result = runQueueCapacityBenchmark(
            runConfig, numProducerThreads,
            entriesPerProducer, numSpecificFiles, producerBatchSize, payloadSize);

        results.push_back(result);

        // Add a small delay between runs
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    std::cout << "\n=========== QUEUE CAPACITY BENCHMARK SUMMARY ===========" << std::endl;
    std::cout << std::left << std::setw(15) << "Queue Capacity"
              << std::setw(15) << "Time (sec)"
              << std::setw(30) << "Throughput (entries/s)"
              << std::setw(20) << "Logical (GiB/s)"
              << std::setw(20) << "Physical (GiB/s)"
              << std::setw(20) << "Write Amplification"
              << std::setw(20) << "Relative Perf" << std::endl;
    std::cout << "---------------------------------------------------------------------------------------------------------------" << std::endl;

    for (size_t i = 0; i < queueSizes.size(); i++)
    {
        double relativePerf = results[i].throughputEntries / results[0].throughputEntries; // Relative to smallest queue
        std::cout << std::left << std::setw(15) << queueSizes[i]
                  << std::setw(15) << std::fixed << std::setprecision(2) << results[i].elapsedSeconds
                  << std::setw(30) << std::fixed << std::setprecision(2) << results[i].throughputEntries
                  << std::setw(20) << std::fixed << std::setprecision(3) << results[i].logicalThroughputGiB
                  << std::setw(20) << std::fixed << std::setprecision(3) << results[i].physicalThroughputGiB
                  << std::setw(20) << std::fixed << std::setprecision(4) << results[i].writeAmplification
                  << std::setw(20) << std::fixed << std::setprecision(2) << relativePerf << std::endl;
    }
    std::cout << "===============================================================================================================" << std::endl;
}

int main()
{
    // system parameters
    LoggingConfig baseConfig;
    baseConfig.baseFilename = "default";
    baseConfig.maxSegmentSize = 50 * 1024 * 1024; // 50 MB
    baseConfig.maxAttempts = 5;
    baseConfig.baseRetryDelay = std::chrono::milliseconds(1);
    baseConfig.batchSize = 8400;
    baseConfig.maxExplicitProducers = 32;
    baseConfig.numWriterThreads = 12;
    baseConfig.appendTimeout = std::chrono::minutes(2);
    baseConfig.useEncryption = true;
    baseConfig.useCompression = true;
    // benchmark parameters
    const int numSpecificFiles = 100;
    const int producerBatchSize = 1000;
    const int numProducers = 32;
    const int entriesPerProducer = 3000000;
    const int payloadSize = 2048;

    std::vector<int> queueSizes = {10000, 50000, 100000, 200000, 500000, 1000000};
    runQueueCapacityComparison(baseConfig, queueSizes,
                               numProducers,
                               entriesPerProducer,
                               numSpecificFiles,
                               producerBatchSize,
                               payloadSize);

    return 0;
}