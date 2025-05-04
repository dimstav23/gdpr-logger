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
    double throughputEntries;
    double throughputGiB;
    int fileCount;
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

int countLogFiles(const std::string &basePath)
{
    int count = 0;
    for (const auto &entry : std::filesystem::directory_iterator(basePath))
    {
        if (entry.is_regular_file() && entry.path().extension() == ".log")
        {
            count++;
        }
    }
    return count;
}

BenchmarkResult runFileRotationBenchmark(
    const LoggingConfig &baseConfig,
    int maxSegmentSizeKB,
    int numProducerThreads,
    int entriesPerProducer,
    int numSpecificFiles,
    int producerBatchSize)
{
    std::string logDir = "./logs/rotation_" + std::to_string(maxSegmentSizeKB) + "kb";

    cleanupLogDirectory(logDir);

    LoggingConfig config = baseConfig;
    config.basePath = logDir;
    config.maxSegmentSize = maxSegmentSizeKB * 1024; // Convert KB to bytes

    std::cout << "Generating batches with pre-determined destinations for all threads...";
    std::vector<BatchWithDestination> batches = generateBatches(entriesPerProducer, numSpecificFiles, producerBatchSize);
    std::cout << " Done." << std::endl;

    size_t totalDataSizeBytes = calculateTotalDataSize(batches, numProducerThreads);
    double totalDataSizeGiB = static_cast<double>(totalDataSizeBytes) / (1024 * 1024 * 1024);

    std::cout << "Total data to be written: " << totalDataSizeBytes << " bytes ("
              << totalDataSizeGiB << " GiB)" << std::endl;

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
    std::cout << "All log entries processed" << std::endl;
    loggingSystem.stop(true);

    size_t finalStorageSize = calculateDirectorySize(logDir);
    double writeAmplification = static_cast<double>(finalStorageSize) / totalDataSizeBytes;

    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = numProducerThreads * entriesPerProducer;
    double throughputEntries = totalEntries / elapsedSeconds;
    double throughputGiB = totalDataSizeGiB / elapsedSeconds;
    int fileCount = countLogFiles(logDir);

    return BenchmarkResult{
        throughputEntries,
        throughputGiB,
        fileCount,
        writeAmplification};
}

void runFileRotationComparison(
    const LoggingConfig &baseConfig,
    const std::vector<int> &segmentSizesKB,
    int numProducerThreads,
    int entriesPerProducer,
    int numSpecificFiles,
    int producerBatchSize)
{
    std::vector<BenchmarkResult> results;

    for (int segmentSize : segmentSizesKB)
    {
        BenchmarkResult result = runFileRotationBenchmark(
            baseConfig,
            segmentSize,
            numProducerThreads,
            entriesPerProducer,
            numSpecificFiles,
            producerBatchSize);

        results.push_back(result);

        // Add a small delay between runs
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::cout << "\n========================== FILE ROTATION BENCHMARK SUMMARY ==========================" << std::endl;
    std::cout << std::left << std::setw(20) << "Segment Size (KB)"
              << std::setw(25) << "Throughput (entries/s)"
              << std::setw(25) << "Throughput (GiB/s)"
              << std::setw(20) << "Log Files Created"
              << std::setw(20) << "Write Amplification"
              << std::setw(20) << "Relative Performance" << std::endl;
    std::cout << "------------------------------------------------------------------------------------------------" << std::endl;

    // Use the first segment size as the baseline for relative performance
    double baselineThroughput = results[0].throughputEntries;

    for (size_t i = 0; i < segmentSizesKB.size(); i++)
    {
        double relativePerf = results[i].throughputEntries / baselineThroughput;
        std::cout << std::left << std::setw(20) << segmentSizesKB[i]
                  << std::setw(25) << std::fixed << std::setprecision(2) << results[i].throughputEntries
                  << std::setw(25) << std::fixed << std::setprecision(3) << results[i].throughputGiB
                  << std::setw(20) << results[i].fileCount
                  << std::setw(20) << std::fixed << std::setprecision(4) << results[i].writeAmplification
                  << std::setw(20) << std::fixed << std::setprecision(2) << relativePerf << std::endl;
    }
    std::cout << "================================================================================================" << std::endl;
}

int main()
{
    // system parameters
    LoggingConfig baseConfig;
    baseConfig.baseFilename = "gdpr_audit";
    baseConfig.maxAttempts = 5;
    baseConfig.baseRetryDelay = std::chrono::milliseconds(1);
    baseConfig.queueCapacity = 200000; // Use large queue to avoid queueing effects
    baseConfig.batchSize = 250;
    baseConfig.numWriterThreads = 4;
    baseConfig.appendTimeout = std::chrono::milliseconds(30000);
    // benchmark parameters
    const int numSpecificFiles = 0;
    const int producerBatchSize = 50;
    const int numProducers = 20;
    const int entriesPerProducer = 50000;

    std::vector<int> segmentSizesKB = {10000, 5000, 2500, 1000, 500, 100, 50};

    runFileRotationComparison(
        baseConfig,
        segmentSizesKB,
        numProducers,
        entriesPerProducer,
        numSpecificFiles,
        producerBatchSize);

    return 0;
}