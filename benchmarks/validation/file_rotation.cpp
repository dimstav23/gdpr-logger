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
    int fileCount;
    double writeAmplification;
    LatencyStats latencyStats;
};

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
    int maxSegmentSizeMB,
    int numProducerThreads,
    int entriesPerProducer,
    int numSpecificFiles,
    int producerBatchSize,
    int payloadSize)
{
    std::string logDir = "./logs/rotation_" + std::to_string(maxSegmentSizeMB) + "mb";

    cleanupLogDirectory(logDir);

    LoggingConfig config = baseConfig;
    config.basePath = logDir;
    config.maxSegmentSize = static_cast<size_t>(maxSegmentSizeMB) * 1024 * 1024;
    std::cout << "Configured max segment size: " << config.maxSegmentSize << " bytes" << std::endl;

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

    size_t finalStorageSize = calculateDirectorySize(logDir);
    double writeAmplification = static_cast<double>(finalStorageSize) / totalDataSizeBytes;

    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = numProducerThreads * entriesPerProducer;
    double throughputEntries = totalEntries / elapsedSeconds;
    double logicalThroughputGiB = totalDataSizeGiB / elapsedSeconds;
    double physicalThroughputGiB = static_cast<double>(finalStorageSize) / (1024.0 * 1024.0 * 1024.0 * elapsedSeconds);
    int fileCount = countLogFiles(logDir);

    // Calculate latency statistics from merged measurements
    LatencyStats latencyStats = calculateLatencyStats(masterCollector);

    cleanupLogDirectory(logDir);

    return BenchmarkResult{
        elapsedSeconds,
        throughputEntries,
        logicalThroughputGiB,
        physicalThroughputGiB,
        fileCount,
        writeAmplification,
        latencyStats};
}

void runFileRotationComparison(
    const LoggingConfig &baseConfig,
    const std::vector<int> &segmentSizesMB,
    int numProducerThreads,
    int entriesPerProducer,
    int numSpecificFiles,
    int producerBatchSize,
    int payloadSize)
{
    std::vector<BenchmarkResult> results;

    for (int segmentSize : segmentSizesMB)
    {
        BenchmarkResult result = runFileRotationBenchmark(
            baseConfig,
            segmentSize,
            numProducerThreads,
            entriesPerProducer,
            numSpecificFiles,
            producerBatchSize,
            payloadSize);

        results.push_back(result);

        // Add a small delay between runs
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    std::cout << "\n========================== FILE ROTATION BENCHMARK SUMMARY ==========================" << std::endl;
    std::cout << std::left << std::setw(20) << "Segment Size (MB)"
              << std::setw(15) << "Time (sec)"
              << std::setw(30) << "Throughput (ent/s)"
              << std::setw(20) << "Logical (GiB/s)"
              << std::setw(20) << "Physical (GiB/s)"
              << std::setw(20) << "Files Created"
              << std::setw(20) << "Write Amp."
              << std::setw(20) << "Rel. Perf"
              << std::setw(12) << "Avg Lat(ms)"
              << std::setw(12) << "Med Lat(ms)"
              << std::setw(12) << "Max Lat(ms)" << std::endl;
    std::cout << "-------------------------------------------------------------------------------------------------------------------------------" << std::endl;

    // Use the first segment size as the baseline for relative performance
    double baselineThroughput = results[0].throughputEntries;

    for (size_t i = 0; i < segmentSizesMB.size(); i++)
    {
        double relativePerf = results[i].throughputEntries / baselineThroughput;
        std::cout << std::left << std::setw(20) << segmentSizesMB[i]
                  << std::setw(15) << std::fixed << std::setprecision(2) << results[i].elapsedSeconds
                  << std::setw(30) << std::fixed << std::setprecision(2) << results[i].throughputEntries
                  << std::setw(20) << std::fixed << std::setprecision(3) << results[i].logicalThroughputGiB
                  << std::setw(20) << std::fixed << std::setprecision(3) << results[i].physicalThroughputGiB
                  << std::setw(20) << results[i].fileCount
                  << std::setw(20) << std::fixed << std::setprecision(4) << results[i].writeAmplification
                  << std::setw(20) << std::fixed << std::setprecision(2) << relativePerf
                  << std::setw(12) << std::fixed << std::setprecision(3) << results[i].latencyStats.avgMs
                  << std::setw(12) << std::fixed << std::setprecision(3) << results[i].latencyStats.medianMs
                  << std::setw(12) << std::fixed << std::setprecision(3) << results[i].latencyStats.maxMs << std::endl;
    }
    std::cout << "================================================================================================================================" << std::endl;
}

int main()
{
    // system parameters
    LoggingConfig baseConfig;
    baseConfig.baseFilename = "default";
    baseConfig.maxAttempts = 5;
    baseConfig.baseRetryDelay = std::chrono::milliseconds(1);
    baseConfig.queueCapacity = 3000000;
    baseConfig.maxExplicitProducers = 32;
    baseConfig.batchSize = 8192;
    baseConfig.numWriterThreads = 64;
    baseConfig.appendTimeout = std::chrono::minutes(2);
    baseConfig.useEncryption = false;
    baseConfig.useCompression = false;
    // benchmark parameters
    const int numSpecificFiles = 0;
    const int producerBatchSize = 256;
    const int numProducers = 32;
    const int entriesPerProducer = 500000;
    const int payloadSize = 2048;

    std::vector<int> segmentSizesMB = {20000, 10000, 5000, 2500, 1000, 500, 100, 50};

    runFileRotationComparison(
        baseConfig,
        segmentSizesMB,
        numProducers,
        entriesPerProducer,
        numSpecificFiles,
        producerBatchSize,
        payloadSize);

    return 0;
}