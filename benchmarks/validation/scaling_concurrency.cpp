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
    double executionTime;
    double throughputEntries;
    double throughputGiB;
    size_t inputDataSizeBytes;
    size_t outputDataSizeBytes;
    double writeAmplification;
};

BenchmarkResult runBenchmark(const LoggingConfig &baseConfig, int numWriterThreads, int numProducerThreads,
                             int entriesPerProducer, int numSpecificFiles, int producerBatchSize, int payloadSize)
{
    LoggingConfig config = baseConfig;
    config.basePath = "./logs_writers";
    config.numWriterThreads = numWriterThreads;

    cleanupLogDirectory(config.basePath);

    std::cout << "Generating batches with pre-determined destinations for all threads...";
    std::vector<BatchWithDestination> batches = generateBatches(entriesPerProducer, numSpecificFiles, producerBatchSize, payloadSize);
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

    loggingSystem.stop();
    auto endTime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = endTime - startTime;

    size_t finalStorageSize = calculateDirectorySize(config.basePath);
    double writeAmplification = static_cast<double>(finalStorageSize) / totalDataSizeBytes;

    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = numProducerThreads * entriesPerProducer;
    double throughputEntries = totalEntries / elapsedSeconds;
    double throughputGiB = totalDataSizeGiB / elapsedSeconds;

    cleanupLogDirectory(config.basePath);

    return BenchmarkResult{
        elapsedSeconds,
        throughputEntries,
        throughputGiB,
        totalDataSizeBytes,
        finalStorageSize,
        writeAmplification};
}

void runScalabilityBenchmark(const LoggingConfig &baseConfig, const std::vector<int> &writerThreadCounts,
                             int baseProducerThreads, int baseEntriesPerProducer,
                             int numSpecificFiles, int producerBatchSize, int payloadSize)
{
    std::vector<BenchmarkResult> results;

    for (int writerCount : writerThreadCounts)
    {
        std::cout << "\nRunning scalability benchmark with " << writerCount << " writer thread(s)..." << std::endl;

        // Option 1: Scale producer threads, keeping entries per producer constant
        int scaledProducers = baseProducerThreads * writerCount;
        int entriesPerProducer = baseEntriesPerProducer;

        std::cout << "Scaled workload: " << scaledProducers << " producers, "
                  << entriesPerProducer << " entries per producer" << std::endl;

        BenchmarkResult result = runBenchmark(baseConfig, writerCount, scaledProducers, entriesPerProducer,
                                              numSpecificFiles, producerBatchSize, payloadSize);

        results.push_back(result);
    }

    std::cout << "\n=================== SCALABILITY BENCHMARK SUMMARY ===================" << std::endl;
    std::cout << std::left << std::setw(20) << "Writer Threads"
              << std::setw(20) << "Producer Threads"
              << std::setw(15) << "Time (sec)"
              << std::setw(25) << "Throughput (entries/s)"
              << std::setw(20) << "Throughput (GiB/s)"
              << std::setw(25) << "Input Size (bytes)"
              << std::setw(25) << "Storage Size (bytes)"
              << std::setw(20) << "Write Amplification"
              << std::setw(15) << "Relative Perf." << std::endl;
    std::cout << "----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------" << std::endl;

    double baselineThroughput = results[0].throughputEntries;

    for (size_t i = 0; i < writerThreadCounts.size(); i++)
    {
        double relativePerformance = results[i].throughputEntries / (baselineThroughput * writerThreadCounts[i]);

        int scaledProducers = baseProducerThreads * writerThreadCounts[i];

        std::cout << std::left << std::setw(20) << writerThreadCounts[i]
                  << std::setw(20) << scaledProducers
                  << std::setw(15) << std::fixed << std::setprecision(2) << results[i].executionTime
                  << std::setw(25) << std::fixed << std::setprecision(2) << results[i].throughputEntries
                  << std::setw(20) << std::fixed << std::setprecision(3) << results[i].throughputGiB
                  << std::setw(25) << results[i].inputDataSizeBytes
                  << std::setw(25) << results[i].outputDataSizeBytes
                  << std::setw(20) << std::fixed << std::setprecision(4) << results[i].writeAmplification
                  << std::setw(15) << std::fixed << std::setprecision(2) << relativePerformance << std::endl;
    }
    std::cout << "===============================================================================================================================================================================================" << std::endl;
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
    baseConfig.maxExplicitProducers = 16;
    baseConfig.batchSize = 8192;
    baseConfig.appendTimeout = std::chrono::minutes(5);
    baseConfig.useEncryption = true;
    baseConfig.useCompression = true;
    // benchmark parameters
    const int numSpecificFiles = 256;
    const int producerBatchSize = 512;
    const int payloadSize = 4096;

    std::vector<int> writerThreadCounts = {1, 2, 4, 8, 16, 32, 64};

    int baseProducerThreads = 1;
    int baseEntriesPerProducer = 4000000;

    runScalabilityBenchmark(baseConfig,
                            writerThreadCounts,
                            baseProducerThreads,
                            baseEntriesPerProducer,
                            numSpecificFiles,
                            producerBatchSize,
                            payloadSize);

    return 0;
}