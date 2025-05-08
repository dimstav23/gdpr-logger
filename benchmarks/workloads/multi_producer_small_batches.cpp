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

BenchmarkResult runBenchmark(const LoggingConfig &baseConfig, int numProducerThreads,
                             int entriesPerProducer, int numSpecificFiles, int producerBatchSize)
{
    LoggingConfig config = baseConfig;
    config.basePath = "./logs/producers_" + std::to_string(numProducerThreads);

    cleanupLogDirectory(config.basePath);

    std::cout << "Generating batches with pre-determined destinations for all threads...";
    std::vector<BatchWithDestination> batches = generateBatches(entriesPerProducer, numSpecificFiles, producerBatchSize);
    std::cout << " Done." << std::endl;
    size_t totalDataSizeBytes = calculateTotalDataSize(batches, numProducerThreads);
    double totalDataSizeGiB = static_cast<double>(totalDataSizeBytes) / (1024 * 1024 * 1024);
    std::cout << "Total data to be written: " << totalDataSizeBytes << " bytes (" << totalDataSizeGiB << " GiB)" << std::endl;

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
    std::cout << "All log entries appended" << std::endl;
    loggingSystem.stop(true);

    size_t finalStorageSize = calculateDirectorySize(config.basePath);
    double writeAmplification = static_cast<double>(finalStorageSize) / totalDataSizeBytes;

    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = numProducerThreads * entriesPerProducer;
    double throughputEntries = totalEntries / elapsedSeconds;
    double throughputGiB = totalDataSizeGiB / elapsedSeconds;

    return BenchmarkResult{
        elapsedSeconds,
        throughputEntries,
        throughputGiB,
        writeAmplification};
}

void runConcurrencyBenchmark(const LoggingConfig &baseConfig,
                             const std::vector<int> &producerThreadCounts, int entriesPerProducer,
                             int numSpecificFiles, int producerBatchSize)
{
    std::vector<BenchmarkResult> results;

    for (int producerCount : producerThreadCounts)
    {
        std::cout << "\nRunning benchmark with " << producerCount << " producer thread(s)..." << std::endl;

        BenchmarkResult result = runBenchmark(baseConfig, producerCount, entriesPerProducer,
                                              numSpecificFiles, producerBatchSize);

        results.push_back(result);
    }

    std::cout << "\n=================== PRODUCER THREAD BENCHMARK SUMMARY ===================" << std::endl;
    std::cout << std::left << std::setw(20) << "Producer Threads"
              << std::setw(25) << "Throughput (entries/s)"
              << std::setw(20) << "Throughput (GiB/s)"
              << std::setw(15) << "Speedup vs. 1"
              << std::setw(20) << "Write Amplification" << std::endl;
    std::cout << "-----------------------------------------------------------------------------------------------------------" << std::endl;

    double baselineThroughputEntries = results[0].throughputEntries;

    for (size_t i = 0; i < producerThreadCounts.size(); i++)
    {
        double speedup = results[i].throughputEntries / baselineThroughputEntries;
        std::cout << std::left << std::setw(20) << producerThreadCounts[i]
                  << std::setw(25) << std::fixed << std::setprecision(2) << results[i].throughputEntries
                  << std::setw(20) << std::fixed << std::setprecision(3) << results[i].throughputGiB
                  << std::setw(15) << std::fixed << std::setprecision(2) << speedup
                  << std::setw(20) << std::fixed << std::setprecision(4) << results[i].writeAmplification << std::endl;
    }
    std::cout << "==========================================================================================================" << std::endl;
}

int main()
{
    // system parameters
    LoggingConfig baseConfig;
    baseConfig.baseFilename = "gdpr_audit";
    baseConfig.maxSegmentSize = 50 * 1024 * 1024; // 50 MB
    baseConfig.maxAttempts = 5;
    baseConfig.baseRetryDelay = std::chrono::milliseconds(1);
    baseConfig.queueCapacity = 3000000;
    baseConfig.batchSize = 8400;
    baseConfig.numWriterThreads = 12;
    baseConfig.appendTimeout = std::chrono::minutes(2);
    baseConfig.useEncryption = true;
    // benchmark parameters
    const int numSpecificFiles = 0;
    const int producerBatchSize = 50;
    const int entriesPerProducer = 3000000;

    std::vector<int> producerThreadCounts = {1, 2, 4, 8, 16, 32};

    runConcurrencyBenchmark(baseConfig,
                            producerThreadCounts,
                            entriesPerProducer,
                            numSpecificFiles,
                            producerBatchSize);

    return 0;
}