#include "BenchmarkUtils.hpp"
#include "LoggingSystem.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <future>
#include <optional>
#include <filesystem>
#include <numeric>
#include <iomanip>

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

struct BenchmarkConfig
{
    int numProducerThreads;
    int entriesPerProducer;
};

struct BenchmarkResult
{
    BenchmarkConfig config;
    double elapsedSeconds;
    size_t totalEntries;
    double totalDataSizeGiB;
    double entriesThroughput;
    double dataThroughputGiB;
    double writeAmplification;
};

BenchmarkResult runBenchmark(const BenchmarkConfig &benchConfig)
{
    // system parameters
    LoggingConfig config;
    config.basePath = "./logs";
    config.baseFilename = "gdpr_audit";
    config.maxSegmentSize = 50 * 1024 * 1024; // 50 MB
    config.maxAttempts = 5;
    config.baseRetryDelay = std::chrono::milliseconds(1);
    config.queueCapacity = 2000000;
    config.batchSize = 1000;
    config.numWriterThreads = 4;
    config.appendTimeout = std::chrono::minutes(2);
    // benchmark parameters
    const int numSpecificFiles = 0;
    const int producerBatchSize = 1;

    cleanupLogDirectory(config.basePath);

    std::cout << "Generating batches..." << std::flush;
    std::vector<std::vector<BatchWithDestination>> allBatches(benchConfig.numProducerThreads);
    for (int i = 0; i < benchConfig.numProducerThreads; i++)
    {
        std::string userId = "user" + std::to_string(i);
        allBatches[i] = generateBatches(benchConfig.entriesPerProducer, userId, numSpecificFiles, producerBatchSize);
    }
    std::cout << " Done." << std::endl;

    size_t totalDataSizeBytes = calculateTotalDataSize(allBatches);
    double totalDataSizeGiB = static_cast<double>(totalDataSizeBytes) / (1024 * 1024 * 1024);

    LoggingSystem loggingSystem(config);
    loggingSystem.start();
    auto startTime = std::chrono::high_resolution_clock::now();

    std::vector<std::future<void>> futures;
    for (int i = 0; i < benchConfig.numProducerThreads; i++)
    {
        futures.push_back(std::async(
            std::launch::async,
            appendLogEntries,
            std::ref(loggingSystem),
            std::ref(allBatches[i])));
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
    const size_t totalEntries = benchConfig.numProducerThreads * benchConfig.entriesPerProducer;
    double entriesThroughput = totalEntries / elapsedSeconds;
    double dataThroughputGiB = totalDataSizeGiB / elapsedSeconds;

    return BenchmarkResult{
        benchConfig,
        elapsedSeconds,
        totalEntries,
        totalDataSizeGiB,
        entriesThroughput,
        dataThroughputGiB,
        writeAmplification};
}

void printSummary(const std::vector<BenchmarkResult> &results)
{
    std::cout << "\n============================== BENCHMARK SUMMARY ===============================" << std::endl;
    std::cout << std::fixed << std::setprecision(2);
    std::cout << std::setw(20) << "Configuration"
              << std::setw(15) << "Time (s)"
              << std::setw(20) << "Entries/sec"
              << std::setw(15) << "GiB/sec"
              << std::setw(15) << "Write Amp"
              << std::endl;
    std::cout << std::string(85, '-') << std::endl;

    for (const auto &result : results)
    {
        std::string configName = std::to_string(result.config.numProducerThreads) + "p x " +
                                 std::to_string(result.config.entriesPerProducer) + "e";
        std::cout << std::setw(20) << configName
                  << std::setw(15) << result.elapsedSeconds
                  << std::setw(20) << result.entriesThroughput
                  << std::setw(15) << result.dataThroughputGiB
                  << std::setw(15) << result.writeAmplification
                  << std::endl;
    }
    std::cout << "===============================================================================" << std::endl;
}

int main()
{
    // Define the benchmark configurations
    std::vector<BenchmarkConfig> benchConfigs = {
        {40, 5000},   // 40 producers * 5000 entries
        {20, 10000},  // 20 producers * 10000 entries
        {10, 20000}}; // 10 producers * 20000 entries

    std::vector<BenchmarkResult> results;
    for (size_t i = 0; i < benchConfigs.size(); i++)
    {
        results.push_back(runBenchmark(benchConfigs[i]));

        // Add a small delay between runs to ensure clean separation
        if (i < benchConfigs.size() - 1)
        {
            std::cout << "Pausing 5 seconds before next run..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
    }

    printSummary(results);

    return 0;
}