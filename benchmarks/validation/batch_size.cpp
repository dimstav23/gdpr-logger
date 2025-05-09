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

BenchmarkResult runBatchSizeBenchmark(const LoggingConfig &baseConfig, int writerBatchSize, int numProducerThreads,
                                      int entriesPerProducer, int numSpecificFiles, int producerBatchSize)
{
    LoggingConfig config = baseConfig;
    config.basePath = "./logs/batch_" + std::to_string(writerBatchSize);
    config.batchSize = writerBatchSize;

    cleanupLogDirectory(config.basePath);

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
        throughputEntries,
        throughputGiB,
        writeAmplification};
}

void runBatchSizeComparison(const LoggingConfig &baseConfig, const std::vector<int> &batchSizes,
                            int numProducerThreads, int entriesPerProducer,
                            int numSpecificFiles, int producerBatchSize)
{
    std::vector<BenchmarkResult> results;

    for (int batchSize : batchSizes)
    {
        std::cout << "\nRunning benchmark with writer batch size: " << batchSize << "..." << std::endl;

        BenchmarkResult result = runBatchSizeBenchmark(
            baseConfig, batchSize, numProducerThreads,
            entriesPerProducer, numSpecificFiles, producerBatchSize);

        results.push_back(result);

        // small delay between runs
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::cout << "\n=========== WRITER BATCH SIZE BENCHMARK SUMMARY ===========" << std::endl;
    std::cout << std::left << std::setw(15) << "Batch Size"
              << std::setw(25) << "Throughput (entries/s)"
              << std::setw(20) << "Throughput (GiB/s)"
              << std::setw(20) << "Relative Perf"
              << std::setw(20) << "Write Amplification" << std::endl;
    std::cout << "--------------------------------------------------------------------------------------------------------" << std::endl;

    for (size_t i = 0; i < batchSizes.size(); i++)
    {
        double relativePerf = results[i].throughputEntries / results[0].throughputEntries;
        std::cout << std::left << std::setw(15) << batchSizes[i]
                  << std::setw(25) << std::fixed << std::setprecision(2) << results[i].throughputEntries
                  << std::setw(20) << std::fixed << std::setprecision(3) << results[i].throughputGiB
                  << std::setw(20) << std::fixed << std::setprecision(2) << relativePerf
                  << std::setw(20) << std::fixed << std::setprecision(4) << results[i].writeAmplification << std::endl;
    }
    std::cout << "========================================================================" << std::endl;
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
    baseConfig.numWriterThreads = 12;
    baseConfig.appendTimeout = std::chrono::minutes(2);
    // benchmark parameters
    const int numSpecificFiles = 100;
    const int producerBatchSize = 1000;
    const int numProducers = 32;
    const int entriesPerProducer = 3000000;

    std::vector<int> batchSizes = {10, 50, 100, 500, 1000, 2000, 5000, 10000};

    runBatchSizeComparison(baseConfig,
                           batchSizes,
                           numProducers,
                           entriesPerProducer,
                           numSpecificFiles,
                           producerBatchSize);

    return 0;
}