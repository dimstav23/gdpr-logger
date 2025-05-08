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

void appendLogEntriesBurst(LoggingSystem &loggingSystem, const std::vector<BatchWithDestination> &batches)
{
    for (const auto &batchWithDest : batches)
    {
        while (!loggingSystem.appendBatch(batchWithDest.first, batchWithDest.second))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
}

int main()
{
    // system parameters
    LoggingConfig config;
    config.basePath = "./logs";
    config.baseFilename = "gdpr_audit";
    config.maxSegmentSize = 50 * 1024 * 1024; // 50 MB
    config.maxAttempts = 5;
    config.baseRetryDelay = std::chrono::milliseconds(1);
    config.queueCapacity = 3000000;
    config.batchSize = 6500;
    config.numWriterThreads = 6;
    config.appendTimeout = std::chrono::minutes(2);
    // benchmark parameters
    const int numBursts = 5;
    const int numSpecificFiles = 100;
    const int producerBatchSize = config.queueCapacity;
    const int entriesPerBurst = 10 * config.queueCapacity;
    const int waitBetweenBurstsSec = 10;

    cleanupLogDirectory(config.basePath);

    std::cout << "Generating burst batches for burst-pattern benchmark...";
    std::vector<BatchWithDestination> batches = generateBatches(entriesPerBurst, numSpecificFiles, producerBatchSize);
    std::cout << " Done." << std::endl;
    size_t totalDataSizeBytes = calculateTotalDataSize(batches, numBursts);
    double totalDataSizeGiB = static_cast<double>(totalDataSizeBytes) / (1024 * 1024 * 1024);

    LoggingSystem loggingSystem(config);
    loggingSystem.start();
    auto startTime = std::chrono::high_resolution_clock::now();

    for (int burst = 0; burst < numBursts; burst++)
    {
        appendLogEntriesBurst(loggingSystem, batches);

        if (burst < numBursts - 1)
        {
            std::this_thread::sleep_for(std::chrono::seconds(waitBetweenBurstsSec));
        }
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = endTime - startTime;
    std::cout << "All bursts finished" << std::endl;
    loggingSystem.stop(true);

    size_t finalStorageSize = calculateDirectorySize(config.basePath);
    double finalStorageSizeGiB = static_cast<double>(finalStorageSize) / (1024 * 1024 * 1024);
    double writeAmplification = static_cast<double>(finalStorageSize) / totalDataSizeBytes;

    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = entriesPerBurst * numBursts;
    double entriesThroughput = totalEntries / elapsedSeconds;
    double dataThroughputGiB = totalDataSizeGiB / elapsedSeconds;
    double averageEntrySize = static_cast<double>(totalDataSizeBytes) / totalEntries;

    std::cout << "\n============== Burst Benchmark Results ==============" << std::endl;
    std::cout << "Execution time: " << elapsedSeconds << " seconds" << std::endl;
    std::cout << "Number of bursts: " << numBursts << std::endl;
    std::cout << "Entries per burst: " << entriesPerBurst << std::endl;
    std::cout << "Total entries appended: " << totalEntries << std::endl;
    std::cout << "Average entry size: " << averageEntrySize << " bytes" << std::endl;
    std::cout << "Total data written: " << totalDataSizeGiB << " GiB" << std::endl;
    std::cout << "Throughput (entries): " << entriesThroughput << " entries/second" << std::endl;
    std::cout << "Throughput (data): " << dataThroughputGiB << " GiB/second" << std::endl;
    std::cout << "Final storage size: " << finalStorageSizeGiB << " GiB" << std::endl;
    std::cout << "Write amplification: " << writeAmplification << " (ratio)" << std::endl;
    std::cout << "======================================================" << std::endl;

    return 0;
}