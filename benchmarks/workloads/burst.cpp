#include "BenchmarkUtils.hpp"
#include "LoggingManager.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <future>
#include <optional>
#include <filesystem>
#include <numeric>

int main()
{
    // system parameters
    LoggingConfig config;
    config.basePath = "./logs";
    config.baseFilename = "default";
    config.maxSegmentSize = 50 * 1024 * 1024; // 50 MB
    config.maxAttempts = 5;
    config.baseRetryDelay = std::chrono::milliseconds(1);
    config.queueCapacity = 3000000;
    config.maxExplicitProducers = 1;
    config.batchSize = 8400;
    config.numWriterThreads = 32;
    config.appendTimeout = std::chrono::minutes(2);
    config.useEncryption = true;
    config.useCompression = true;
    // benchmark parameters
    const int numBursts = 5;
    const int numSpecificFiles = 100;
    const int producerBatchSize = config.queueCapacity;
    const int entriesPerBurst = 10 * config.queueCapacity;
    const int waitBetweenBurstsSec = 3;
    const int payloadSize = 2048;

    cleanupLogDirectory(config.basePath);

    std::cout << "Generating burst batches for burst-pattern benchmark...";
    std::vector<BatchWithDestination> batches = generateBatches(entriesPerBurst, numSpecificFiles, producerBatchSize, payloadSize);
    std::cout << " Done." << std::endl;
    size_t totalDataSizeBytes = calculateTotalDataSize(batches, numBursts);
    double totalDataSizeGiB = static_cast<double>(totalDataSizeBytes) / (1024 * 1024 * 1024);

    LoggingManager loggingManager(config);
    loggingManager.start();
    auto startTime = std::chrono::high_resolution_clock::now();

    for (int burst = 0; burst < numBursts; burst++)
    {
        appendLogEntries(loggingManager, batches);

        if (burst < numBursts - 1)
        {
            std::this_thread::sleep_for(std::chrono::seconds(waitBetweenBurstsSec));
        }
    }

    loggingManager.stop();
    auto endTime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = endTime - startTime;

    size_t finalStorageSize = calculateDirectorySize(config.basePath);
    double finalStorageSizeGiB = static_cast<double>(finalStorageSize) / (1024 * 1024 * 1024);
    double writeAmplification = static_cast<double>(finalStorageSize) / totalDataSizeBytes;

    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = entriesPerBurst * numBursts;
    double entriesThroughput = totalEntries / elapsedSeconds;
    double logicalThroughputGiB = totalDataSizeGiB / elapsedSeconds;
    double physicalThroughputGiB = finalStorageSizeGiB / elapsedSeconds;
    double averageEntrySize = static_cast<double>(totalDataSizeBytes) / totalEntries;

    cleanupLogDirectory(config.basePath);

    std::cout << "============== Burst Benchmark Results ==============" << std::endl;
    std::cout << "Execution time: " << elapsedSeconds << " seconds" << std::endl;
    std::cout << "Number of bursts: " << numBursts << std::endl;
    std::cout << "Entries per burst: " << entriesPerBurst << std::endl;
    std::cout << "Total entries appended: " << totalEntries << std::endl;
    std::cout << "Average entry size: " << averageEntrySize << " bytes" << std::endl;
    std::cout << "Total data written: " << totalDataSizeGiB << " GiB" << std::endl;
    std::cout << "Final storage size: " << finalStorageSizeGiB << " GiB" << std::endl;
    std::cout << "Write amplification: " << writeAmplification << " (ratio)" << std::endl;
    std::cout << "Throughput (entries): " << entriesThroughput << " entries/second" << std::endl;
    std::cout << "Throughput (logical): " << logicalThroughputGiB << " GiB/second" << std::endl;
    std::cout << "Throughput (physical): " << physicalThroughputGiB << " GiB/second" << std::endl;
    std::cout << "===============================================" << std::endl;

    return 0;
}