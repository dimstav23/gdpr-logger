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

double runFileRotationBenchmark(
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

    std::cout << "Generating batches with pre-determined destinations for all threads..." << std::endl;

    std::vector<std::vector<BatchWithDestination>> allBatches(numProducerThreads);
    for (int i = 0; i < numProducerThreads; i++)
    {
        std::string userId = "user" + std::to_string(i);
        allBatches[i] = generateBatches(entriesPerProducer, userId, numSpecificFiles, producerBatchSize);
    }

    std::cout << "All batches with destinations pre-generated" << std::endl;

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
            std::ref(allBatches[i])));
    }

    for (auto &future : futures)
    {
        future.wait();
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = endTime - startTime;
    std::cout << "All log entries processed" << std::endl;
    loggingSystem.stop(true);

    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = numProducerThreads * entriesPerProducer;
    double throughput = totalEntries / elapsedSeconds;

    return throughput;
}

void runFileRotationComparison(
    const LoggingConfig &baseConfig,
    const std::vector<int> &segmentSizesKB,
    int numProducerThreads,
    int entriesPerProducer,
    int numSpecificFiles,
    int producerBatchSize)
{

    std::vector<double> throughputs;
    std::vector<int> fileCountsPerRun;

    for (int segmentSize : segmentSizesKB)
    {
        double throughput = runFileRotationBenchmark(
            baseConfig,
            segmentSize,
            numProducerThreads,
            entriesPerProducer,
            numSpecificFiles,
            producerBatchSize);

        throughputs.push_back(throughput);

        std::string logDir = "./logs/rotation_" + std::to_string(segmentSize) + "kb";
        int fileCount = countLogFiles(logDir);
        fileCountsPerRun.push_back(fileCount);

        // Add a small delay between runs
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::cout << "\n========================== FILE ROTATION BENCHMARK SUMMARY ==========================" << std::endl;
    std::cout << std::left << std::setw(20) << "Segment Size (KB)"
              << std::setw(25) << "Throughput (entries/s)"
              << std::setw(20) << "Log Files Created"
              << std::setw(20) << "Relative Performance" << std::endl;
    std::cout << "-------------------------------------------------------------------------------------" << std::endl;

    // Use the largest segment size as the baseline for relative performance
    double baselineThroughput = throughputs[0];

    for (size_t i = 0; i < segmentSizesKB.size(); i++)
    {
        double relativePerf = throughputs[i] / baselineThroughput;
        std::cout << std::left << std::setw(20) << segmentSizesKB[i]
                  << std::setw(25) << std::fixed << std::setprecision(2) << throughputs[i]
                  << std::setw(20) << fileCountsPerRun[i]
                  << std::setw(20) << std::fixed << std::setprecision(2) << relativePerf << std::endl;
    }
    std::cout << "=====================================================================================" << std::endl;
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