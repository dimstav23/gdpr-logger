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

double runBatchSizeBenchmark(const LoggingConfig &baseConfig, int writerBatchSize, int numProducerThreads,
                             int entriesPerProducer, int numSpecificFiles, int producerBatchSize)
{
    LoggingConfig config = baseConfig;
    config.basePath = "./logs/batch_" + std::to_string(writerBatchSize);
    config.batchSize = writerBatchSize;

    cleanupLogDirectory(config.basePath);

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
    std::cout << "All log entries appended" << std::endl;
    loggingSystem.stop(true);

    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = numProducerThreads * entriesPerProducer;
    double throughput = totalEntries / elapsedSeconds;

    std::cout << "============== Benchmark Results ==============" << std::endl;
    std::cout << "Writer batch size: " << writerBatchSize << std::endl;
    std::cout << "Number of specific log files: " << numSpecificFiles << std::endl;
    std::cout << "Client batch size: " << producerBatchSize << std::endl;
    std::cout << "Execution time: " << elapsedSeconds << " seconds" << std::endl;
    std::cout << "Total entries to process: " << totalEntries << std::endl;
    std::cout << "Throughput: " << throughput << " entries/second" << std::endl;
    std::cout << "===============================================" << std::endl;

    return throughput;
}

void runBatchSizeComparison(const LoggingConfig &baseConfig, const std::vector<int> &batchSizes,
                            int numProducerThreads, int entriesPerProducer,
                            int numSpecificFiles, int producerBatchSize)
{
    std::vector<double> throughputs;

    for (int batchSize : batchSizes)
    {
        std::cout << "\nRunning benchmark with writer batch size: " << batchSize << "..." << std::endl;

        double throughput = runBatchSizeBenchmark(
            baseConfig, batchSize, numProducerThreads,
            entriesPerProducer, numSpecificFiles, producerBatchSize);

        throughputs.push_back(throughput);

        // small delay between runs
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::cout << "\n=========== WRITER BATCH SIZE BENCHMARK SUMMARY ===========" << std::endl;
    std::cout << std::left << std::setw(15) << "Batch Size"
              << std::setw(20) << "Throughput (entries/s)"
              << std::setw(20) << "Relative Performance" << std::endl;
    std::cout << "--------------------------------------------------------" << std::endl;

    for (size_t i = 0; i < batchSizes.size(); i++)
    {
        double relativePerf = throughputs[i] / throughputs[0];
        std::cout << std::left << std::setw(15) << batchSizes[i]
                  << std::setw(20) << std::fixed << std::setprecision(2) << throughputs[i]
                  << std::setw(20) << std::fixed << std::setprecision(2) << relativePerf << "x" << std::endl;
    }
    std::cout << "=========================================================" << std::endl;
}

int main()
{
    // system parameters
    LoggingConfig baseConfig;
    baseConfig.baseFilename = "gdpr_audit";
    baseConfig.maxSegmentSize = 5 * 1024 * 1024; // 5 MB
    baseConfig.maxAttempts = 5;
    baseConfig.baseRetryDelay = std::chrono::milliseconds(1);
    baseConfig.queueCapacity = 1000000;
    baseConfig.numWriterThreads = 4;
    baseConfig.appendTimeout = std::chrono::milliseconds(300000);
    // benchmark parameters
    const int numSpecificFiles = 20;
    const int producerBatchSize = 50;
    const int numProducers = 20;
    const int entriesPerProducer = 500000;

    std::vector<int> batchSizes = {10, 50, 100, 250, 500, 750, 1000, 2000};

    runBatchSizeComparison(baseConfig,
                           batchSizes,
                           numProducers,
                           entriesPerProducer,
                           numSpecificFiles,
                           producerBatchSize);

    return 0;
}