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

void runBenchmark(const LoggingConfig &baseConfig, int numWriterThreads, int numProducerThreads,
                  int entriesPerProducer, int numSpecificFiles, int producerBatchSize)
{
    LoggingConfig config = baseConfig;
    config.basePath = "./logs/writers_" + std::to_string(numWriterThreads);
    config.numWriterThreads = numWriterThreads;

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

    std::cout << "All log entries appended" << std::endl;
    loggingSystem.stop(true);

    return;
}

void runConcurrencyBenchmark(const LoggingConfig &baseConfig, const std::vector<int> &writerThreadCounts,
                             int numProducerThreads, int entriesPerProducer,
                             int numSpecificFiles, int producerBatchSize)
{
    std::vector<double> throughputs;
    std::vector<double> times;

    for (int writerCount : writerThreadCounts)
    {
        std::cout << "\nRunning benchmark with " << writerCount << " writer thread(s)..." << std::endl;

        auto startTime = std::chrono::high_resolution_clock::now();

        runBenchmark(baseConfig, writerCount, numProducerThreads, entriesPerProducer,
                     numSpecificFiles, producerBatchSize);

        auto endTime = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed = endTime - startTime;

        double elapsedSeconds = elapsed.count();
        const size_t totalEntries = numProducerThreads * entriesPerProducer;
        double throughput = totalEntries / elapsedSeconds;

        throughputs.push_back(throughput);
        times.push_back(elapsedSeconds);
    }

    std::cout << "\n=================== CONCURRENCY BENCHMARK SUMMARY ===================" << std::endl;
    std::cout << std::left << std::setw(20) << "Writer Threads"
              << std::setw(25) << "Throughput (entries/s)"
              << std::setw(25) << "Time (seconds)"
              << std::setw(10) << "Speedup vs. 1 Thread" << std::endl;
    std::cout << "---------------------------------------------------------------------" << std::endl;

    double baselineThroughput = throughputs[0];

    for (size_t i = 0; i < writerThreadCounts.size(); i++)
    {
        double speedup = throughputs[i] / baselineThroughput;
        std::cout << std::left << std::setw(20) << writerThreadCounts[i]
                  << std::setw(25) << std::fixed << std::setprecision(2) << throughputs[i]
                  << std::setw(25) << std::fixed << std::setprecision(2) << times[i]
                  << std::setw(10) << std::fixed << std::setprecision(2) << speedup << std::endl;
    }
    std::cout << "=====================================================================" << std::endl;
}

int main()
{
    // system parameters
    LoggingConfig baseConfig;
    baseConfig.baseFilename = "gdpr_audit";
    baseConfig.maxSegmentSize = 1 * 1024 * 1024; // 1 MB
    baseConfig.maxAttempts = 5;
    baseConfig.baseRetryDelay = std::chrono::milliseconds(1);
    baseConfig.queueCapacity = 1000000;
    baseConfig.batchSize = 15;
    baseConfig.appendTimeout = std::chrono::milliseconds(30000);
    // benchmark parameters
    const int numSpecificFiles = 20;
    const int producerBatchSize = 50;
    const int numProducers = 20;
    const int entriesPerProducer = 100000;

    std::vector<int> writerThreadCounts = {1, 2, 4, 8, 16};

    runConcurrencyBenchmark(baseConfig,
                            writerThreadCounts,
                            numProducers,
                            entriesPerProducer,
                            numSpecificFiles,
                            producerBatchSize);

    return 0;
}