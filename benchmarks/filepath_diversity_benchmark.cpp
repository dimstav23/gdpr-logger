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

double runFilepathDiversityBenchmark(const LoggingConfig &config, int numSpecificFiles, int numProducerThreads,
                                     int entriesPerProducer, int producerBatchSize)
{
    LoggingConfig runConfig = config;
    runConfig.basePath = "./logs/files_" + std::to_string(numSpecificFiles);

    cleanupLogDirectory(runConfig.basePath);

    std::cout << "Generating batches with " << numSpecificFiles << " specific files for all threads..." << std::endl;
    std::vector<std::vector<BatchWithDestination>> allBatches(numProducerThreads);
    for (int i = 0; i < numProducerThreads; i++)
    {
        std::string userId = "user" + std::to_string(i);
        allBatches[i] = generateBatches(entriesPerProducer, userId, numSpecificFiles, producerBatchSize);
    }
    std::cout << "All batches with destinations pre-generated" << std::endl;

    LoggingSystem loggingSystem(runConfig);
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

    return throughput;
}

void runFilepathDiversityComparison(const LoggingConfig &baseConfig, const std::vector<int> &numFilesVariants,
                                    int numProducerThreads, int entriesPerProducer, int producerBatchSize)
{
    std::vector<double> throughputs;
    std::vector<std::string> descriptions;

    for (int fileCount : numFilesVariants)
    {
        if (fileCount == 0)
        {
            descriptions.push_back("Default file only");
        }
        else if (fileCount == 1)
        {
            descriptions.push_back("1 specific file");
        }
        else
        {
            descriptions.push_back(std::to_string(fileCount) + " specific files");
        }
    }

    for (size_t i = 0; i < numFilesVariants.size(); i++)
    {
        int fileCount = numFilesVariants[i];
        std::cout << "\nRunning benchmark with " << descriptions[i] << "..." << std::endl;

        double throughput = runFilepathDiversityBenchmark(
            baseConfig,
            fileCount,
            numProducerThreads, entriesPerProducer, producerBatchSize);

        throughputs.push_back(throughput);

        // Add a small delay between runs
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::cout << "\n=========== FILEPATH DIVERSITY BENCHMARK SUMMARY ===========" << std::endl;
    std::cout << std::left << std::setw(30) << "Configuration"
              << std::setw(20) << "Throughput (entries/s)"
              << std::setw(20) << "Relative Performance" << std::endl;
    std::cout << "-----------------------------------------------------------" << std::endl;

    // Calculate base throughput for relative performance
    double baseThroughput = throughputs[0];

    for (size_t i = 0; i < numFilesVariants.size(); i++)
    {
        double relativePerf = throughputs[i] / baseThroughput;
        std::cout << std::left << std::setw(30) << descriptions[i]
                  << std::setw(20) << std::fixed << std::setprecision(2) << throughputs[i]
                  << std::setw(20) << std::fixed << std::setprecision(2) << relativePerf << "x" << std::endl;
    }
    std::cout << "===========================================================" << std::endl;
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
    baseConfig.batchSize = 750;
    baseConfig.numWriterThreads = 4;
    baseConfig.appendTimeout = std::chrono::milliseconds(300000);
    // benchmark parameters
    const int numProducers = 25;
    const int entriesPerProducer = 100000;
    const int producerBatchSize = 100;

    std::vector<int> numFilesVariants = {0, 1, 5, 20, 50, 100, 200, 500, 1000};

    runFilepathDiversityComparison(baseConfig,
                                   numFilesVariants,
                                   numProducers,
                                   entriesPerProducer,
                                   producerBatchSize);

    return 0;
}