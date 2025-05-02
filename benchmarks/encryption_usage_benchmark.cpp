#include "LoggingSystem.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <future>
#include <optional>
#include <iomanip>
#include <filesystem>

using BatchWithDestination = std::pair<std::vector<LogEntry>, std::optional<std::string>>;

struct BenchmarkResult
{
    bool useEncryption;
    double executionTime;
    size_t totalEntries;
    double throughput;
};

void cleanupLogDirectory(const std::string &logDir)
{
    try
    {
        if (std::filesystem::exists(logDir))
        {
            for (const auto &entry : std::filesystem::directory_iterator(logDir))
            {
                std::filesystem::remove_all(entry.path());
            }
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error cleaning log directory: " << e.what() << std::endl;
    }
}

std::vector<BatchWithDestination> generateBatches(int numEntries, const std::string &userId, int numSpecificFiles, int batchSize)
{
    std::vector<BatchWithDestination> batches;

    std::vector<std::string> specificFilenames;
    for (int i = 0; i < numSpecificFiles; i++)
    {
        specificFilenames.push_back("specific_log_file" + std::to_string(i + 1) + ".log");
    }

    int totalChoices = numSpecificFiles + 1; // +1 for default (std::nullopt)
    int generated = 0;
    int destinationIndex = 0;

    while (generated < numEntries)
    {
        int currentBatchSize = std::min(batchSize, numEntries - generated);

        // Deterministically assign a destination (cycling through options)
        std::optional<std::string> targetFilename = std::nullopt;
        if (destinationIndex % totalChoices > 0)
        {
            targetFilename = specificFilenames[(destinationIndex % totalChoices) - 1];
        }

        // Generate the batch
        std::vector<LogEntry> batch;
        batch.reserve(currentBatchSize);
        for (int i = 0; i < currentBatchSize; i++)
        {
            std::string dataLocation = "database/table/row" + std::to_string(generated + i);
            std::string dataSubjectId = "subject" + std::to_string((generated + i) % 10);
            LogEntry entry(LogEntry::ActionType::CREATE, dataLocation, userId, dataSubjectId);
            batch.push_back(entry);
        }

        batches.push_back({batch, targetFilename});
        generated += currentBatchSize;
        destinationIndex++; // Move to the next destination
    }

    return batches;
}

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

// Function to run the benchmark with the given encryption setting
BenchmarkResult runBenchmark(bool useEncryption, const std::vector<std::vector<BatchWithDestination>> &allBatches,
                             int numProducerThreads, int entriesPerProducer)
{
    // Create config with the specified encryption setting
    LoggingConfig config;
    config.basePath = useEncryption ? "./logs_encrypted" : "./logs_unencrypted";
    config.baseFilename = "gdpr_audit";
    config.maxSegmentSize = 50 * 1024 * 1024; // 50 MB
    config.maxAttempts = 5;
    config.baseRetryDelay = std::chrono::milliseconds(1);
    config.queueCapacity = 1000000;
    config.batchSize = 20;
    config.numWriterThreads = 4;
    config.appendTimeout = std::chrono::minutes(1);
    config.useEncryption = useEncryption;

    cleanupLogDirectory(config.basePath);

    LoggingSystem loggingSystem(config);
    loggingSystem.start();
    auto startTime = std::chrono::high_resolution_clock::now();

    // Create multiple producer threads to append pre-generated batches
    std::vector<std::future<void>> futures;
    for (int i = 0; i < numProducerThreads; i++)
    {
        futures.push_back(std::async(
            std::launch::async,
            appendLogEntries,
            std::ref(loggingSystem),
            std::ref(allBatches[i])));
    }

    // Wait for all producer threads to complete
    for (auto &future : futures)
    {
        future.wait();
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = endTime - startTime;
    loggingSystem.stop(true);

    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = numProducerThreads * entriesPerProducer;
    double throughput = totalEntries / elapsedSeconds;
    return BenchmarkResult{useEncryption, elapsedSeconds, totalEntries, throughput};
}

int main()
{
    // Benchmark parameters
    const int numProducerThreads = 20;
    const int entriesPerProducer = 100000;
    const int numSpecificFiles = 25;   // Number of specific files to distribute logs to
    const int producerBatchSize = 100; // Size of batches for batch append operations

    // Pre-generate all batches with destinations for all threads
    std::cout << "Generating batches with pre-determined destinations for all threads..." << std::endl;
    std::vector<std::vector<BatchWithDestination>> allBatches(numProducerThreads);
    for (int i = 0; i < numProducerThreads; i++)
    {
        std::string userId = "user" + std::to_string(i);
        allBatches[i] = generateBatches(entriesPerProducer, userId, numSpecificFiles, producerBatchSize);
    }
    std::cout << "All batches with destinations pre-generated" << std::endl;

    // Run benchmarks and store results
    BenchmarkResult resultEncrypted = runBenchmark(true, allBatches, numProducerThreads, entriesPerProducer);
    BenchmarkResult resultUnencrypted = runBenchmark(false, allBatches, numProducerThreads, entriesPerProducer);

    // Print comparison summary table
    std::cout << "\n============== ENCRYPTION BENCHMARK SUMMARY ==============" << std::endl;
    std::cout << std::left << std::setw(15) << "Encryption"
              << std::setw(20) << "Execution Time (s)"
              << std::setw(25) << "Throughput (entries/s)"
              << std::setw(20) << "Relative Performance" << std::endl;
    std::cout << "--------------------------------------------------------" << std::endl;

    // Unencrypted result (baseline)
    std::cout << std::left << std::setw(15) << "Disabled"
              << std::fixed << std::setprecision(3) << std::setw(20) << resultUnencrypted.executionTime
              << std::fixed << std::setprecision(3) << std::setw(25) << resultUnencrypted.throughput
              << std::fixed << std::setprecision(3) << std::setw(20) << 1.00 << std::endl;

    // Encrypted result with relative performance
    double relativePerf = resultEncrypted.throughput / resultUnencrypted.throughput;
    std::cout << std::left << std::setw(15) << "Enabled"
              << std::fixed << std::setprecision(3) << std::setw(20) << resultEncrypted.executionTime
              << std::fixed << std::setprecision(3) << std::setw(25) << resultEncrypted.throughput
              << std::fixed << std::setprecision(3) << std::setw(20) << relativePerf << std::endl;

    std::cout << "========================================================" << std::endl;

    return 0;
}