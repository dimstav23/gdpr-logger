#include "LoggingSystem.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <future>
#include <optional>
#include <iomanip>
#include <filesystem>

// Type alias for a batch of log entries with a destination
using BatchWithDestination = std::pair<std::vector<LogEntry>, std::optional<std::string>>;

// Function to generate batches of log entries with pre-determined destinations
std::vector<BatchWithDestination> generateBatches(int numEntries, const std::string &userId, int numSpecificFiles, int batchSize)
{
    std::vector<BatchWithDestination> batches;

    // Generate specific filenames based on the parameter
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

// Function to count the number of log files in the log directory
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

// Function to delete all log files in a directory
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
        else
        {
            std::filesystem::create_directories(logDir);
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error cleaning log directory: " << e.what() << std::endl;
    }
}

// Function to run a benchmark with a specific segment size
double runFileRotationBenchmark(
    int maxSegmentSizeKB,
    int numProducerThreads,
    int entriesPerProducer,
    int numSpecificFiles,
    int producerBatchSize)
{
    // Create a unique directory for this test run
    std::string logDir = "./logs/rotation_" + std::to_string(maxSegmentSizeKB) + "kb";

    // Clean up any existing log files from previous runs
    cleanupLogDirectory(logDir);

    // system parameters
    LoggingConfig config;
    config.basePath = logDir;
    config.baseFilename = "gdpr_audit";
    config.maxSegmentSize = maxSegmentSizeKB * 1024; // Convert KB to bytes
    config.maxAttempts = 5;
    config.baseRetryDelay = std::chrono::milliseconds(1);
    config.queueCapacity = 200000; // Use large queue to avoid queueing effects
    config.batchSize = 250;        // number of entries a single writer thread can dequeue at once at most
    config.numWriterThreads = 4;
    config.appendTimeout = std::chrono::milliseconds(30000);

    // Pre-generate all batches with destinations for all threads
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
    std::cout << "Logging system started with max segment size: " << maxSegmentSizeKB << " KB" << std::endl;
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
    std::cout << "All log entries processed" << std::endl;

    // Count the number of log files generated
    int logFileCount = countLogFiles(logDir);

    // Stop the logging system gracefully
    std::cout << "Stopping logging system..." << std::endl;
    loggingSystem.stop(true);

    // Calculate and print statistics
    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = numProducerThreads * entriesPerProducer;
    double throughput = totalEntries / elapsedSeconds;

    std::cout << "============== Benchmark Results ==============" << std::endl;
    std::cout << "Max segment size: " << maxSegmentSizeKB << " KB" << std::endl;
    std::cout << "Number of specific log files: " << numSpecificFiles << std::endl;
    std::cout << "Client batch size: " << producerBatchSize << std::endl;
    std::cout << "Execution time: " << elapsedSeconds << " seconds" << std::endl;
    std::cout << "Total entries processed: " << totalEntries << std::endl;
    std::cout << "Throughput: " << throughput << " entries/second" << std::endl;
    std::cout << "Total log files created: " << logFileCount << std::endl;
    std::cout << "===============================================" << std::endl;

    return throughput;
}

void runFileRotationComparison(
    const std::vector<int> &segmentSizesKB,
    int numProducerThreads,
    int entriesPerProducer,
    int numSpecificFiles,
    int producerBatchSize)
{
    std::cout << "\n============== FILE ROTATION BENCHMARK ==============" << std::endl;
    std::cout << "Testing performance with different maximum segment sizes" << std::endl;
    std::cout << "Producer threads: " << numProducerThreads << std::endl;
    std::cout << "Entries per producer: " << entriesPerProducer << std::endl;
    std::cout << "Specific log files: " << numSpecificFiles << std::endl;
    std::cout << "Producer batch size: " << producerBatchSize << std::endl;
    std::cout << "======================================================" << std::endl;

    // Store results for comparison
    std::vector<double> throughputs;
    std::vector<int> fileCountsPerRun;

    for (int segmentSize : segmentSizesKB)
    {
        std::cout << "\nRunning benchmark with max segment size: " << segmentSize << " KB..." << std::endl;

        // Run the benchmark and collect throughput
        double throughput = runFileRotationBenchmark(
            segmentSize,
            numProducerThreads,
            entriesPerProducer,
            numSpecificFiles,
            producerBatchSize);

        // Store the results
        throughputs.push_back(throughput);

        // Count log files in the directory
        std::string logDir = "./logs/rotation_" + std::to_string(segmentSize) + "kb";
        int fileCount = countLogFiles(logDir);
        fileCountsPerRun.push_back(fileCount);

        // Add a small delay between runs
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Print comparison table
    std::cout << "\n=========== FILE ROTATION BENCHMARK SUMMARY ===========" << std::endl;
    std::cout << std::left << std::setw(20) << "Segment Size (KB)"
              << std::setw(25) << "Throughput (entries/s)"
              << std::setw(20) << "Log Files Created"
              << std::setw(20) << "Relative Performance" << std::endl;
    std::cout << "--------------------------------------------------------" << std::endl;

    // Use the largest segment size as the baseline for relative performance
    double baselineThroughput = throughputs[0];

    for (size_t i = 0; i < segmentSizesKB.size(); i++)
    {
        double relativePerf = throughputs[i] / baselineThroughput;
        std::cout << std::left << std::setw(20) << segmentSizesKB[i]
                  << std::setw(25) << std::fixed << std::setprecision(2) << throughputs[i]
                  << std::setw(20) << fileCountsPerRun[i]
                  << std::setw(20) << std::fixed << std::setprecision(2) << relativePerf << "x" << std::endl;
    }
    std::cout << "========================================================" << std::endl;
}

int main()
{
    // benchmark parameters
    const int numSpecificFiles = 0;       // Number of specific log files
    const int producerBatchSize = 50;     // Size of batches for batch append operations
    const int numProducers = 20;          // Number of producer threads
    const int entriesPerProducer = 50000; // Each producer generates this many entries

    std::vector<int> segmentSizesKB = {5000, 2500, 1000, 500, 100, 50};

    runFileRotationComparison(
        segmentSizesKB,
        numProducers,
        entriesPerProducer,
        numSpecificFiles,
        producerBatchSize);

    return 0;
}