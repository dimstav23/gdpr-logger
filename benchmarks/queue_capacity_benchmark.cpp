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

// Function to run a benchmark with a specific queue capacity
double runQueueCapacityBenchmark(int queueCapacity, int numWriterThreads, int numProducerThreads,
                                 int entriesPerProducer, int numSpecificFiles, int producerBatchSize,
                                 bool verbose = true)
{
    // system parameters
    LoggingConfig config;
    config.basePath = "./logs/queue_" + std::to_string(queueCapacity);
    config.baseFilename = "gdpr_audit";
    config.maxSegmentSize = 5 * 1024 * 1024; // 1 MB
    config.maxAttempts = 5;
    config.baseRetryDelay = std::chrono::milliseconds(1);
    config.queueCapacity = queueCapacity;
    config.batchSize = 250;                     // number of entries a single writer thread can dequeue at once at most
    config.numWriterThreads = numWriterThreads; // Set the number of writer threads
    config.appendTimeout = std::chrono::milliseconds(300000);

    cleanupLogDirectory(config.basePath);

    // Pre-generate all batches with destinations for all threads
    if (verbose)
    {
        std::cout << "Generating batches with pre-determined destinations for all threads..." << std::endl;
    }
    std::vector<std::vector<BatchWithDestination>> allBatches(numProducerThreads);
    for (int i = 0; i < numProducerThreads; i++)
    {
        std::string userId = "user" + std::to_string(i);
        allBatches[i] = generateBatches(entriesPerProducer, userId, numSpecificFiles, producerBatchSize);
    }
    if (verbose)
    {
        std::cout << "All batches with destinations pre-generated" << std::endl;
    }

    LoggingSystem loggingSystem(config);
    loggingSystem.start();
    if (verbose)
    {
        std::cout << "Logging system started with queue capacity: " << queueCapacity << std::endl;
        std::cout << "Using " << numWriterThreads << " writer thread(s)" << std::endl;
        std::cout << "Using producer batch size: " << producerBatchSize << std::endl;
    }
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
    if (verbose)
    {
        std::cout << "All log entries processed" << std::endl;
    }

    // Stop the logging system gracefully
    if (verbose)
    {
        std::cout << "Stopping logging system..." << std::endl;
    }
    loggingSystem.stop(true);

    // Calculate and print statistics
    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = numProducerThreads * entriesPerProducer;
    double throughput = totalEntries / elapsedSeconds;

    if (verbose)
    {
        std::cout << "============== Benchmark Results ==============" << std::endl;
        std::cout << "Queue capacity: " << queueCapacity << std::endl;
        std::cout << "Writer threads: " << numWriterThreads << std::endl;
        std::cout << "Number of specific log files: " << numSpecificFiles << std::endl;
        std::cout << "Client batch size: " << producerBatchSize << std::endl;
        std::cout << "Execution time: " << elapsedSeconds << " seconds" << std::endl;
        std::cout << "Total entries to process: " << totalEntries << std::endl;
        std::cout << "Throughput: " << throughput << " entries/second" << std::endl;
        std::cout << "===============================================" << std::endl;
    }

    return throughput;
}

void runQueueCapacityComparison(const std::vector<int> &queueSizes,
                                int numWriterThreads, int numProducerThreads,
                                int entriesPerProducer, int numSpecificFiles, int producerBatchSize)
{
    std::cout << "\n============== QUEUE CAPACITY BENCHMARK ==============" << std::endl;
    std::cout << "Testing performance with different queue capacities" << std::endl;
    std::cout << "Writer threads: " << numWriterThreads << std::endl;
    std::cout << "Producer threads: " << numProducerThreads << std::endl;
    std::cout << "Entries per producer: " << entriesPerProducer << std::endl;
    std::cout << "Specific log files: " << numSpecificFiles << std::endl;
    std::cout << "Producer batch size: " << producerBatchSize << std::endl;
    std::cout << "======================================================" << std::endl;

    // Store results for comparison
    std::vector<double> throughputs;
    std::vector<int> queueFullCounts;
    std::vector<int> maxQueueSizes;

    for (int queueSize : queueSizes)
    {
        std::cout << "\nRunning benchmark with queue capacity: " << queueSize << "..." << std::endl;

        // Run the benchmark and collect throughput
        double throughput = runQueueCapacityBenchmark(
            queueSize, numWriterThreads, numProducerThreads,
            entriesPerProducer, numSpecificFiles, producerBatchSize);

        throughputs.push_back(throughput);

        // Add a small delay between runs
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Print comparison table
    std::cout << "\n=========== QUEUE CAPACITY BENCHMARK SUMMARY ===========" << std::endl;
    std::cout << std::left << std::setw(15) << "Queue Capacity"
              << std::setw(20) << "Throughput (entries/s)"
              << std::setw(20) << "Relative Performance" << std::endl;
    std::cout << "------------------------------------------------------" << std::endl;

    for (size_t i = 0; i < queueSizes.size(); i++)
    {
        double relativePerf = throughputs[i] / throughputs[0]; // Relative to smallest queue
        std::cout << std::left << std::setw(15) << queueSizes[i]
                  << std::setw(20) << std::fixed << std::setprecision(2) << throughputs[i]
                  << std::setw(20) << std::fixed << std::setprecision(2) << relativePerf << "x" << std::endl;
    }
    std::cout << "=======================================================" << std::endl;
}

int main()
{
    // benchmark parameters
    const int numWriterThreads = 4;
    const int numSpecificFiles = 20;
    const int producerBatchSize = 50;
    const int numProducers = 20;
    const int entriesPerProducer = 50000;

    std::vector<int> queueSizes = {2000, 10000, 50000, 100000, 200000, 500000, 1000000};
    runQueueCapacityComparison(queueSizes,
                               numWriterThreads,
                               numProducers,
                               entriesPerProducer,
                               numSpecificFiles,
                               producerBatchSize);

    return 0;
}