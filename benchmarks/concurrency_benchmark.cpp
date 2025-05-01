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

// Function to run a benchmark with a specific number of writer threads
void runBenchmark(int numWriterThreads, int numProducerThreads, int entriesPerProducer,
                  int numSpecificFiles, int producerBatchSize)
{
    // system parameters
    LoggingConfig config;
    config.basePath = "./logs/writers_" + std::to_string(numWriterThreads);
    config.baseFilename = "gdpr_audit";
    config.maxSegmentSize = 1 * 1024 * 1024; // 50 MB
    config.maxAttempts = 5;
    config.baseRetryDelay = std::chrono::milliseconds(1);
    config.queueCapacity = 1000000;
    config.batchSize = 15;                      // number of entries a single writer thread can dequeue at once at most
    config.numWriterThreads = numWriterThreads; // Set the number of writer threads
    config.appendTimeout = std::chrono::milliseconds(30000);

    cleanupLogDirectory(config.basePath);

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
    std::cout << "Logging system started with " << numWriterThreads << " writer thread(s)" << std::endl;
    std::cout << "Using batch size: " << producerBatchSize << std::endl;
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
    std::cout << "All log entries appended" << std::endl;

    // Stop the logging system gracefully
    std::cout << "Stopping logging system..." << std::endl;
    loggingSystem.stop(true);

    // Calculate and print statistics
    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = numProducerThreads * entriesPerProducer;
    double throughput = totalEntries / elapsedSeconds;

    std::cout << "============== Benchmark Results ==============" << std::endl;
    std::cout << "Writer threads: " << numWriterThreads << std::endl;
    std::cout << "Number of specific log files: " << numSpecificFiles << std::endl;
    std::cout << "Client batch size: " << producerBatchSize << std::endl;
    std::cout << "Execution time: " << elapsedSeconds << " seconds" << std::endl;
    std::cout << "Total entries appended: " << totalEntries << std::endl;
    std::cout << "Throughput: " << throughput << " entries/second" << std::endl;
    std::cout << "===============================================" << std::endl;

    return;
}

// Run a concurrency benchmark comparing different numbers of writer threads
void runConcurrencyBenchmark(const std::vector<int> &writerThreadCounts,
                             int numProducerThreads, int entriesPerProducer,
                             int numSpecificFiles, int producerBatchSize)
{
    std::cout << "\n=================== CONCURRENCY BENCHMARK ===================" << std::endl;
    std::cout << "Testing performance with different numbers of writer threads" << std::endl;
    std::cout << "Producer threads: " << numProducerThreads << std::endl;
    std::cout << "Entries per producer: " << entriesPerProducer << std::endl;
    std::cout << "Specific log files: " << numSpecificFiles << std::endl;
    std::cout << "Producer batch size: " << producerBatchSize << std::endl;
    std::cout << "===========================================================" << std::endl;

    // Store results for comparison
    std::vector<double> throughputs;
    std::vector<double> times;

    for (int writerCount : writerThreadCounts)
    {
        std::cout << "\nRunning benchmark with " << writerCount << " writer thread(s)..." << std::endl;

        auto startTime = std::chrono::high_resolution_clock::now();

        runBenchmark(writerCount, numProducerThreads, entriesPerProducer,
                     numSpecificFiles, producerBatchSize);

        auto endTime = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed = endTime - startTime;

        double elapsedSeconds = elapsed.count();
        const size_t totalEntries = numProducerThreads * entriesPerProducer;
        double throughput = totalEntries / elapsedSeconds;

        throughputs.push_back(throughput);
        times.push_back(elapsedSeconds);
    }

    // Print comparison table
    std::cout << "\n=========== CONCURRENCY BENCHMARK SUMMARY ===========" << std::endl;
    std::cout << std::left << std::setw(15) << "Writer Threads"
              << std::setw(20) << "Throughput (entries/s)"
              << std::setw(20) << "Time (seconds)"
              << std::setw(20) << "Speedup vs. 1 Thread" << std::endl;
    std::cout << "---------------------------------------------------" << std::endl;

    double baselineThroughput = throughputs[0];

    for (size_t i = 0; i < writerThreadCounts.size(); i++)
    {
        double speedup = throughputs[i] / baselineThroughput;
        std::cout << std::left << std::setw(15) << writerThreadCounts[i]
                  << std::setw(20) << std::fixed << std::setprecision(2) << throughputs[i]
                  << std::setw(20) << std::fixed << std::setprecision(2) << times[i]
                  << std::setw(20) << std::fixed << std::setprecision(2) << speedup << "x" << std::endl;
    }
    std::cout << "===================================================" << std::endl;
}

int main()
{
    // benchmark parameters
    const int numSpecificFiles = 20;  // Number of specific files to distribute logs to
    const int producerBatchSize = 50; // Size of batches for batch append operations
    const int numProducers = 20;
    const int entriesPerProducer = 100000;
    std::vector<int> writerThreadCounts = {1, 2, 4, 8, 16};

    runConcurrencyBenchmark(writerThreadCounts,
                            numProducers,
                            entriesPerProducer,
                            numSpecificFiles,
                            producerBatchSize);

    return 0;
}