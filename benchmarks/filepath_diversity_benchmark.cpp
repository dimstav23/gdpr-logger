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

// Function to run a benchmark with a specific number of target files
double runFilepathDiversityBenchmark(int numSpecificFiles, int numProducerThreads,
                                     int entriesPerProducer, int producerBatchSize)
{
    // system parameters
    LoggingConfig config;
    config.basePath = "./logs/files_" + std::to_string(numSpecificFiles);
    config.baseFilename = "gdpr_audit";
    config.maxSegmentSize = 5 * 1024 * 1024; // 5 MB
    config.maxAttempts = 5;
    config.baseRetryDelay = std::chrono::milliseconds(1);
    config.queueCapacity = 1000000;
    config.batchSize = 750;
    config.numWriterThreads = 4;
    config.appendTimeout = std::chrono::milliseconds(300000);

    cleanupLogDirectory(config.basePath);

    // Pre-generate all batches with destinations for all threads
    std::cout << "Generating batches with " << numSpecificFiles << " specific files for all threads..." << std::endl;
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
    loggingSystem.stop(true);

    // Calculate and print statistics
    double elapsedSeconds = elapsed.count();
    const size_t totalEntries = numProducerThreads * entriesPerProducer;
    double throughput = totalEntries / elapsedSeconds;

    return throughput;
}

void runFilepathDiversityComparison(const std::vector<int> &numFilesVariants,
                                    int numProducerThreads, int entriesPerProducer, int producerBatchSize)
{
    // Store results for comparison
    std::vector<double> throughputs;
    std::vector<std::string> descriptions;

    // Add descriptive name for each file count
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

        // Run the benchmark and collect throughput
        double throughput = runFilepathDiversityBenchmark(
            fileCount,
            numProducerThreads, entriesPerProducer, producerBatchSize);

        throughputs.push_back(throughput);

        // Add a small delay between runs
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Print comparison table
    std::cout << "\n=========== FILEPATH DIVERSITY BENCHMARK SUMMARY ===========" << std::endl;
    std::cout << std::left << std::setw(30) << "Configuration"
              << std::setw(20) << "Throughput (entries/s)"
              << std::setw(20) << "Relative Performance" << std::endl;
    std::cout << "-----------------------------------------------------------" << std::endl;

    // Calculate base throughput for relative performance
    // Using the "Default file only" (0 specific files) as the baseline
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
    // benchmark parameters
    const int numProducers = 25;
    const int entriesPerProducer = 100000;
    const int producerBatchSize = 100;

    std::vector<int> numFilesVariants = {0, 1, 5, 20, 50, 100, 200, 500, 1000};

    runFilepathDiversityComparison(numFilesVariants,
                                   numProducers,
                                   entriesPerProducer,
                                   producerBatchSize);

    return 0;
}