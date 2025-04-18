#include "LoggingSystem.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <future>

// Function to generate sample log entries
void generateLogEntries(LoggingSystem &loggingSystem, int numEntries, const std::string &userId)
{
    std::cout << "Generating " << numEntries << " log entries for user " << userId << std::endl;

    std::vector<LogEntry> batch;
    const int MAX_BATCH_SIZE = 20;

    for (int i = 0; i < numEntries; i++)
    {
        std::string dataLocation = "database/table/row" + std::to_string(i);
        std::string dataSubjectId = "subject" + std::to_string(i % 10);

        // Randomly select an action type
        LogEntry::ActionType action;
        switch (i % 4)
        {
        case 0:
            action = LogEntry::ActionType::CREATE;
            break;
        case 1:
            action = LogEntry::ActionType::READ;
            break;
        case 2:
            action = LogEntry::ActionType::UPDATE;
            break;
        default:
            action = LogEntry::ActionType::DELETE;
            break;
        }

        LogEntry entry(action, dataLocation, userId, dataSubjectId);

        // Decide whether to use batch append or single append
        // Use batch about 30% of the time, but build the batch gradually
        if (i % 10 < 3) // 30% probability
        {
            batch.push_back(entry);

            // When batch reaches MAX_BATCH_SIZE or at random intervals, append the batch
            if (batch.size() >= MAX_BATCH_SIZE || (batch.size() > 0 && rand() % 10 == 0))
            {
                if (!loggingSystem.appendBatch(batch))
                {
                    std::cerr << "Failed to append batch of " << batch.size() << " entries" << std::endl;
                }
                batch.clear();

                // Add a small delay after batch operations to simulate real-world patterns
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
        }
        else
        {
            // Use regular append
            if (!loggingSystem.append(entry))
            {
                std::cerr << "Failed to append single log entry " << i << std::endl;
            }

            // Add a small delay occasionally to simulate real-world usage patterns
            if (i % 100 == 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    }

    // Make sure to append any remaining entries in the batch
    if (!batch.empty())
    {
        if (!loggingSystem.appendBatch(batch))
        {
            std::cerr << "Failed to append final batch of " << batch.size() << " entries" << std::endl;
        }
    }
}

int main()
{
    try
    {
        LoggingConfig config;
        config.basePath = "./logs";
        config.baseFilename = "gdpr_audit";
        config.maxSegmentSize = 50 * 1024 * 1024; // 50 MB
        config.bufferSize = 128 * 1024;           // 128 KB
        config.queueCapacity = 16384;
        config.batchSize = 750; // number of entries a single writer thread can dequeue at once at most
        config.numWriterThreads = 4;
        config.appendTimeout = std::chrono::milliseconds(30000);

        LoggingSystem loggingSystem(config);

        if (!loggingSystem.start())
        {
            std::cerr << "Failed to start logging system" << std::endl;
            return 1;
        }

        std::cout << "Logging system started" << std::endl;
        auto startTime = std::chrono::high_resolution_clock::now();

        // Create multiple producer threads to generate log entries
        std::vector<std::future<void>> futures;

        for (int i = 0; i < 25; i++)
        {
            std::string userId = "user" + std::to_string(i);
            futures.push_back(std::async(
                std::launch::async,
                generateLogEntries,
                std::ref(loggingSystem),
                40000,
                userId));
        }

        // Wait for all producer threads to complete
        for (auto &future : futures)
        {
            future.wait();
        }

        std::cout << "All log entries generated" << std::endl;

        // Export logs
        // std::cout << "Exporting logs..." << std::endl;
        /* if (loggingSystem.exportLogs("./export/audit_export.log"))
         {
             std::cout << "Logs exported successfully" << std::endl;
         }
         else
         {
             std::cerr << "Failed to export logs" << std::endl;
         } */

        // Stop the logging system gracefully
        std::cout << "Stopping logging system..." << std::endl;
        if (!loggingSystem.stop(true))
        {
            std::cerr << "Failed to stop logging system" << std::endl;
            return 1;
        }

        auto endTime = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed = endTime - startTime;
        std::cout << "Execution time: " << elapsed.count() << " seconds" << std::endl;

        return 0;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
}