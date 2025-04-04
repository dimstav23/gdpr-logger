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

        if (!loggingSystem.append(action, dataLocation, userId, dataSubjectId))
        {
            std::cerr << "Failed to append log entry " << i << std::endl;
        }
        else
        {
            // std::cout << "Successfully appended log entry " << i << std::endl;
        }

        // Add a small delay occasionally to simulate real-world usage patterns
        if (i % 100 == 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
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
        config.batchSize = 250;
        config.numWriterThreads = 4;

        LoggingSystem loggingSystem(config);

        // Start the logging system
        if (!loggingSystem.start())
        {
            std::cerr << "Failed to start logging system" << std::endl;
            return 1;
        }

        std::cout << "Logging system started" << std::endl;

        // Create multiple producer threads to generate log entries
        std::vector<std::future<void>> futures;

        for (int i = 0; i < 5; i++)
        {
            std::string userId = "user" + std::to_string(i);
            futures.push_back(std::async(
                std::launch::async,
                generateLogEntries,
                std::ref(loggingSystem),
                10000, // Each thread generates 10 entries
                userId));
        }

        // Wait for all producer threads to complete
        for (auto &future : futures)
        {
            future.wait();
        }

        std::cout << "All log entries generated" << std::endl;

        // Allow some time for remaining entries to be processed
        std::cout << "Waiting for queue to drain..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(2));

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

        std::cout << "Logging system stopped" << std::endl;

        return 0;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
}