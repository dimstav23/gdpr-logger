#include "BenchmarkUtils.hpp"

void appendLogEntries(LoggingSystem &loggingSystem, const std::vector<BatchWithDestination> &batches)
{
    auto token = loggingSystem.createProducerToken();

    for (const auto &batchWithDest : batches)
    {
        if (!loggingSystem.appendBatch(batchWithDest.first, token, batchWithDest.second))
        {
            std::cerr << "Failed to append batch of " << batchWithDest.first.size() << " entries to "
                      << (batchWithDest.second ? *batchWithDest.second : "default") << std::endl;
        }
    }
}

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

size_t calculateTotalDataSize(const std::vector<BatchWithDestination> &batches, int numProducers)
{
    size_t totalSize = 0;

    for (const auto &batchWithDest : batches)
    {
        for (const auto &entry : batchWithDest.first)
        {
            totalSize += entry.serialize().size();
        }
    }

    return totalSize * numProducers;
}

size_t calculateDirectorySize(const std::string &dirPath)
{
    size_t totalSize = 0;
    for (const auto &entry : std::filesystem::recursive_directory_iterator(dirPath))
    {
        if (entry.is_regular_file())
        {
            totalSize += std::filesystem::file_size(entry.path());
        }
    }
    return totalSize;
}

std::vector<BatchWithDestination> generateBatches(
    int numEntries,
    int numSpecificFiles,
    int batchSize,
    int payloadSize)
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
            std::string userId = "user" + std::to_string(generated + i);
            std::vector<uint8_t> payload(payloadSize, 0x22); // payloadSize number of bytes of 0x22
            LogEntry entry(LogEntry::ActionType::CREATE,
                           std::move(dataLocation),
                           std::move(userId),
                           std::move(dataSubjectId),
                           std::move(payload));
            batch.push_back(entry);
        }

        batches.push_back({batch, targetFilename});
        generated += currentBatchSize;
        destinationIndex++; // Move to the next destination
    }

    return batches;
}