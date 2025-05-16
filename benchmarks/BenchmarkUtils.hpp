#ifndef BENCHMARK_UTILS_HPP
#define BENCHMARK_UTILS_HPP

#include "LoggingSystem.hpp"
#include <vector>
#include <string>
#include <optional>
#include <filesystem>
#include <iostream>

using BatchWithDestination = std::pair<std::vector<LogEntry>, std::optional<std::string>>;

void appendLogEntries(LoggingSystem &loggingSystem, const std::vector<BatchWithDestination> &batches);

void cleanupLogDirectory(const std::string &logDir);

size_t calculateTotalDataSize(const std::vector<BatchWithDestination> &batches, int numProducers);

size_t calculateDirectorySize(const std::string &dirPath);

std::vector<BatchWithDestination> generateBatches(
    int numEntries,
    int numSpecificFiles,
    int batchSize,
    int payloadSize = 0);

#endif