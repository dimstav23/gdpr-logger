#ifndef BENCHMARK_UTILS_HPP
#define BENCHMARK_UTILS_HPP

#include "LoggingSystem.hpp"
#include <vector>
#include <string>
#include <optional>
#include <filesystem>
#include <iostream>

using BatchWithDestination = std::pair<std::vector<LogEntry>, std::optional<std::string>>;

void cleanupLogDirectory(const std::string &logDir);

size_t calculateTotalDataSize(const std::vector<std::vector<BatchWithDestination>> &allBatches);

size_t calculateDirectorySize(const std::string &dirPath);

std::vector<BatchWithDestination> generateBatches(
    int numEntries,
    const std::string &userId,
    int numSpecificFiles,
    int batchSize);

#endif