#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <string>
#include <chrono>

struct LoggingConfig
{
    // segmented storage
    std::string basePath = "./logs";
    std::string baseFilename = "gdpr_audit";
    size_t maxSegmentSize = 100 * 1024 * 1024; // 100 MB
    size_t maxAttempts = 10;
    std::chrono::milliseconds baseRetryDelay = std::chrono::milliseconds(1);
    // queue
    size_t queueCapacity = 8192;
    // writers
    size_t batchSize = 100;
    size_t numWriterThreads = 2;
    std::chrono::milliseconds appendTimeout = std::chrono::milliseconds(30000);
    bool useEncryption = true;
};

#endif