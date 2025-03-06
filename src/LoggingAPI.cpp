#include "LoggingAPI.hpp"
#include "LogEntry.hpp"
#include "Config.hpp"
#include "Buffer.hpp"
#include "Writer.hpp"
#include <iostream>
#include <stdexcept>

LoggingAPI::LoggingAPI(const Config &config)
    : m_config(std::make_unique<Config>(config)),
      m_buffer(std::make_unique<Buffer>(m_config->getBufferCapacity())),
      m_initialized(false),
      m_shutdownRequested(false)
{
    if (!initialize())
    {
        throw std::runtime_error("Failed to initialize LoggingAPI");
    }
}

LoggingAPI::~LoggingAPI()
{
    shutdown();
}

bool LoggingAPI::initialize()
{
    try
    {
        size_t writerCount = m_config->getWriterCount();
        for (size_t i = 0; i < writerCount; ++i)
        {
            m_writers.emplace_back(std::make_unique<Writer>(m_buffer.get(), m_config));
        }
        m_initialized = true;
        return true;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error initializing LoggingAPI: " << e.what() << std::endl;
        return false;
    }
}

void LoggingAPI::shutdown()
{
    if (!m_shutdownRequested)
    {
        m_shutdownRequested = true;
        m_buffer->signalShutdown();
        m_writers.clear();
    }
}

bool LoggingAPI::append(const std::string &action,
                        const std::string &dataSubject,
                        const std::string &dataCategory,
                        const std::string &dataLocation,
                        const std::string &requestReason,
                        const std::string &additionalInfo)
{
    if (!m_initialized || m_shutdownRequested)
    {
        return false;
    }

    auto entry = std::make_unique<LogEntry>(action, dataSubject, dataCategory, dataLocation, requestReason, additionalInfo);
    return m_buffer->enqueue(std::move(entry));
}

size_t LoggingAPI::exportLogs(std::time_t startTime, std::time_t endTime, const std::string &outputPath)
{
    // Placeholder: Implement log retrieval, decryption, verification, and writing logic
    std::cout << "Exporting logs from " << startTime << " to " << endTime << " to " << outputPath << std::endl;
    return 0;
}

bool LoggingAPI::verifyIntegrity()
{
    // Placeholder: Implement log integrity verification logic
    std::cout << "Verifying log integrity..." << std::endl;
    return true;
}

LoggingStatus LoggingAPI::getStatus() const
{
    // Placeholder: Implement status retrieval logic
    return LoggingStatus{m_buffer->size(), m_writers.size()};
}
