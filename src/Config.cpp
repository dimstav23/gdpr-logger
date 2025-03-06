#include "Config.hpp"
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <iostream>

Config::Config()
{
    setDefaults();
}

Config::Config(const std::string &configFilePath)
{
    setDefaults();
    if (!loadFromFile(configFilePath))
    {
        throw std::runtime_error("Failed to load config file: " + configFilePath);
    }
}

bool Config::loadFromFile(const std::string &configFilePath)
{
    std::ifstream file(configFilePath);
    if (!file)
    {
        return false;
    }

    std::string line;
    while (std::getline(file, line))
    {
        std::istringstream iss(line);
        std::string key, value;
        if (std::getline(iss, key, '=') && std::getline(iss, value))
        {
            setSettingString(key, value);
        }
    }
    return validate();
}

bool Config::saveToFile(const std::string &configFilePath) const
{
    std::ofstream file(configFilePath);
    if (!file)
    {
        return false;
    }
    for (const auto &pair : m_stringSettings)
    {
        file << pair.first << "=" << pair.second << "\n";
    }
    return true;
}

bool Config::validate() const
{
    return !m_segmentDirectory.empty() && m_writerCount > 0 && m_bufferCapacity > 0;
}

void Config::setDefaults()
{
    m_bufferCapacity = 1000;
    m_writerCount = 4;
    m_batchSize = 50;
    m_writeIntervalMs = 100;
    m_segmentDirectory = "./logs";
    m_maxSegmentSize = 10485760;
    m_maxSegmentAgeSec = 86400;
    m_encryptionEnabled = false;
    m_encryptionKeyPath = "";
    m_hashAlgorithm = "SHA-256";
    m_compressionEnabled = false;
    m_defaultExportDirectory = "./exports";
}

size_t Config::getBufferCapacity() const { return m_bufferCapacity; }
void Config::setBufferCapacity(size_t capacity) { m_bufferCapacity = capacity; }

size_t Config::getWriterCount() const { return m_writerCount; }
void Config::setWriterCount(size_t count) { m_writerCount = count; }

size_t Config::getBatchSize() const { return m_batchSize; }
void Config::setBatchSize(size_t size) { m_batchSize = size; }

unsigned Config::getWriteIntervalMs() const { return m_writeIntervalMs; }
void Config::setWriteIntervalMs(unsigned intervalMs) { m_writeIntervalMs = intervalMs; }

std::string Config::getSegmentDirectory() const { return m_segmentDirectory; }
void Config::setSegmentDirectory(const std::string &directory) { m_segmentDirectory = directory; }

size_t Config::getMaxSegmentSize() const { return m_maxSegmentSize; }
void Config::setMaxSegmentSize(size_t size) { m_maxSegmentSize = size; }

unsigned Config::getMaxSegmentAgeSec() const { return m_maxSegmentAgeSec; }
void Config::setMaxSegmentAgeSec(unsigned ageSec) { m_maxSegmentAgeSec = ageSec; }

bool Config::isEncryptionEnabled() const { return m_encryptionEnabled; }
void Config::setEncryptionEnabled(bool enabled) { m_encryptionEnabled = enabled; }

std::string Config::getEncryptionKeyPath() const { return m_encryptionKeyPath; }
void Config::setEncryptionKeyPath(const std::string &path) { m_encryptionKeyPath = path; }

std::string Config::getHashAlgorithm() const { return m_hashAlgorithm; }
void Config::setHashAlgorithm(const std::string &algorithm) { m_hashAlgorithm = algorithm; }

bool Config::isCompressionEnabled() const { return m_compressionEnabled; }
void Config::setCompressionEnabled(bool enabled) { m_compressionEnabled = enabled; }

std::string Config::getDefaultExportDirectory() const { return m_defaultExportDirectory; }
void Config::setDefaultExportDirectory(const std::string &directory) { m_defaultExportDirectory = directory; }

bool Config::getSettingBool(const std::string &key, bool defaultValue) const
{
    auto it = m_boolSettings.find(key);
    return it != m_boolSettings.end() ? it->second : defaultValue;
}
void Config::setSettingBool(const std::string &key, bool value) { m_boolSettings[key] = value; }

int Config::getSettingInt(const std::string &key, int defaultValue) const
{
    auto it = m_intSettings.find(key);
    return it != m_intSettings.end() ? it->second : defaultValue;
}
void Config::setSettingInt(const std::string &key, int value) { m_intSettings[key] = value; }

std::string Config::getSettingString(const std::string &key, const std::string &defaultValue) const
{
    auto it = m_stringSettings.find(key);
    return it != m_stringSettings.end() ? it->second : defaultValue;
}
void Config::setSettingString(const std::string &key, const std::string &value) { m_stringSettings[key] = value; }
