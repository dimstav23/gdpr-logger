#pragma once

#include <string>
#include <map>
#include <vector>

/**
 * @brief Configuration for the GDPR logging system
 *
 * Holds all configuration parameters for the system and provides
 * methods to load/save configuration.
 */
class Config
{
public:
    /**
     * @brief Constructs a configuration with default values
     */
    Config();

    /**
     * @brief Constructs a configuration from a file
     *
     * @param configFilePath Path to configuration file
     * @throws ConfigException if file cannot be loaded
     */
    explicit Config(const std::string &configFilePath);

    /**
     * @brief Destructor
     */
    ~Config() = default;

    /**
     * @brief Loads configuration from a file
     *
     * @param configFilePath Path to configuration file
     * @return true if successful, false otherwise
     * @throws ConfigException if file cannot be loaded
     */
    bool loadFromFile(const std::string &configFilePath);

    /**
     * @brief Saves configuration to a file
     *
     * @param configFilePath Path to configuration file
     * @return true if successful, false otherwise
     * @throws ConfigException if file cannot be saved
     */
    bool saveToFile(const std::string &configFilePath) const;

    /**
     * @brief Validates the configuration
     *
     * @return true if valid, false otherwise
     */
    bool validate() const;

    // Getters and setters for configuration parameters

    // Buffer settings
    size_t getBufferCapacity() const;
    void setBufferCapacity(size_t capacity);

    // Writer settings
    size_t getWriterCount() const;
    void setWriterCount(size_t count);

    size_t getBatchSize() const;
    void setBatchSize(size_t size);

    unsigned getWriteIntervalMs() const;
    void setWriteIntervalMs(unsigned intervalMs);

    // Segment settings
    std::string getSegmentDirectory() const;
    void setSegmentDirectory(const std::string &directory);

    size_t getMaxSegmentSize() const;
    void setMaxSegmentSize(size_t size);

    unsigned getMaxSegmentAgeSec() const;
    void setMaxSegmentAgeSec(unsigned ageSec);

    // Crypto settings
    bool isEncryptionEnabled() const;
    void setEncryptionEnabled(bool enabled);

    std::string getEncryptionKeyPath() const;
    void setEncryptionKeyPath(const std::string &path);

    std::string getHashAlgorithm() const;
    void setHashAlgorithm(const std::string &algorithm);

    bool isCompressionEnabled() const;
    void setCompressionEnabled(bool enabled);

    // Export settings
    std::string getDefaultExportDirectory() const;
    void setDefaultExportDirectory(const std::string &directory);

    // Additional settings
    bool getSettingBool(const std::string &key, bool defaultValue = false) const;
    void setSettingBool(const std::string &key, bool value);

    int getSettingInt(const std::string &key, int defaultValue = 0) const;
    void setSettingInt(const std::string &key, int value);

    std::string getSettingString(const std::string &key, const std::string &defaultValue = "") const;
    void setSettingString(const std::string &key, const std::string &value);

private:
    // Buffer settings
    size_t m_bufferCapacity;

    // Writer settings
    size_t m_writerCount;
    size_t m_batchSize;
    unsigned m_writeIntervalMs;

    // Segment settings
    std::string m_segmentDirectory;
    size_t m_maxSegmentSize;
    unsigned m_maxSegmentAgeSec;

    // Crypto settings
    bool m_encryptionEnabled;
    std::string m_encryptionKeyPath;
    std::string m_hashAlgorithm;
    bool m_compressionEnabled;

    // Export settings
    std::string m_defaultExportDirectory;

    // Additional settings
    std::map<std::string, std::string> m_stringSettings;
    std::map<std::string, int> m_intSettings;
    std::map<std::string, bool> m_boolSettings;

    /**
     * @brief Sets default values for all configuration parameters
     */
    void setDefaults();
};