#pragma once

#include <string>
#include <ctime>
#include <vector>
#include <memory>

/**
 * @brief Status information about the logging system
 */
struct LoggingStatus
{
    size_t queueSize;               // Current number of entries in queue
    size_t activeWriters;           // Number of active writer threads
    size_t totalEntriesWritten;     // Total entries written since startup
    size_t currentSegmentSize;      // Size of current segment in bytes
    std::string currentSegmentPath; // Path to current segment file
};

/**
 * @brief Represents a single log entry in the GDPR logging system
 *
 * Contains all relevant information for GDPR compliance logging, including
 * data about the operation, subject, timing, and cryptographic verification.
 */
class LogEntry
{
public:
    /**
     * @brief Creates a new log entry with the current timestamp
     *
     * @param action Type of action performed (e.g., "READ", "UPDATE", "DELETE")
     * @param dataSubject Identifier for the data subject (e.g., user ID)
     * @param dataCategory Category of data being accessed (e.g., "PERSONAL", "SENSITIVE")
     * @param dataLocation Reference to where the data is stored
     * @param requestReason Reason for the data operation
     * @param additionalInfo Optional additional information
     */
    LogEntry(const std::string &action,
             const std::string &dataSubject,
             const std::string &dataCategory,
             const std::string &dataLocation,
             const std::string &requestReason,
             const std::string &additionalInfo = "");

    /**
     * @brief Creates a log entry with a specified timestamp
     *
     * @param timestamp Time when the operation occurred
     * @param action Type of action performed
     * @param dataSubject Identifier for the data subject
     * @param dataCategory Category of data being accessed
     * @param dataLocation Reference to where the data is stored
     * @param requestReason Reason for the data operation
     * @param additionalInfo Optional additional information
     */
    LogEntry(std::time_t timestamp,
             const std::string &action,
             const std::string &dataSubject,
             const std::string &dataCategory,
             const std::string &dataLocation,
             const std::string &requestReason,
             const std::string &additionalInfo = "");

    /**
     * @brief Destructor
     */
    ~LogEntry() = default;

    /**
     * @brief Serializes the log entry to a byte array
     *
     * @return Byte array containing the serialized log entry
     */
    std::vector<uint8_t> serialize() const;

    /**
     * @brief Deserializes a log entry from a byte array
     *
     * @param data Byte array containing a serialized log entry
     * @return Unique pointer to the deserialized LogEntry
     * @throws DeserializeException if data is invalid
     */
    static std::unique_ptr<LogEntry> deserialize(const std::vector<uint8_t> &data);

    /**
     * @brief Sets the previous entry's hash for tamper-evident chaining
     *
     * @param prevHash Hash of the previous log entry
     */
    void setPreviousHash(const std::vector<uint8_t> &prevHash);

    /**
     * @brief Calculates the hash of this log entry
     *
     * @return Byte array containing the hash
     */
    std::vector<uint8_t> calculateHash() const;

    /**
     * @brief Verifies that this entry properly references the previous entry
     *
     * @param prevEntry Previous log entry
     * @return true if the chain is valid, false if tampering detected
     */
    bool verifyChain(const LogEntry &prevEntry) const;

    // Getters
    std::time_t getTimestamp() const { return m_timestamp; }
    const std::string &getAction() const { return m_action; }
    const std::string &getDataSubject() const { return m_dataSubject; }
    const std::string &getDataCategory() const { return m_dataCategory; }
    const std::string &getDataLocation() const { return m_dataLocation; }
    const std::string &getRequestReason() const { return m_requestReason; }
    const std::string &getAdditionalInfo() const { return m_additionalInfo; }
    const std::vector<uint8_t> &getPreviousHash() const { return m_previousHash; }

private:
    std::time_t m_timestamp;
    std::string m_action;
    std::string m_dataSubject;
    std::string m_dataCategory;
    std::string m_dataLocation;
    std::string m_requestReason;
    std::string m_additionalInfo;
    std::vector<uint8_t> m_previousHash; // Hash of previous entry for tamper-evident chaining

    // Unique sequence number for this entry
    uint64_t m_sequenceNumber;

    // Static counter to generate unique sequence numbers
    static std::atomic<uint64_t> s_nextSequenceNumber;
};