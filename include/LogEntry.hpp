#ifndef LOG_ENTRY_HPP
#define LOG_ENTRY_HPP

#include <string>
#include <chrono>
#include <vector>
#include <memory>
#include <cstdint>

class LogEntry
{
public:
    enum class ActionType
    {
        CREATE,
        READ,
        UPDATE,
        DELETE,
    };

    LogEntry();

    LogEntry(
        ActionType actionType,
        const std::string &dataLocation,
        const std::string &userId,
        const std::string &dataSubjectId);

    // set the previous entry's hash to maintain the chain
    void setPreviousHash(const std::vector<uint8_t> &previousHash);

    // Calculate the hash of this entry, returns vector of bytes representing the hash
    std::vector<uint8_t> calculateHash() const;

    std::vector<uint8_t> serialize() const;

    bool deserialize(const std::vector<uint8_t> &data);

    std::string toString() const;

    ActionType getActionType() const { return m_actionType; }
    void setActionType(ActionType actionType) { m_actionType = actionType; }

    std::string getDataLocation() const { return m_dataLocation; }
    void setDataLocation(const std::string &dataLocation) { m_dataLocation = dataLocation; }

    std::string getUserId() const { return m_userId; }
    void setUserId(const std::string &userId) { m_userId = userId; }

    std::string getDataSubjectId() const { return m_dataSubjectId; }
    void setDataSubjectId(const std::string &dataSubjectId) { m_dataSubjectId = dataSubjectId; }

    std::chrono::system_clock::time_point getTimestamp() const { return m_timestamp; }
    void setTimestamp(const std::chrono::system_clock::time_point &timestamp) { m_timestamp = timestamp; }

    const std::vector<uint8_t> &getPreviousHash() const { return m_previousHash; }

private:
    ActionType m_actionType;                           // Type of GDPR operation
    std::string m_dataLocation;                        // Location of the data being operated on
    std::string m_userId;                              // ID of the user performing the operation
    std::string m_dataSubjectId;                       // ID of the data subject
    std::chrono::system_clock::time_point m_timestamp; // When the operation occurred
    std::vector<uint8_t> m_previousHash;               // Hash of the previous log entry (for chaining)
};

// Helper functions
std::string actionTypeToString(LogEntry::ActionType actionType);

#endif