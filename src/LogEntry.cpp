#include "LogEntry.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <stdexcept>

LogEntry::LogEntry()
    : m_actionType(ActionType::CREATE),
      m_dataLocation(""),
      m_userId(""),
      m_dataSubjectId(""),
      m_timestamp(std::chrono::system_clock::now()) {}

LogEntry::LogEntry(ActionType actionType, const std::string &dataLocation,
                   const std::string &userId, const std::string &dataSubjectId)
    : m_actionType(actionType),
      m_dataLocation(dataLocation),
      m_userId(userId),
      m_dataSubjectId(dataSubjectId),
      m_timestamp(std::chrono::system_clock::now()) {}

// Serialize the log entry into a byte vector
std::vector<uint8_t> LogEntry::serialize() const
{
    std::ostringstream oss;

    // Action type
    oss << static_cast<int>(m_actionType) << "|";

    // Data location, user ID, data subject ID
    oss << m_dataLocation << "|"
        << m_userId << "|"
        << m_dataSubjectId << "|";

    // Timestamp (convert to string)
    auto time_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(m_timestamp.time_since_epoch()).count();
    oss << time_since_epoch << "|";

    // Convert to string and then to a byte vector
    std::string logData = oss.str();
    return std::vector<uint8_t>(logData.begin(), logData.end());
}

// Deserialize a byte vector into a LogEntry object
bool LogEntry::deserialize(const std::vector<uint8_t> &data)
{
    try
    {
        std::string logData(data.begin(), data.end());
        std::istringstream iss(logData);

        int actionTypeInt;
        char separator;

        // Deserialize the action type
        iss >> actionTypeInt >> separator;
        m_actionType = static_cast<ActionType>(actionTypeInt);

        // Deserialize other fields
        std::getline(iss, m_dataLocation, '|');
        std::getline(iss, m_userId, '|');
        std::getline(iss, m_dataSubjectId, '|');

        // Deserialize timestamp
        long long timestampMillis;
        iss >> timestampMillis >> separator;
        m_timestamp = std::chrono::system_clock::time_point(std::chrono::milliseconds(timestampMillis));

        return true;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Deserialization failed: " << e.what() << std::endl;
        return false;
    }
}