#include "LogEntry.hpp"
#include <cstring>
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

// Fast binary serialization with pre-allocated memory
std::vector<uint8_t> LogEntry::serialize() const
{
    // Calculate required size upfront
    size_t totalSize =
        sizeof(int) +                               // ActionType
        sizeof(uint32_t) + m_dataLocation.size() +  // Size + data location
        sizeof(uint32_t) + m_userId.size() +        // Size + user ID
        sizeof(uint32_t) + m_dataSubjectId.size() + // Size + data subject ID
        sizeof(int64_t);                            // Timestamp

    // Pre-allocate the vector to avoid reallocations
    std::vector<uint8_t> result;
    result.reserve(totalSize);

    // Push ActionType
    int actionType = static_cast<int>(m_actionType);
    appendToVector(result, &actionType, sizeof(actionType));

    // Push strings with their lengths
    appendStringToVector(result, m_dataLocation);
    appendStringToVector(result, m_userId);
    appendStringToVector(result, m_dataSubjectId);

    // Push timestamp
    int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                            m_timestamp.time_since_epoch())
                            .count();
    appendToVector(result, &timestamp, sizeof(timestamp));

    return result;
}

bool LogEntry::deserialize(const std::vector<uint8_t> &data)
{
    try
    {
        size_t offset = 0;

        // Check if we have enough data for the basic structure
        if (data.size() < sizeof(int))
            return false;

        // Extract action type
        int actionType;
        std::memcpy(&actionType, data.data() + offset, sizeof(actionType));
        offset += sizeof(actionType);
        m_actionType = static_cast<ActionType>(actionType);

        // Extract data location
        if (!extractStringFromVector(data, offset, m_dataLocation))
            return false;

        // Extract user ID
        if (!extractStringFromVector(data, offset, m_userId))
            return false;

        // Extract data subject ID
        if (!extractStringFromVector(data, offset, m_dataSubjectId))
            return false;

        // Extract timestamp
        if (offset + sizeof(int64_t) > data.size())
            return false;

        int64_t timestamp;
        std::memcpy(&timestamp, data.data() + offset, sizeof(timestamp));
        m_timestamp = std::chrono::system_clock::time_point(std::chrono::milliseconds(timestamp));

        return true;
    }
    catch (const std::exception &)
    {
        return false;
    }
}

// Helper method to append data to a vector
void LogEntry::appendToVector(std::vector<uint8_t> &vec, const void *data, size_t size) const
{
    const uint8_t *bytes = static_cast<const uint8_t *>(data);
    vec.insert(vec.end(), bytes, bytes + size);
}

// Helper method to append a string with its length
void LogEntry::appendStringToVector(std::vector<uint8_t> &vec, const std::string &str) const
{
    uint32_t length = static_cast<uint32_t>(str.size());
    appendToVector(vec, &length, sizeof(length));

    if (length > 0)
    {
        appendToVector(vec, str.data(), str.size());
    }
}

// Helper method to extract a string from a vector
bool LogEntry::extractStringFromVector(const std::vector<uint8_t> &vec, size_t &offset, std::string &str)
{
    // Check if we have enough data for the string length
    if (offset + sizeof(uint32_t) > vec.size())
        return false;

    uint32_t length;
    std::memcpy(&length, vec.data() + offset, sizeof(length));
    offset += sizeof(length);

    // Check if we have enough data for the string content
    if (offset + length > vec.size())
        return false;

    str.assign(reinterpret_cast<const char *>(vec.data() + offset), length);
    offset += length;

    return true;
}