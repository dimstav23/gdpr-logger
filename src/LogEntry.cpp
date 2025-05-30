#include "LogEntry.hpp"
#include <cstring>
#include <stdexcept>
#include <iostream>

LogEntry::LogEntry()
    : m_actionType(ActionType::CREATE),
      m_dataLocation(""),
      m_userId(""),
      m_dataSubjectId(""),
      m_timestamp(std::chrono::system_clock::now()),
      m_payload() {}

LogEntry::LogEntry(ActionType actionType,
                   std::string dataLocation,
                   std::string userId,
                   std::string dataSubjectId,
                   std::vector<uint8_t> payload)
    : m_actionType(actionType),
      m_dataLocation(std::move(dataLocation)),
      m_userId(std::move(userId)),
      m_dataSubjectId(std::move(dataSubjectId)),
      m_timestamp(std::chrono::system_clock::now()),
      m_payload(std::move(payload))
{
}

// Fast binary serialization with pre-allocated memory
std::vector<uint8_t> LogEntry::serialize() const
{
    // Calculate required size upfront (add variable data size)
    size_t totalSize =
        sizeof(int) +                               // ActionType
        sizeof(uint32_t) + m_dataLocation.size() +  // Size + data location
        sizeof(uint32_t) + m_userId.size() +        // Size + user ID
        sizeof(uint32_t) + m_dataSubjectId.size() + // Size + data subject ID
        sizeof(int64_t) +                           // Timestamp
        sizeof(uint32_t) + m_payload.size();        // Size + payload data

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

    // Push payload with its length
    uint32_t payloadSize = static_cast<uint32_t>(m_payload.size());
    appendToVector(result, &payloadSize, sizeof(payloadSize));
    if (!m_payload.empty())
    {
        appendToVector(result, m_payload.data(), m_payload.size());
    }

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
        offset += sizeof(timestamp);
        m_timestamp = std::chrono::system_clock::time_point(std::chrono::milliseconds(timestamp));

        // Extract payload
        if (offset + sizeof(uint32_t) > data.size())
            return false;

        uint32_t payloadSize;
        std::memcpy(&payloadSize, data.data() + offset, sizeof(payloadSize));
        offset += sizeof(payloadSize);

        if (offset + payloadSize > data.size())
            return false;

        // Copy the payload
        m_payload.resize(payloadSize);
        if (payloadSize > 0)
        {
            std::memcpy(m_payload.data(), data.data() + offset, payloadSize);
            offset += payloadSize;
        }

        return true;
    }
    catch (const std::exception &)
    {
        return false;
    }
}

std::vector<uint8_t> LogEntry::serializeBatch(const std::vector<LogEntry> &entries)
{
    std::vector<uint8_t> batchData;

    // First, store the number of entries
    uint32_t numEntries = entries.size();
    batchData.resize(sizeof(numEntries));
    std::memcpy(batchData.data(), &numEntries, sizeof(numEntries));

    // Then serialize and append each entry
    for (const auto &entry : entries)
    {
        // Get the serialized entry
        std::vector<uint8_t> entryData = entry.serialize();

        // Store the size of the serialized entry
        uint32_t entrySize = entryData.size();
        size_t currentSize = batchData.size();
        batchData.resize(currentSize + sizeof(entrySize));
        std::memcpy(batchData.data() + currentSize, &entrySize, sizeof(entrySize));

        // Store the serialized entry
        currentSize = batchData.size();
        batchData.resize(currentSize + entryData.size());
        std::memcpy(batchData.data() + currentSize, entryData.data(), entryData.size());
    }

    return batchData;
}

std::vector<LogEntry> LogEntry::deserializeBatch(const std::vector<uint8_t> &batchData)
{
    std::vector<LogEntry> entries;

    try
    {
        // Read the number of entries
        if (batchData.size() < sizeof(uint32_t))
        {
            throw std::runtime_error("Batch data too small to contain entry count");
        }

        uint32_t numEntries;
        std::memcpy(&numEntries, batchData.data(), sizeof(numEntries));

        // Position in the batch data
        size_t position = sizeof(numEntries);

        // Extract each entry
        for (uint32_t i = 0; i < numEntries; ++i)
        {
            // Check if we have enough data left to read the entry size
            if (position + sizeof(uint32_t) > batchData.size())
            {
                throw std::runtime_error("Unexpected end of batch data");
            }

            // Read the size of the entry
            uint32_t entrySize;
            std::memcpy(&entrySize, batchData.data() + position, sizeof(entrySize));
            position += sizeof(entrySize);

            // Check if we have enough data left to read the entry
            if (position + entrySize > batchData.size())
            {
                throw std::runtime_error("Unexpected end of batch data");
            }

            // Extract the entry data
            std::vector<uint8_t> entryData(batchData.begin() + position,
                                           batchData.begin() + position + entrySize);
            position += entrySize;

            // Deserialize the entry
            LogEntry entry;
            if (entry.deserialize(entryData))
            {
                entries.push_back(entry);
            }
            else
            {
                throw std::runtime_error("Failed to deserialize log entry");
            }
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error deserializing log batch: " << e.what() << std::endl;
    }

    return entries;
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