#include "LogEntry.hpp"
#include <cstring>
#include <stdexcept>
#include <iostream>

LogEntry::LogEntry()
    : m_actionType(ActionType::CREATE),
      m_dataLocation(""),
      m_dataControllerId(""),
      m_dataProcessorId(""),
      m_dataSubjectId(""),
      m_timestamp(std::chrono::system_clock::now()),
      m_payload() {}

LogEntry::LogEntry(ActionType actionType,
                   std::string dataLocation,
                   std::string dataControllerId,
                   std::string dataProcessorId,
                   std::string dataSubjectId,
                   std::vector<uint8_t> payload)
    : m_actionType(actionType),
      m_dataLocation(std::move(dataLocation)),
      m_dataControllerId(std::move(dataControllerId)),
      m_dataProcessorId(std::move(dataProcessorId)),
      m_dataSubjectId(std::move(dataSubjectId)),
      m_timestamp(std::chrono::system_clock::now()),
      m_payload(std::move(payload))
{
}

// Move version that consumes the LogEntry
std::vector<uint8_t> LogEntry::serialize() &&
{
    // Calculate required size upfront
    size_t totalSize =
        sizeof(int) +                                  // ActionType
        sizeof(uint32_t) + m_dataLocation.size() +     // Size + data location
        sizeof(uint32_t) + m_dataControllerId.size() + // Size + data controller ID
        sizeof(uint32_t) + m_dataProcessorId.size() +  // Size + data processor ID
        sizeof(uint32_t) + m_dataSubjectId.size() +    // Size + data subject ID
        sizeof(int64_t) +                              // Timestamp
        sizeof(uint32_t) + m_payload.size();           // Size + payload data

    // Pre-allocate the vector
    std::vector<uint8_t> result;
    result.reserve(totalSize);

    // Push ActionType
    int actionType = static_cast<int>(m_actionType);
    appendToVector(result, &actionType, sizeof(actionType));

    // Move strings
    appendStringToVector(result, std::move(m_dataLocation));
    appendStringToVector(result, std::move(m_dataControllerId));
    appendStringToVector(result, std::move(m_dataProcessorId));
    appendStringToVector(result, std::move(m_dataSubjectId));

    // Push timestamp
    int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                            m_timestamp.time_since_epoch())
                            .count();
    appendToVector(result, &timestamp, sizeof(timestamp));

    // Move payload
    uint32_t payloadSize = static_cast<uint32_t>(m_payload.size());
    appendToVector(result, &payloadSize, sizeof(payloadSize));
    if (!m_payload.empty())
    {
        result.insert(result.end(),
                      std::make_move_iterator(m_payload.begin()),
                      std::make_move_iterator(m_payload.end()));
    }

    return result;
}

// Const version for when you need to keep the LogEntry
std::vector<uint8_t> LogEntry::serialize() const &
{
    // Calculate required size upfront
    size_t totalSize =
        sizeof(int) +                                  // ActionType
        sizeof(uint32_t) + m_dataLocation.size() +     // Size + data location
        sizeof(uint32_t) + m_dataControllerId.size() + // Size + data controller  ID
        sizeof(uint32_t) + m_dataProcessorId.size() +  // Size + data processor  ID
        sizeof(uint32_t) + m_dataSubjectId.size() +    // Size + data subject ID
        sizeof(int64_t) +                              // Timestamp
        sizeof(uint32_t) + m_payload.size();           // Size + payload data

    // Pre-allocate the vector
    std::vector<uint8_t> result;
    result.reserve(totalSize);

    // Push ActionType
    int actionType = static_cast<int>(m_actionType);
    appendToVector(result, &actionType, sizeof(actionType));

    // Copy strings
    appendStringToVector(result, m_dataLocation);
    appendStringToVector(result, m_dataControllerId);
    appendStringToVector(result, m_dataProcessorId);
    appendStringToVector(result, m_dataSubjectId);

    // Push timestamp
    int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                            m_timestamp.time_since_epoch())
                            .count();
    appendToVector(result, &timestamp, sizeof(timestamp));

    // Copy payload
    uint32_t payloadSize = static_cast<uint32_t>(m_payload.size());
    appendToVector(result, &payloadSize, sizeof(payloadSize));
    if (!m_payload.empty())
    {
        appendToVector(result, m_payload.data(), m_payload.size());
    }

    return result;
}

bool LogEntry::deserialize(std::vector<uint8_t> &&data)
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

        // Extract data controller ID
        if (!extractStringFromVector(data, offset, m_dataControllerId))
            return false;

        // Extract data processor ID
        if (!extractStringFromVector(data, offset, m_dataProcessorId))
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

        if (payloadSize > 0)
        {
            m_payload.clear();
            m_payload.reserve(payloadSize);

            auto start_it = data.begin() + offset;
            auto end_it = start_it + payloadSize;
            m_payload.assign(std::make_move_iterator(start_it),
                             std::make_move_iterator(end_it));
            offset += payloadSize;
        }
        else
        {
            m_payload.clear();
        }

        return true;
    }
    catch (const std::exception &)
    {
        return false;
    }
}

std::vector<uint8_t> LogEntry::serializeBatch(std::vector<LogEntry> &&entries)
{
    if (entries.empty())
    {
        // Just return a vector with count = 0
        std::vector<uint8_t> batchData(sizeof(uint32_t));
        uint32_t numEntries = 0;
        std::memcpy(batchData.data(), &numEntries, sizeof(numEntries));
        return batchData;
    }

    // Pre-calculate approximate total size to minimize reallocations
    size_t estimatedSize = sizeof(uint32_t); // Number of entries
    for (const auto &entry : entries)
    {
        // Rough estimate: header size + string sizes + payload size
        estimatedSize += sizeof(uint32_t) +     // Entry size field
                         sizeof(int) +          // ActionType
                         3 * sizeof(uint32_t) + // 3 string length fields
                         entry.getDataLocation().size() +
                         entry.getDataControllerId().size() +
                         entry.getDataProcessorId().size() +
                         entry.getDataSubjectId().size() +
                         sizeof(int64_t) +  // Timestamp
                         sizeof(uint32_t) + // Payload size
                         entry.getPayload().size();
    }

    std::vector<uint8_t> batchData;
    batchData.reserve(estimatedSize);

    // Store the number of entries
    uint32_t numEntries = static_cast<uint32_t>(entries.size());
    batchData.resize(sizeof(numEntries));
    std::memcpy(batchData.data(), &numEntries, sizeof(numEntries));

    // Serialize and append each entry using move semantics
    for (auto &entry : entries)
    {
        // Move-serialize the entry
        std::vector<uint8_t> entryData = std::move(entry).serialize();

        // Store the size of the serialized entry
        uint32_t entrySize = static_cast<uint32_t>(entryData.size());
        size_t currentSize = batchData.size();
        batchData.resize(currentSize + sizeof(entrySize));
        std::memcpy(batchData.data() + currentSize, &entrySize, sizeof(entrySize));

        // Move the serialized entry data
        batchData.insert(batchData.end(),
                         std::make_move_iterator(entryData.begin()),
                         std::make_move_iterator(entryData.end()));
    }

    return batchData;
}

std::vector<LogEntry> LogEntry::deserializeBatch(std::vector<uint8_t> &&batchData)
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

        // Reserve space for entries to avoid reallocations
        entries.reserve(numEntries);

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

            // Create entry data by moving a slice from the batch data
            std::vector<uint8_t> entryData;
            entryData.reserve(entrySize);

            auto start_it = batchData.begin() + position;
            auto end_it = start_it + entrySize;
            entryData.assign(std::make_move_iterator(start_it),
                             std::make_move_iterator(end_it));
            position += entrySize;

            // Deserialize the entry using move semantics
            LogEntry entry;
            if (entry.deserialize(std::move(entryData)))
            {
                entries.emplace_back(std::move(entry));
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

// Helper method to append a string with its length (const version)
void LogEntry::appendStringToVector(std::vector<uint8_t> &vec, const std::string &str) const
{
    uint32_t length = static_cast<uint32_t>(str.size());
    appendToVector(vec, &length, sizeof(length));

    if (length > 0)
    {
        appendToVector(vec, str.data(), str.size());
    }
}

// Helper method to append a string with its length (move version)
void LogEntry::appendStringToVector(std::vector<uint8_t> &vec, std::string &&str)
{
    uint32_t length = static_cast<uint32_t>(str.size());
    appendToVector(vec, &length, sizeof(length));

    if (length > 0)
    {
        vec.insert(vec.end(), str.begin(), str.end());
    }
}

// Helper method to extract a string from a vector
bool LogEntry::extractStringFromVector(std::vector<uint8_t> &vec, size_t &offset, std::string &str)
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