#include "LogEntry.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <openssl/evp.h>
#include <stdexcept>

std::string actionTypeToString(LogEntry::ActionType actionType)
{
    switch (actionType)
    {
    case LogEntry::ActionType::CREATE:
        return "CREATE";
    case LogEntry::ActionType::READ:
        return "READ";
    case LogEntry::ActionType::UPDATE:
        return "UPDATE";
    case LogEntry::ActionType::DELETE:
        return "DELETE";
    default:
        throw std::invalid_argument("Unknown ActionType");
    }
}

LogEntry::LogEntry()
    : m_actionType(ActionType::CREATE),
      m_dataLocation(""),
      m_userId(""),
      m_dataSubjectId(""),
      m_timestamp(std::chrono::system_clock::now()),
      m_sequenceNumber(0) {}

LogEntry::LogEntry(ActionType actionType, const std::string &dataLocation,
                   const std::string &userId, const std::string &dataSubjectId)
    : m_actionType(actionType),
      m_dataLocation(dataLocation),
      m_userId(userId),
      m_dataSubjectId(dataSubjectId),
      m_timestamp(std::chrono::system_clock::now()),
      m_sequenceNumber(0) {}

// Set the previous log entry's hash (for tamper-evident chaining)
void LogEntry::setPreviousHash(const std::vector<uint8_t> &previousHash)
{
    m_previousHash = previousHash;
}

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

    // Previous hash (if any)
    if (!m_previousHash.empty())
    {
        for (auto &byte : m_previousHash)
        {
            oss << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(byte);
        }
    }
    else
    {
        oss << "0000000000000000";
    }

    // Sequence number
    oss << "|";
    oss << std::dec << m_sequenceNumber;

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

        // Deserialize previous hash (if any)
        std::string prevHashStr;
        std::getline(iss, prevHashStr, '|');
        m_previousHash.clear();
        if (prevHashStr != "0000000000000000")
        {
            for (size_t i = 0; i < prevHashStr.size(); i += 2)
            {
                unsigned int byte;
                std::istringstream(prevHashStr.substr(i, 2)) >> std::hex >> byte;
                m_previousHash.push_back(static_cast<uint8_t>(byte));
            }
        }

        // Deserialize sequence number
        iss >> m_sequenceNumber;

        return true;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Deserialization failed: " << e.what() << std::endl;
        return false;
    }
}

// Calculate the hash of the log entry
std::vector<uint8_t> LogEntry::calculateHash() const
{
    std::string data = toString();

    // Use EVP API for SHA-256
    std::vector<uint8_t> hash(EVP_MAX_MD_SIZE);
    unsigned int hash_len = 0;

    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);
    EVP_DigestUpdate(ctx, data.c_str(), data.size());
    EVP_DigestFinal_ex(ctx, hash.data(), &hash_len);
    EVP_MD_CTX_free(ctx);

    hash.resize(hash_len); // Adjust to actual hash size
    return hash;
}

// Convert the log entry to a human-readable string
std::string LogEntry::toString() const
{
    std::ostringstream oss;
    oss << "ActionType: " << actionTypeToString(m_actionType) << "\n"
        << "DataLocation: " << m_dataLocation << "\n"
        << "UserId: " << m_userId << "\n"
        << "DataSubjectId: " << m_dataSubjectId << "\n"
        << "Timestamp: " << std::chrono::system_clock::to_time_t(m_timestamp) << "\n"
        << "PreviousHash: ";

    if (!m_previousHash.empty())
    {
        for (auto byte : m_previousHash)
        {
            oss << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(byte);
        }
    }
    else
    {
        oss << "None";
    }

    oss << "\nSequenceNumber: " << m_sequenceNumber;
    return oss.str();
}
