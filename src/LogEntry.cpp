#include "LogEntry.hpp"
#include <sstream>
#include <cstring>
#include <stdexcept>
#include <atomic>
#include <openssl/sha.h>

std::atomic<uint64_t> LogEntry::s_nextSequenceNumber{0};

LogEntry::LogEntry(const std::string &action,
                   const std::string &dataSubject,
                   const std::string &dataCategory,
                   const std::string &dataLocation,
                   const std::string &requestReason,
                   const std::string &additionalInfo)
    : m_timestamp(std::time(nullptr)), m_action(action), m_dataSubject(dataSubject),
      m_dataCategory(dataCategory), m_dataLocation(dataLocation),
      m_requestReason(requestReason), m_additionalInfo(additionalInfo),
      m_sequenceNumber(s_nextSequenceNumber++) {}

LogEntry::LogEntry(std::time_t timestamp,
                   const std::string &action,
                   const std::string &dataSubject,
                   const std::string &dataCategory,
                   const std::string &dataLocation,
                   const std::string &requestReason,
                   const std::string &additionalInfo)
    : m_timestamp(timestamp), m_action(action), m_dataSubject(dataSubject),
      m_dataCategory(dataCategory), m_dataLocation(dataLocation),
      m_requestReason(requestReason), m_additionalInfo(additionalInfo),
      m_sequenceNumber(s_nextSequenceNumber++) {}

void LogEntry::setPreviousHash(const std::vector<uint8_t> &prevHash)
{
    m_previousHash = prevHash;
}

std::vector<uint8_t> LogEntry::calculateHash() const
{
    std::ostringstream oss;
    oss << m_timestamp << m_action << m_dataSubject << m_dataCategory
        << m_dataLocation << m_requestReason << m_additionalInfo
        << m_sequenceNumber;
    for (auto byte : m_previousHash)
    {
        oss << static_cast<int>(byte);
    }

    std::string dataStr = oss.str();
    std::vector<uint8_t> hash(SHA256_DIGEST_LENGTH);
    SHA256(reinterpret_cast<const unsigned char *>(dataStr.c_str()), dataStr.size(), hash.data());

    return hash;
}

bool LogEntry::verifyChain(const LogEntry &prevEntry) const
{
    return m_previousHash == prevEntry.calculateHash();
}

std::vector<uint8_t> LogEntry::serialize() const
{
    std::ostringstream oss;
    oss << m_timestamp << "|" << m_action << "|" << m_dataSubject << "|"
        << m_dataCategory << "|" << m_dataLocation << "|" << m_requestReason << "|"
        << m_additionalInfo << "|" << m_sequenceNumber;

    std::string serialized = oss.str();
    return std::vector<uint8_t>(serialized.begin(), serialized.end());
}

std::unique_ptr<LogEntry> LogEntry::deserialize(const std::vector<uint8_t> &data)
{
    std::string strData(data.begin(), data.end());
    std::istringstream iss(strData);
    std::string timestampStr, action, dataSubject, dataCategory, dataLocation, requestReason, additionalInfo, seqStr;

    if (!std::getline(iss, timestampStr, '|') || !std::getline(iss, action, '|') ||
        !std::getline(iss, dataSubject, '|') || !std::getline(iss, dataCategory, '|') ||
        !std::getline(iss, dataLocation, '|') || !std::getline(iss, requestReason, '|') ||
        !std::getline(iss, additionalInfo, '|') || !std::getline(iss, seqStr, '|'))
    {
        throw std::runtime_error("Deserialization failed: malformed data");
    }

    std::time_t timestamp = std::stoll(timestampStr);
    uint64_t sequenceNumber = std::stoull(seqStr);

    auto entry = std::make_unique<LogEntry>(timestamp, action, dataSubject, dataCategory, dataLocation, requestReason, additionalInfo);
    entry->m_sequenceNumber = sequenceNumber;
    return entry;
}
