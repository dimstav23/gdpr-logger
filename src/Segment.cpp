#include "Segment.hpp"
#include "LogEntry.hpp"
#include "Crypto.hpp"
#include <stdexcept>
#include <fstream>
#include <chrono>

Segment::Segment(const std::string &path,
                 std::shared_ptr<Crypto> crypto,
                 size_t maxSize,
                 std::time_t maxAge,
                 bool compress,
                 bool encrypt)
    : m_path(path), m_crypto(std::move(crypto)), m_maxSize(maxSize), m_maxAge(maxAge),
      m_compress(compress), m_encrypt(encrypt), m_currentSize(0), m_sealed(false)
{
    m_creationTime = std::time(nullptr);
    if (!openFile())
    {
        throw std::runtime_error("Failed to open segment file: " + path);
    }
    writeHeader();
}

Segment::~Segment()
{
    if (!m_sealed)
    {
        seal();
    }
    m_file.close();
}

bool Segment::openFile()
{
    m_file.open(m_path, std::ios::out | std::ios::app | std::ios::binary);
    return m_file.is_open();
}

bool Segment::writeHeader()
{
    if (!m_file.is_open())
        return false;
    m_file << "SEGMENT HEADER\n";
    return true;
}

std::vector<uint8_t> Segment::append(const LogEntry &entry, const std::vector<uint8_t> &previousHash)
{
    if (m_sealed)
    {
        throw std::runtime_error("Cannot append to a sealed segment.");
    }

    std::string serializedEntry = entry.serialize();
    if (m_compress)
    {
        serializedEntry = Crypto::compress(serializedEntry);
    }
    if (m_encrypt)
    {
        serializedEntry = m_crypto->encrypt(serializedEntry);
    }

    m_file << serializedEntry << "\n";
    m_currentSize += serializedEntry.size();

    std::vector<uint8_t> entryHash = m_crypto->hash(serializedEntry, previousHash);
    m_lastEntryHash = entryHash;
    return entryHash;
}

bool Segment::shouldRotate() const
{
    return (m_currentSize >= m_maxSize) || (std::time(nullptr) - m_creationTime >= m_maxAge);
}

bool Segment::seal()
{
    if (m_sealed)
        return false;

    m_file << "SEGMENT FOOTER\n";
    m_segmentHash = m_crypto->hash(m_lastEntryHash);
    m_file << "Final Hash: " << m_segmentHash.data() << "\n";
    m_sealed = true;
    return true;
}

bool Segment::isSealed() const
{
    return m_sealed;
}

size_t Segment::size() const
{
    return m_currentSize;
}

std::time_t Segment::age() const
{
    return std::time(nullptr) - m_creationTime;
}

std::string Segment::getPath() const
{
    return m_path;
}

bool Segment::verifyIntegrity()
{
    // Implement hash chain verification here
    return true;
}

size_t Segment::exportEntries(const std::string &outputPath)
{
    std::ifstream inputFile(m_path, std::ios::binary);
    std::ofstream outputFile(outputPath, std::ios::binary);
    if (!inputFile || !outputFile)
    {
        throw std::runtime_error("Failed to open files for export.");
    }

    std::string line;
    size_t count = 0;
    while (std::getline(inputFile, line))
    {
        outputFile << line << "\n";
        count++;
    }
    return count;
}
