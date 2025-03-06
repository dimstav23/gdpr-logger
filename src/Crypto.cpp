#include "Crypto.hpp"
#include <stdexcept>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <openssl/rand.h>
#include <zlib.h>

struct Crypto::CryptoImpl
{
    std::string hashAlgorithm;
    bool encryptionEnabled;
    std::vector<uint8_t> encryptionKey;
};

Crypto::Crypto(bool encryptionEnabled, const std::string &encryptionKey, const std::string &hashAlgorithm)
    : m_encryptionEnabled(encryptionEnabled), m_hashAlgorithm(hashAlgorithm), m_impl(std::make_unique<CryptoImpl>())
{
    if (encryptionEnabled)
    {
        initializeEncryption(encryptionKey);
    }
    initializeHashing();
}

Crypto::~Crypto() = default;

void Crypto::initializeEncryption(const std::string &key)
{
    if (!key.empty())
    {
        m_impl->encryptionKey.assign(key.begin(), key.end());
    }
    else
    {
        m_impl->encryptionKey.resize(32);
        RAND_bytes(m_impl->encryptionKey.data(), 32);
    }
}

void Crypto::initializeHashing()
{
    m_impl->hashAlgorithm = m_hashAlgorithm;
}

std::vector<uint8_t> Crypto::hash(const std::vector<uint8_t> &data)
{
    std::vector<uint8_t> digest(SHA256_DIGEST_LENGTH);
    SHA256(data.data(), data.size(), digest.data());
    return digest;
}

std::vector<uint8_t> Crypto::compress(const std::vector<uint8_t> &data)
{
    uLongf compressedSize = compressBound(data.size());
    std::vector<uint8_t> compressedData(compressedSize);
    if (compress2(compressedData.data(), &compressedSize, data.data(), data.size(), Z_BEST_COMPRESSION) != Z_OK)
    {
        throw std::runtime_error("Compression failed");
    }
    compressedData.resize(compressedSize);
    return compressedData;
}

std::vector<uint8_t> Crypto::decompress(const std::vector<uint8_t> &data)
{
    uLongf decompressedSize = data.size() * 4;
    std::vector<uint8_t> decompressedData(decompressedSize);
    if (uncompress(decompressedData.data(), &decompressedSize, data.data(), data.size()) != Z_OK)
    {
        throw std::runtime_error("Decompression failed");
    }
    decompressedData.resize(decompressedSize);
    return decompressedData;
}

bool Crypto::isEncryptionEnabled() const
{
    return m_encryptionEnabled;
}

std::string Crypto::getHashAlgorithm() const
{
    return m_impl->hashAlgorithm;
}
