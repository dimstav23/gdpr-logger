#include "Crypto.hpp"
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/err.h>
#include <stdexcept>
#include <cstring>
#include <iostream>

Crypto::Crypto()
{
    // Initialize OpenSSL
    OpenSSL_add_all_algorithms();
}

Crypto::~Crypto()
{
    // Clean up OpenSSL
    EVP_cleanup();
}

// Generate a random initialization vector
std::vector<uint8_t> Crypto::generateIV(size_t size)
{
    std::vector<uint8_t> iv(size);
    if (RAND_bytes(iv.data(), size) != 1)
    {
        throw std::runtime_error("Failed to generate random IV");
    }
    return iv;
}

// Encrypt data using AES-256-GCM
std::vector<uint8_t> Crypto::encrypt(const std::vector<uint8_t> &compressedData, const std::vector<uint8_t> &key)
{
    if (compressedData.empty())
    {
        return std::vector<uint8_t>();
    }

    // Validate key size
    if (key.size() != KEY_SIZE)
    {
        throw std::runtime_error("Invalid key size. Expected 32 bytes for AES-256");
    }

    // Generate a random IV
    std::vector<uint8_t> iv = generateIV(GCM_IV_SIZE);

    // Initialize OpenSSL cipher context
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    if (!ctx)
    {
        throw std::runtime_error("Failed to create cipher context");
    }

    // Initialize encryption operation
    if (EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, key.data(), iv.data()) != 1)
    {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to initialize encryption");
    }

    // Prepare output buffer for ciphertext
    // The encrypted data might be larger than the input data due to padding
    std::vector<uint8_t> encryptedData(compressedData.size() + EVP_MAX_BLOCK_LENGTH);
    int encryptedLen = 0;

    // Perform encryption
    if (EVP_EncryptUpdate(ctx, encryptedData.data(), &encryptedLen,
                          compressedData.data(), compressedData.size()) != 1)
    {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed during encryption update");
    }

    // Finalize encryption
    int finalLen = 0;
    if (EVP_EncryptFinal_ex(ctx, encryptedData.data() + encryptedLen, &finalLen) != 1)
    {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to finalize encryption");
    }

    // Get the authentication tag
    std::vector<uint8_t> tag(GCM_TAG_SIZE);
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, GCM_TAG_SIZE, tag.data()) != 1)
    {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Failed to get authentication tag");
    }

    // Clean up
    EVP_CIPHER_CTX_free(ctx);

    // Resize the encrypted data to the actual length
    encryptedData.resize(encryptedLen + finalLen);

    // Format the output: IV + Ciphertext + Tag
    std::vector<uint8_t> result;

    // Store the IV size
    uint32_t ivSize = iv.size();
    result.resize(sizeof(ivSize));
    std::memcpy(result.data(), &ivSize, sizeof(ivSize));

    // Store the IV
    size_t currentSize = result.size();
    result.resize(currentSize + iv.size());
    std::memcpy(result.data() + currentSize, iv.data(), iv.size());

    // Store the encrypted data size
    uint32_t dataSize = encryptedData.size();
    currentSize = result.size();
    result.resize(currentSize + sizeof(dataSize));
    std::memcpy(result.data() + currentSize, &dataSize, sizeof(dataSize));

    // Store the encrypted data
    currentSize = result.size();
    result.resize(currentSize + encryptedData.size());
    std::memcpy(result.data() + currentSize, encryptedData.data(), encryptedData.size());

    // Store the tag
    currentSize = result.size();
    result.resize(currentSize + tag.size());
    std::memcpy(result.data() + currentSize, tag.data(), tag.size());

    return result;
}

// Decrypt data using AES-256-GCM
std::vector<uint8_t> Crypto::decrypt(const std::vector<uint8_t> &encryptedData, const std::vector<uint8_t> &key)
{
    try
    {
        if (encryptedData.empty())
        {
            return std::vector<uint8_t>();
        }

        // Validate key size
        if (key.size() != KEY_SIZE)
        {
            throw std::runtime_error("Invalid key size. Expected 32 bytes for AES-256");
        }

        // Ensure we have at least enough data for the IV size field
        if (encryptedData.size() < sizeof(uint32_t))
        {
            throw std::runtime_error("Encrypted data too small - missing IV size");
        }

        // Extract the IV size
        uint32_t ivSize;
        std::memcpy(&ivSize, encryptedData.data(), sizeof(ivSize));

        // Validate IV size
        if (ivSize != GCM_IV_SIZE || encryptedData.size() < sizeof(ivSize) + ivSize)
        {
            throw std::runtime_error("Invalid IV size or encrypted data too small");
        }

        // Extract the IV
        std::vector<uint8_t> iv(ivSize);
        std::memcpy(iv.data(), encryptedData.data() + sizeof(ivSize), ivSize);

        // Position after IV
        size_t position = sizeof(ivSize) + ivSize;

        // Extract the encrypted data size
        if (position + sizeof(uint32_t) > encryptedData.size())
        {
            throw std::runtime_error("Encrypted data too small - missing data size");
        }

        uint32_t dataSize;
        std::memcpy(&dataSize, encryptedData.data() + position, sizeof(dataSize));
        position += sizeof(dataSize);

        // Validate data size
        if (position + dataSize > encryptedData.size())
        {
            throw std::runtime_error("Encrypted data too small - missing complete data");
        }

        // Extract the encrypted data
        std::vector<uint8_t> ciphertext(dataSize);
        std::memcpy(ciphertext.data(), encryptedData.data() + position, dataSize);
        position += dataSize;

        // Extract the authentication tag
        if (position + GCM_TAG_SIZE > encryptedData.size())
        {
            throw std::runtime_error("Encrypted data too small - missing authentication tag");
        }

        std::vector<uint8_t> tag(GCM_TAG_SIZE);
        std::memcpy(tag.data(), encryptedData.data() + position, GCM_TAG_SIZE);

        // Initialize OpenSSL cipher context
        EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
        if (!ctx)
        {
            throw std::runtime_error("Failed to create cipher context");
        }

        // Initialize decryption operation
        if (EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, key.data(), iv.data()) != 1)
        {
            EVP_CIPHER_CTX_free(ctx);
            throw std::runtime_error("Failed to initialize decryption");
        }

        // Set expected tag value
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, GCM_TAG_SIZE, tag.data()) != 1)
        {
            EVP_CIPHER_CTX_free(ctx);
            throw std::runtime_error("Failed to set authentication tag");
        }

        // Prepare output buffer for plaintext
        std::vector<uint8_t> decryptedData(ciphertext.size());
        int decryptedLen = 0;

        // Perform decryption
        if (EVP_DecryptUpdate(ctx, decryptedData.data(), &decryptedLen,
                              ciphertext.data(), ciphertext.size()) != 1)
        {
            EVP_CIPHER_CTX_free(ctx);
            throw std::runtime_error("Failed during decryption update");
        }

        // Finalize decryption and verify tag
        int finalLen = 0;
        int ret = EVP_DecryptFinal_ex(ctx, decryptedData.data() + decryptedLen, &finalLen);

        // Clean up
        EVP_CIPHER_CTX_free(ctx);

        if (ret != 1)
        {
            throw std::runtime_error("Authentication failed: data may have been tampered with");
        }

        // Resize the decrypted data to the actual length
        decryptedData.resize(decryptedLen + finalLen);

        return decryptedData;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error decrypting data: " << e.what() << std::endl;
        // Print OpenSSL error queue
        ERR_print_errors_fp(stderr);
        return std::vector<uint8_t>();
    }
}