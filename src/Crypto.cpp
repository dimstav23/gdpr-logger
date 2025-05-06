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

// Encrypt data using AES-256-GCM with provided IV
std::vector<uint8_t> Crypto::encrypt(const std::vector<uint8_t> &compressedData,
                                     const std::vector<uint8_t> &key,
                                     const std::vector<uint8_t> &iv)
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

    // Validate IV size
    if (iv.size() != GCM_IV_SIZE)
    {
        throw std::runtime_error("Invalid IV size. Expected 12 bytes for GCM");
    }

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

    // Format the output: Ciphertext + Tag (no IV)
    std::vector<uint8_t> result;

    // Store the encrypted data size
    uint32_t dataSize = encryptedData.size();
    result.resize(sizeof(dataSize));
    std::memcpy(result.data(), &dataSize, sizeof(dataSize));

    // Store the encrypted data
    size_t currentSize = result.size();
    result.resize(currentSize + encryptedData.size());
    std::memcpy(result.data() + currentSize, encryptedData.data(), encryptedData.size());

    // Store the tag
    currentSize = result.size();
    result.resize(currentSize + tag.size());
    std::memcpy(result.data() + currentSize, tag.data(), tag.size());

    return result;
}

// Decrypt data using AES-256-GCM with provided IV
std::vector<uint8_t> Crypto::decrypt(const std::vector<uint8_t> &encryptedData,
                                     const std::vector<uint8_t> &key,
                                     const std::vector<uint8_t> &iv)
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

        // Validate IV size
        if (iv.size() != GCM_IV_SIZE)
        {
            throw std::runtime_error("Invalid IV size. Expected 12 bytes for GCM");
        }

        // Ensure we have at least enough data for the data size field
        if (encryptedData.size() < sizeof(uint32_t))
        {
            throw std::runtime_error("Encrypted data too small - missing data size");
        }

        // Extract the encrypted data size
        uint32_t dataSize;
        std::memcpy(&dataSize, encryptedData.data(), sizeof(dataSize));
        size_t position = sizeof(dataSize);

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