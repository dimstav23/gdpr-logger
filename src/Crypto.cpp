#include "Crypto.hpp"
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/err.h>
#include <stdexcept>
#include <cstring>
#include <iostream>

// Thread-local storage
thread_local EVP_CIPHER_CTX* encrypt_ctx = nullptr;
thread_local EVP_CIPHER_CTX* decrypt_ctx = nullptr;

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

EVP_CIPHER_CTX* Crypto::getEncryptContext()
{
    if (!encrypt_ctx) {
        encrypt_ctx = EVP_CIPHER_CTX_new();
        if (!encrypt_ctx) {
            throw std::runtime_error("Failed to create thread-local encryption context");
        }
    }
    return encrypt_ctx;
}

EVP_CIPHER_CTX* Crypto::getDecryptContext()
{
    if (!decrypt_ctx) {
        decrypt_ctx = EVP_CIPHER_CTX_new();
        if (!decrypt_ctx) {
            throw std::runtime_error("Failed to create thread-local decryption context");
        }
    }
    return decrypt_ctx;
}

// Encrypt data using AES-256-GCM with provided IV
std::vector<uint8_t> Crypto::encrypt(std::vector<uint8_t> &&plaintext,
                                     const std::vector<uint8_t> &key,
                                     const std::vector<uint8_t> &iv)
{
    if (plaintext.empty())
        return {};
    if (key.size() != KEY_SIZE)
        throw std::runtime_error("Invalid key size");
    if (iv.size() != GCM_IV_SIZE)
        throw std::runtime_error("Invalid IV size");

    // Reset the existing context instead of creating a new one
    EVP_CIPHER_CTX* m_encryptCtx = getEncryptContext();
    EVP_CIPHER_CTX_reset(m_encryptCtx);

    // Initialize encryption operation
    if (EVP_EncryptInit_ex(m_encryptCtx, EVP_aes_256_gcm(), nullptr, key.data(), iv.data()) != 1)
    {
        throw std::runtime_error("Failed to initialize encryption");
    }

    // Calculate the exact output size: size_field + ciphertext + tag
    // For GCM mode, ciphertext size equals plaintext size (no padding)
    const size_t sizeFieldSize = sizeof(uint32_t);
    const size_t ciphertextSize = plaintext.size();
    const size_t totalSize = sizeFieldSize + ciphertextSize + GCM_TAG_SIZE;

    // Pre-allocate result buffer with exact final size
    std::vector<uint8_t> result(totalSize);

    // Reserve space for data size field
    uint32_t dataSize = ciphertextSize;
    std::memcpy(result.data(), &dataSize, sizeFieldSize);

    // Perform encryption directly into the result buffer (after the size field)
    int encryptedLen = 0;
    if (EVP_EncryptUpdate(m_encryptCtx, result.data() + sizeFieldSize, &encryptedLen,
                          plaintext.data(), plaintext.size()) != 1)
    {
        throw std::runtime_error("Failed during encryption update");
    }

    // Finalize encryption (writing to the buffer right after the existing encrypted data)
    int finalLen = 0;
    if (EVP_EncryptFinal_ex(m_encryptCtx, result.data() + sizeFieldSize + encryptedLen, &finalLen) != 1)
    {
        throw std::runtime_error("Failed to finalize encryption");
    }

    // Sanity check: for GCM, encryptedLen + finalLen should equal plaintext.size()
    if (encryptedLen + finalLen != static_cast<int>(plaintext.size()))
    {
        throw std::runtime_error("Unexpected encryption output size");
    }

    // Get the authentication tag and write it directly to the result buffer
    if (EVP_CIPHER_CTX_ctrl(m_encryptCtx, EVP_CTRL_GCM_GET_TAG, GCM_TAG_SIZE,
                            result.data() + sizeFieldSize + ciphertextSize) != 1)
    {
        throw std::runtime_error("Failed to get authentication tag");
    }

    return result;
}

// Decrypt data using AES-256-GCM with provided IV
std::vector<uint8_t> Crypto::decrypt(const std::vector<uint8_t> &encryptedData,
                                     const std::vector<uint8_t> &key,
                                     const std::vector<uint8_t> &iv)
{
    EVP_CIPHER_CTX* m_decryptCtx = getDecryptContext();
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

        // Reset the existing context instead of creating a new one
        EVP_CIPHER_CTX_reset(m_decryptCtx);

        // Initialize decryption operation
        if (EVP_DecryptInit_ex(m_decryptCtx, EVP_aes_256_gcm(), nullptr, key.data(), iv.data()) != 1)
        {
            throw std::runtime_error("Failed to initialize decryption");
        }

        // Set expected tag value
        if (EVP_CIPHER_CTX_ctrl(m_decryptCtx, EVP_CTRL_GCM_SET_TAG, GCM_TAG_SIZE, tag.data()) != 1)
        {
            throw std::runtime_error("Failed to set authentication tag");
        }

        // Prepare output buffer for plaintext
        std::vector<uint8_t> decryptedData(ciphertext.size());
        int decryptedLen = 0;

        // Perform decryption
        if (EVP_DecryptUpdate(m_decryptCtx, decryptedData.data(), &decryptedLen,
                              ciphertext.data(), ciphertext.size()) != 1)
        {
            throw std::runtime_error("Failed during decryption update");
        }

        // Finalize decryption and verify tag
        int finalLen = 0;
        int ret = EVP_DecryptFinal_ex(m_decryptCtx, decryptedData.data() + decryptedLen, &finalLen);

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

// Thread-local destructor (automatic cleanup)
struct ThreadLocalCleanup {
    ~ThreadLocalCleanup() {
        if (encrypt_ctx) {
            EVP_CIPHER_CTX_free(encrypt_ctx);
            encrypt_ctx = nullptr;
        }
        if (decrypt_ctx) {
            EVP_CIPHER_CTX_free(decrypt_ctx);
            decrypt_ctx = nullptr;
        }
    }
};

thread_local ThreadLocalCleanup cleanup_helper;