#pragma once

#include <vector>
#include <string>
#include <memory>

/**
 * @brief Provides cryptographic operations for the logging system
 *
 * Handles hashing, encryption, decryption, and key management.
 */
class Crypto
{
public:
    /**
     * @brief Constructs a crypto utility with the given parameters
     *
     * @param encryptionEnabled Whether encryption is enabled
     * @param encryptionKey Key used for encryption/decryption (if empty, a new one is generated)
     * @param hashAlgorithm Hash algorithm to use (default: SHA-256)
     * @throws CryptoException if initialization fails
     */
    Crypto(bool encryptionEnabled,
           const std::string &encryptionKey = "",
           const std::string &hashAlgorithm = "SHA-256");

    /**
     * @brief Destructor ensures secure cleanup of cryptographic resources
     */
    ~Crypto();

    /**
     * @brief Delete copy constructor and assignment operator
     */
    Crypto(const Crypto &) = delete;
    Crypto &operator=(const Crypto &) = delete;

    /**
     * @brief Calculates the hash of data
     *
     * @param data Data to hash
     * @return Calculated hash
     */
    std::vector<uint8_t> hash(const std::vector<uint8_t> &data);

    /**
     * @brief Encrypts data
     *
     * @param plaintext Data to encrypt
     * @return Encrypted data
     * @throws CryptoException if encryption fails
     */
    std::vector<uint8_t> encrypt(const std::vector<uint8_t> &plaintext);

    /**
     * @brief Decrypts data
     *
     * @param ciphertext Data to decrypt
     * @return Decrypted data
     * @throws CryptoException if decryption fails
     */
    std::vector<uint8_t> decrypt(const std::vector<uint8_t> &ciphertext);

    /**
     * @brief Compresses data
     *
     * @param data Data to compress
     * @return Compressed data
     * @throws CryptoException if compression fails
     */
    std::vector<uint8_t> compress(const std::vector<uint8_t> &data);

    /**
     * @brief Decompresses data
     *
     * @param data Data to decompress
     * @return Decompressed data
     * @throws CryptoException if decompression fails
     */
    std::vector<uint8_t> decompress(const std::vector<uint8_t> &data);

    /**
     * @brief Generates a new encryption key
     *
     * @return Generated key
     */
    std::string generateKey();

    /**
     * @brief Exports the current encryption key (for backup purposes)
     *
     * @param password Password to protect the exported key
     * @return Protected key data
     * @throws CryptoException if export fails
     */
    std::vector<uint8_t> exportKey(const std::string &password);

    /**
     * @brief Imports an encryption key
     *
     * @param keyData Protected key data
     * @param password Password to decrypt the key
     * @return true if import was successful, false otherwise
     * @throws CryptoException if import fails
     */
    bool importKey(const std::vector<uint8_t> &keyData, const std::string &password);

    /**
     * @brief Checks if encryption is enabled
     *
     * @return true if enabled, false otherwise
     */
    bool isEncryptionEnabled() const;

    /**
     * @brief Gets the name of the hash algorithm being used
     *
     * @return Hash algorithm name
     */
    std::string getHashAlgorithm() const;

private:
    bool m_encryptionEnabled;
    std::string m_hashAlgorithm;

    // Implementation will depend on the cryptographic library used
    // These could be pointers to library-specific contexts
    struct CryptoImpl;
    std::unique_ptr<CryptoImpl> m_impl;

    // Private implementation methods
    void initializeEncryption(const std::string &key);
    void initializeHashing();
};