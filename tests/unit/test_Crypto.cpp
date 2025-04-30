#include <gtest/gtest.h>
#include "Crypto.hpp"
#include <string>
#include <vector>
#include <algorithm>

class CryptoTest : public ::testing::Test
{
protected:
    Crypto crypto;

    // Helper method to create a random key of proper size
    std::vector<uint8_t> createRandomKey()
    {
        std::vector<uint8_t> key(32); // KEY_SIZE = 32
        for (size_t i = 0; i < key.size(); ++i)
        {
            key[i] = static_cast<uint8_t>(rand() % 256);
        }
        return key;
    }

    // Helper method to convert string to byte vector
    std::vector<uint8_t> stringToBytes(const std::string &str)
    {
        return std::vector<uint8_t>(str.begin(), str.end());
    }

    // Helper method to convert byte vector to string
    std::string bytesToString(const std::vector<uint8_t> &bytes)
    {
        return std::string(bytes.begin(), bytes.end());
    }

    void SetUp() override
    {
        // Seed random number generator for consistent test results
        srand(42);
    }
};

// Test empty data encryption and decryption
TEST_F(CryptoTest, EmptyData)
{
    std::vector<uint8_t> emptyData;
    std::vector<uint8_t> key = createRandomKey();

    // Encrypt empty data
    std::vector<uint8_t> encrypted = crypto.encrypt(emptyData, key);
    EXPECT_TRUE(encrypted.empty());

    // Decrypt empty data
    std::vector<uint8_t> decrypted = crypto.decrypt(encrypted, key);
    EXPECT_TRUE(decrypted.empty());
}

// Test basic encryption and decryption
TEST_F(CryptoTest, BasicEncryptDecrypt)
{
    std::string testMessage = "This is a test message for encryption";
    std::vector<uint8_t> data = stringToBytes(testMessage);
    std::vector<uint8_t> key = createRandomKey();

    // Encrypt the data
    std::vector<uint8_t> encrypted = crypto.encrypt(data, key);
    EXPECT_FALSE(encrypted.empty());

    // The encrypted data should be different from the original
    EXPECT_NE(data, encrypted);

    // Decrypt the data
    std::vector<uint8_t> decrypted = crypto.decrypt(encrypted, key);

    // The decrypted data should match the original
    EXPECT_EQ(data, decrypted);
    EXPECT_EQ(testMessage, bytesToString(decrypted));
}

// Test encryption with various data sizes
TEST_F(CryptoTest, VariousDataSizes)
{
    std::vector<size_t> sizes = {10, 100, 1000, 10000};
    std::vector<uint8_t> key = createRandomKey();

    for (size_t size : sizes)
    {
        // Create data of specified size
        std::vector<uint8_t> data(size);
        for (size_t i = 0; i < size; ++i)
        {
            data[i] = static_cast<uint8_t>(i % 256);
        }

        // Encrypt the data
        std::vector<uint8_t> encrypted = crypto.encrypt(data, key);
        EXPECT_FALSE(encrypted.empty());

        // Decrypt the data
        std::vector<uint8_t> decrypted = crypto.decrypt(encrypted, key);

        // The decrypted data should match the original
        EXPECT_EQ(data, decrypted);
    }
}

// Test encryption with invalid key size
TEST_F(CryptoTest, InvalidKeySize)
{
    std::string testMessage = "Testing invalid key size";
    std::vector<uint8_t> data = stringToBytes(testMessage);

    // Create keys with invalid sizes
    std::vector<uint8_t> shortKey(16); // Too short
    std::vector<uint8_t> longKey(64);  // Too long

    // Encryption with short key should throw
    EXPECT_THROW(crypto.encrypt(data, shortKey), std::runtime_error);

    // Encryption with long key should throw
    EXPECT_THROW(crypto.encrypt(data, longKey), std::runtime_error);
}

// Test decryption with wrong key
TEST_F(CryptoTest, WrongKey)
{
    std::string testMessage = "This should not decrypt correctly with wrong key";
    std::vector<uint8_t> data = stringToBytes(testMessage);

    // Create two different keys
    std::vector<uint8_t> correctKey = createRandomKey();
    std::vector<uint8_t> wrongKey = createRandomKey();

    // Make sure the keys are different
    ASSERT_NE(correctKey, wrongKey);

    // Encrypt with the correct key
    std::vector<uint8_t> encrypted = crypto.encrypt(data, correctKey);

    // Attempt to decrypt with the wrong key
    std::vector<uint8_t> decrypted = crypto.decrypt(encrypted, wrongKey);

    // The decryption should fail (return empty vector) or the result should be different
    // from the original data
    EXPECT_TRUE(decrypted.empty() || decrypted != data);
}

// Test tampering detection
TEST_F(CryptoTest, TamperingDetection)
{
    std::string testMessage = "This message should be protected against tampering";
    std::vector<uint8_t> data = stringToBytes(testMessage);
    std::vector<uint8_t> key = createRandomKey();

    // Encrypt the data
    std::vector<uint8_t> encrypted = crypto.encrypt(data, key);
    ASSERT_FALSE(encrypted.empty());

    // Tamper with the encrypted data (modify a byte in the middle)
    if (encrypted.size() > 20)
    {
        encrypted[encrypted.size() / 2] ^= 0xFF; // Flip all bits in one byte

        // Decryption should now fail or produce incorrect results
        std::vector<uint8_t> decrypted = crypto.decrypt(encrypted, key);
        EXPECT_TRUE(decrypted.empty() || decrypted != data);
    }
}

// Test binary data encryption and decryption
TEST_F(CryptoTest, BinaryData)
{
    // Create binary data with all possible byte values
    std::vector<uint8_t> binaryData(256);
    for (int i = 0; i < 256; ++i)
    {
        binaryData[i] = static_cast<uint8_t>(i);
    }

    std::vector<uint8_t> key = createRandomKey();

    // Encrypt the binary data
    std::vector<uint8_t> encrypted = crypto.encrypt(binaryData, key);
    EXPECT_FALSE(encrypted.empty());

    // Decrypt the data
    std::vector<uint8_t> decrypted = crypto.decrypt(encrypted, key);

    // The decrypted data should match the original
    EXPECT_EQ(binaryData, decrypted);
}

// Test large data encryption and decryption
TEST_F(CryptoTest, LargeData)
{
    // Create a large data set (1MB)
    const size_t size = 1024 * 1024;
    std::vector<uint8_t> largeData(size);
    for (size_t i = 0; i < size; ++i)
    {
        largeData[i] = static_cast<uint8_t>(i % 256);
    }

    std::vector<uint8_t> key = createRandomKey();

    // Encrypt the large data
    std::vector<uint8_t> encrypted = crypto.encrypt(largeData, key);
    EXPECT_FALSE(encrypted.empty());

    // Decrypt the data
    std::vector<uint8_t> decrypted = crypto.decrypt(encrypted, key);

    // The decrypted data should match the original
    EXPECT_EQ(largeData, decrypted);
}

// Test encryption and decryption with a fixed key (for reproducibility)
TEST_F(CryptoTest, FixedKey)
{
    std::string testMessage = "Testing with fixed key";
    std::vector<uint8_t> data = stringToBytes(testMessage);

    // Create a fixed key
    std::vector<uint8_t> fixedKey(32, 0x42); // Fill with the value 0x42

    // Encrypt with the fixed key
    std::vector<uint8_t> encrypted1 = crypto.encrypt(data, fixedKey);
    EXPECT_FALSE(encrypted1.empty());

    // Decrypt with the same key
    std::vector<uint8_t> decrypted = crypto.decrypt(encrypted1, fixedKey);
    EXPECT_EQ(data, decrypted);

    // The same data encrypted with the same key should produce different ciphertexts
    // due to random IV (each encryption should be unique)
    std::vector<uint8_t> encrypted2 = crypto.encrypt(data, fixedKey);
    EXPECT_NE(encrypted1, encrypted2);

    // But decryption should still work
    decrypted = crypto.decrypt(encrypted2, fixedKey);
    EXPECT_EQ(data, decrypted);
}

// Main function that runs all the tests
int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}