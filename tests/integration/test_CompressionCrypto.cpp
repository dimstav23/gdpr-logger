#include <gtest/gtest.h>
#include "Compression.hpp"
#include "Crypto.hpp"
#include "LogEntry.hpp"
#include <vector>
#include <string>
#include <memory>

class CompressionCryptoTest : public ::testing::Test
{
protected:
    Crypto crypto;

    void SetUp() override
    {
        // Create sample log entries for testing
        entry1 = LogEntry(LogEntry::ActionType::CREATE, "/data/records/1", "user123", "subject456");
        entry2 = LogEntry(LogEntry::ActionType::READ, "/data/records/2", "admin789", "subject456");
        entry3 = LogEntry(LogEntry::ActionType::UPDATE, "/data/records/3", "user123", "subject789");

        // Create encryption key
        key = std::vector<uint8_t>(32, 0x42);      // Fixed key for reproducibility
        wrongKey = std::vector<uint8_t>(32, 0x24); // Different key for testing
    }

    // Helper function to compare two LogEntry objects
    bool LogEntriesEqual(const LogEntry &a, const LogEntry &b)
    {
        return a.serialize() == b.serialize();
    }

    LogEntry entry1, entry2, entry3;
    std::vector<uint8_t> key;
    std::vector<uint8_t> wrongKey;
};

// Test 1: Single entry full cycle - original -> compress -> encrypt -> decrypt -> decompress -> recovered
TEST_F(CompressionCryptoTest, SingleEntryFullCycle)
{
    std::vector<uint8_t> compressed = Compression::compressEntry(entry1);
    ASSERT_GT(compressed.size(), 0);

    std::vector<uint8_t> encrypted = crypto.encrypt(compressed, key);
    ASSERT_GT(encrypted.size(), 0);
    EXPECT_NE(encrypted, compressed);

    // Verify wrong key produces invalid or no results
    std::vector<uint8_t> wrongDecrypted = crypto.decrypt(encrypted, wrongKey);
    if (!wrongDecrypted.empty())
    {
        EXPECT_NE(wrongDecrypted, compressed);
    }

    std::vector<uint8_t> decrypted = crypto.decrypt(encrypted, key);
    ASSERT_GT(decrypted.size(), 0);
    EXPECT_EQ(decrypted, compressed);

    std::unique_ptr<LogEntry> recovered = Compression::decompressEntry(decrypted);

    ASSERT_TRUE(recovered != nullptr);
    EXPECT_TRUE(LogEntriesEqual(entry1, *recovered));
    EXPECT_EQ(entry1.getActionType(), recovered->getActionType());
    EXPECT_EQ(entry1.getDataLocation(), recovered->getDataLocation());
    EXPECT_EQ(entry1.getUserId(), recovered->getUserId());
    EXPECT_EQ(entry1.getDataSubjectId(), recovered->getDataSubjectId());
}

// Test 2: Batch processing - riginal -> compress -> encrypt -> decrypt -> decompress -> recovered
TEST_F(CompressionCryptoTest, BatchProcessing)
{
    std::vector<LogEntry> batch = {entry1, entry2, entry3};
    std::vector<uint8_t> compressed = Compression::compressBatch(batch);
    ASSERT_GT(compressed.size(), 0);

    std::vector<uint8_t> encrypted = crypto.encrypt(compressed, key);
    ASSERT_GT(encrypted.size(), 0);
    EXPECT_NE(encrypted, compressed);

    std::vector<uint8_t> decrypted = crypto.decrypt(encrypted, key);
    ASSERT_GT(decrypted.size(), 0);
    EXPECT_EQ(decrypted, compressed);

    std::vector<LogEntry> recovered = Compression::decompressBatch(decrypted);
    ASSERT_EQ(batch.size(), recovered.size());

    for (size_t i = 0; i < batch.size(); i++)
    {
        EXPECT_TRUE(LogEntriesEqual(batch[i], recovered[i]))
            << "Entries at index " << i << " don't match";
    }

    // Test with empty batch
    std::vector<LogEntry> emptyBatch;
    std::vector<uint8_t> emptyCompressed = Compression::compressBatch(emptyBatch);
    std::vector<uint8_t> emptyEncrypted = crypto.encrypt(emptyCompressed, key);
    std::vector<uint8_t> emptyDecrypted = crypto.decrypt(emptyEncrypted, key);
    std::vector<LogEntry> emptyRecovered = Compression::decompressBatch(emptyDecrypted);
    EXPECT_TRUE(emptyRecovered.empty());

    // Test with single entry batch
    std::vector<LogEntry> singleBatch = {entry1};
    std::vector<uint8_t> singleCompressed = Compression::compressBatch(singleBatch);
    std::vector<uint8_t> singleEncrypted = crypto.encrypt(singleCompressed, key);
    std::vector<uint8_t> singleDecrypted = crypto.decrypt(singleEncrypted, key);
    std::vector<LogEntry> singleRecovered = Compression::decompressBatch(singleDecrypted);
    ASSERT_EQ(1, singleRecovered.size());
    EXPECT_TRUE(LogEntriesEqual(entry1, singleRecovered[0]));
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}