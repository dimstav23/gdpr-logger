#include <gtest/gtest.h>
#include "Compression.hpp"
#include "LogEntry.hpp"
#include <vector>
#include <string>
#include <algorithm>

class CompressionTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Create a few sample log entries for testing
        entry1 = LogEntry(LogEntry::ActionType::CREATE, "/data/records/1", "user123", "subject456");
        entry2 = LogEntry(LogEntry::ActionType::READ, "/data/records/2", "admin789", "subject456");
        entry3 = LogEntry(LogEntry::ActionType::UPDATE, "/data/records/3", "user123", "subject789");
        entry4 = LogEntry(LogEntry::ActionType::DELETE, "/data/records/4", "admin789", "subject123");
    }

    LogEntry entry1, entry2, entry3, entry4;
};

// Helper function to compare two LogEntry objects
bool LogEntriesEqual(const LogEntry &a, const LogEntry &b)
{
    // Compare serialized representations to check equality
    auto serializedA = a.serialize();
    auto serializedB = b.serialize();

    return serializedA == serializedB;
}

// Test compressing and decompressing a single log entry
TEST_F(CompressionTest, CompressDecompressSingleEntry)
{
    // Compress a single entry
    std::vector<uint8_t> compressed = Compression::compressEntry(entry1);

    // Make sure compression actually reduced the size (or at least did something)
    ASSERT_GT(compressed.size(), 0);

    // Decompress the entry
    std::unique_ptr<LogEntry> decompressed = Compression::decompressEntry(compressed);

    // Verify the decompressed entry matches the original
    ASSERT_TRUE(decompressed != nullptr);
    EXPECT_TRUE(LogEntriesEqual(entry1, *decompressed));
}

// Test compressing and decompressing a batch of log entries
TEST_F(CompressionTest, CompressDecompressBatch)
{
    // Create a batch of entries
    std::vector<LogEntry> batch = {entry1, entry2, entry3, entry4};

    // Compress the batch
    std::vector<uint8_t> compressed = Compression::compressBatch(batch);

    // Make sure compression produced data
    ASSERT_GT(compressed.size(), 0);

    // Decompress the batch
    std::vector<LogEntry> decompressed = Compression::decompressBatch(compressed);

    // Verify we got back the same number of entries
    ASSERT_EQ(batch.size(), decompressed.size());

    // Verify each entry matches
    for (size_t i = 0; i < batch.size(); i++)
    {
        EXPECT_TRUE(LogEntriesEqual(batch[i], decompressed[i]))
            << "Entries at index " << i << " don't match";
    }
}

// Test with an empty batch
TEST_F(CompressionTest, EmptyBatch)
{
    // Create an empty batch
    std::vector<LogEntry> emptyBatch;

    // Compress the empty batch
    std::vector<uint8_t> compressed = Compression::compressBatch(emptyBatch);

    // Decompress it back
    std::vector<LogEntry> decompressed = Compression::decompressBatch(compressed);

    // Verify we still have an empty vector
    EXPECT_TRUE(decompressed.empty());
}

// Test with extremely large entries
TEST_F(CompressionTest, LargeEntry)
{
    // Create a log entry with large fields
    std::string largeDataLocation(10000, 'A'); // 10KB string of 'A's
    std::string largeUserId(5000, 'B');        // 5KB string of 'B's
    std::string largeSubjectId(5000, 'C');     // 5KB string of 'C's

    LogEntry largeEntry(LogEntry::ActionType::CREATE, largeDataLocation, largeUserId, largeSubjectId);

    // Compress and decompress
    std::vector<uint8_t> compressed = Compression::compressEntry(largeEntry);
    std::unique_ptr<LogEntry> decompressed = Compression::decompressEntry(compressed);

    // Verify compression worked (should be much smaller)
    EXPECT_LT(compressed.size(), largeDataLocation.size() + largeUserId.size() + largeSubjectId.size());

    // Verify the entry was correctly reconstructed
    ASSERT_TRUE(decompressed != nullptr);
    EXPECT_TRUE(LogEntriesEqual(largeEntry, *decompressed));
}

// Test with invalid compressed data
TEST_F(CompressionTest, InvalidCompressedData)
{
    // Create some invalid compressed data
    std::vector<uint8_t> invalidData = {0x01, 0x02, 0x03, 0x04};

    // Try to decompress it
    std::unique_ptr<LogEntry> decompressedEntry = Compression::decompressEntry(invalidData);
    std::vector<LogEntry> decompressedBatch = Compression::decompressBatch(invalidData);

    // Verify that decompression failed
    EXPECT_EQ(nullptr, decompressedEntry);
    EXPECT_TRUE(decompressedBatch.empty());
}

// Test compression ratio
TEST_F(CompressionTest, CompressionRatio)
{
    // Create log entries with repetitive data which should compress well
    std::string repetitiveData(1000, 'X');
    LogEntry repetitiveEntry(LogEntry::ActionType::CREATE, repetitiveData, repetitiveData, repetitiveData);

    // Compress the entry
    std::vector<uint8_t> serialized = repetitiveEntry.serialize();
    std::vector<uint8_t> compressed = Compression::compressEntry(repetitiveEntry);

    // Check that compression significantly reduced the size
    double compressionRatio = static_cast<double>(compressed.size()) / static_cast<double>(serialized.size());
    EXPECT_LT(compressionRatio, 0.1); // Expect at least 90% compression

    // Decompress to verify integrity
    std::unique_ptr<LogEntry> decompressed = Compression::decompressEntry(compressed);
    ASSERT_TRUE(decompressed != nullptr);
    EXPECT_TRUE(LogEntriesEqual(repetitiveEntry, *decompressed));
}

// Test with a large batch of entries
TEST_F(CompressionTest, LargeBatch)
{
    // Create a large batch of 100 identical entries
    std::vector<LogEntry> largeBatch(100, entry1);

    // Compress the batch
    std::vector<uint8_t> compressed = Compression::compressBatch(largeBatch);

    // Decompress the batch
    std::vector<LogEntry> decompressed = Compression::decompressBatch(compressed);

    // Verify the correct number of entries
    ASSERT_EQ(largeBatch.size(), decompressed.size());

    // Verify the entries match
    for (size_t i = 0; i < largeBatch.size(); i++)
    {
        EXPECT_TRUE(LogEntriesEqual(largeBatch[i], decompressed[i]));
    }
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}