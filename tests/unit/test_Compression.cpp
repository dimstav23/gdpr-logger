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

// Helper function to create serialized batch from log entries
std::vector<std::vector<uint8_t>> serializeBatch(const std::vector<LogEntry> &entries)
{
    std::vector<std::vector<uint8_t>> serializedEntries;
    for (const auto &entry : entries)
    {
        serializedEntries.push_back(entry.serialize());
    }
    return serializedEntries;
}

// Test compressing and decompressing a batch of log entries
TEST_F(CompressionTest, CompressDecompressBatch)
{
    // Create a batch of entries
    std::vector<LogEntry> batch = {entry1, entry2, entry3, entry4};

    // Serialize the batch first
    std::vector<std::vector<uint8_t>> serializedBatch = serializeBatch(batch);

    // Compress the serialized batch
    std::vector<uint8_t> compressed = Compression::compressBatch(serializedBatch);

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

    // Serialize the empty batch
    std::vector<std::vector<uint8_t>> serializedBatch = serializeBatch(emptyBatch);

    // Compress the empty serialized batch
    std::vector<uint8_t> compressed = Compression::compressBatch(serializedBatch);

    // Decompress it back
    std::vector<LogEntry> decompressed = Compression::decompressBatch(compressed);

    // Verify we still have an empty vector
    EXPECT_TRUE(decompressed.empty());
}

// Test with invalid compressed data
TEST_F(CompressionTest, InvalidCompressedData)
{
    // Create some invalid compressed data
    std::vector<uint8_t> invalidData = {0x01, 0x02, 0x03, 0x04};

    // Try to decompress it
    std::vector<LogEntry> decompressedBatch = Compression::decompressBatch(invalidData);

    // Verify that decompression failed
    EXPECT_TRUE(decompressedBatch.empty());
}

// Test batch compression ratio
TEST_F(CompressionTest, BatchCompressionRatio)
{
    // Create a batch of log entries with repetitive data which should compress well
    const int batchSize = 50;
    std::string repetitiveData(1000, 'X');
    LogEntry repetitiveEntry(LogEntry::ActionType::CREATE, repetitiveData, repetitiveData, repetitiveData);

    std::vector<LogEntry> repetitiveBatch(batchSize, repetitiveEntry);

    // Calculate the total size of serialized entries
    std::vector<uint8_t> totalSerialized;
    for (const auto &entry : repetitiveBatch)
    {
        auto serialized = entry.serialize();
        totalSerialized.insert(totalSerialized.end(), serialized.begin(), serialized.end());
    }

    // Serialize the batch for compression
    std::vector<std::vector<uint8_t>> serializedBatch = serializeBatch(repetitiveBatch);

    // Compress the serialized batch
    std::vector<uint8_t> compressed = Compression::compressBatch(serializedBatch);

    // Check that batch compression significantly reduced the size
    double compressionRatio = static_cast<double>(compressed.size()) / static_cast<double>(totalSerialized.size());
    EXPECT_LT(compressionRatio, 0.05); // Expect at least 95% compression for batch

    // Decompress to verify integrity
    std::vector<LogEntry> decompressed = Compression::decompressBatch(compressed);

    // Verify the correct number of entries and their content
    ASSERT_EQ(repetitiveBatch.size(), decompressed.size());
    for (size_t i = 0; i < repetitiveBatch.size(); i++)
    {
        EXPECT_TRUE(LogEntriesEqual(repetitiveBatch[i], decompressed[i]));
    }
}

// Test with a large batch of entries
TEST_F(CompressionTest, LargeBatch)
{
    // Create a large batch of 100 identical entries
    std::vector<LogEntry> largeBatch(100, entry1);

    // Serialize the batch
    std::vector<std::vector<uint8_t>> serializedBatch = serializeBatch(largeBatch);

    // Compress the serialized batch
    std::vector<uint8_t> compressed = Compression::compressBatch(serializedBatch);

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

// Additional test: Compress batches of already serialized data directly
TEST_F(CompressionTest, DirectSerializedCompression)
{
    // Create some serialized data directly
    std::vector<std::vector<uint8_t>> serializedEntries;

    // Add a few serialized entries
    for (int i = 0; i < 5; i++)
    {
        std::vector<uint8_t> data(50, static_cast<uint8_t>(i)); // Fill with values based on index
        serializedEntries.push_back(data);
    }

    // Compress the serialized data
    std::vector<uint8_t> compressed = Compression::compressBatch(serializedEntries);

    // Make sure compression produced data
    ASSERT_GT(compressed.size(), 0);

    // Since we don't have valid LogEntry data, this will fail to deserialize completely,
    // but we can still test the compression works with arbitrary byte vectors
    // For actual testing, use real serialized LogEntry data
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}