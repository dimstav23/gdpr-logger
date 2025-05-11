#include <gtest/gtest.h>
#include "LogEntry.hpp"
#include <vector>
#include <iostream>
#include <chrono>

// Test default constructor
TEST(LogEntryTest1, DefaultConstructor_InitializesCorrectly)
{
    LogEntry entry;

    EXPECT_EQ(entry.getActionType(), LogEntry::ActionType::CREATE);
    EXPECT_EQ(entry.getDataLocation(), "");
    EXPECT_EQ(entry.getUserId(), "");
    EXPECT_EQ(entry.getDataSubjectId(), "");

    auto now = std::chrono::system_clock::now();
    EXPECT_NEAR(std::chrono::system_clock::to_time_t(entry.getTimestamp()),
                std::chrono::system_clock::to_time_t(now), 1);
}

// Test parameterized constructor
TEST(LogEntryTest2, ParameterizedConstructor_SetsFieldsCorrectly)
{
    LogEntry entry(LogEntry::ActionType::UPDATE, "database/users", "user123", "subject456");

    EXPECT_EQ(entry.getActionType(), LogEntry::ActionType::UPDATE);
    EXPECT_EQ(entry.getDataLocation(), "database/users");
    EXPECT_EQ(entry.getUserId(), "user123");
    EXPECT_EQ(entry.getDataSubjectId(), "subject456");

    auto now = std::chrono::system_clock::now();
    EXPECT_NEAR(std::chrono::system_clock::to_time_t(entry.getTimestamp()),
                std::chrono::system_clock::to_time_t(now), 1);
}

// Test serialization and deserialization
TEST(LogEntryTest4, SerializationDeserialization_WorksCorrectly)
{
    LogEntry entry(LogEntry::ActionType::READ, "storage/files", "userABC", "subjectXYZ");

    std::vector<uint8_t> serializedData = entry.serialize();
    LogEntry newEntry;
    bool success = newEntry.deserialize(serializedData);

    EXPECT_TRUE(success);
    EXPECT_EQ(newEntry.getActionType(), LogEntry::ActionType::READ);
    EXPECT_EQ(newEntry.getDataLocation(), "storage/files");
    EXPECT_EQ(newEntry.getUserId(), "userABC");
    EXPECT_EQ(newEntry.getDataSubjectId(), "subjectXYZ");

    std::vector<uint8_t> serializedData2 = entry.serialize();
    success = newEntry.deserialize(serializedData2);

    EXPECT_TRUE(success);

    EXPECT_EQ(newEntry.getActionType(), LogEntry::ActionType::READ);
    EXPECT_EQ(newEntry.getDataLocation(), "storage/files");
    EXPECT_EQ(newEntry.getUserId(), "userABC");
    EXPECT_EQ(newEntry.getDataSubjectId(), "subjectXYZ");
    EXPECT_NEAR(std::chrono::system_clock::to_time_t(newEntry.getTimestamp()),
                std::chrono::system_clock::to_time_t(entry.getTimestamp()), 1);
}

// Test batch serialization and deserialization
TEST(LogEntryTest5, BatchSerializationDeserialization_WorksCorrectly)
{
    // Create a batch of log entries
    std::vector<LogEntry> originalEntries;

    // Add various entries with different action types and data
    originalEntries.push_back(LogEntry(LogEntry::ActionType::CREATE, "db/users", "admin1", "user1"));
    originalEntries.push_back(LogEntry(LogEntry::ActionType::READ, "files/documents", "user2", "doc1"));
    originalEntries.push_back(LogEntry(LogEntry::ActionType::UPDATE, "cache/profiles", "editor1", "profile5"));
    originalEntries.push_back(LogEntry(LogEntry::ActionType::DELETE, "archive/logs", "admin2", "log10"));

    // Serialize the batch
    std::vector<uint8_t> batchData = LogEntry::serializeBatch(originalEntries);

    // Check that the batch has reasonable size
    EXPECT_GT(batchData.size(), sizeof(uint32_t)); // At least space for entry count

    // Deserialize the batch
    std::vector<LogEntry> recoveredEntries = LogEntry::deserializeBatch(batchData);

    // Verify the number of entries
    EXPECT_EQ(recoveredEntries.size(), originalEntries.size());

    // Verify each entry's data
    for (size_t i = 0; i < originalEntries.size() && i < recoveredEntries.size(); ++i)
    {
        EXPECT_EQ(recoveredEntries[i].getActionType(), originalEntries[i].getActionType());
        EXPECT_EQ(recoveredEntries[i].getDataLocation(), originalEntries[i].getDataLocation());
        EXPECT_EQ(recoveredEntries[i].getUserId(), originalEntries[i].getUserId());
        EXPECT_EQ(recoveredEntries[i].getDataSubjectId(), originalEntries[i].getDataSubjectId());

        // Compare timestamps (allowing 1 second difference for potential precision issues)
        EXPECT_NEAR(
            std::chrono::system_clock::to_time_t(recoveredEntries[i].getTimestamp()),
            std::chrono::system_clock::to_time_t(originalEntries[i].getTimestamp()),
            1);
    }
}