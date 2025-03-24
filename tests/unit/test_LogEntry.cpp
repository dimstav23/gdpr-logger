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
    EXPECT_EQ(entry.getSequenceNumber(), 0);

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

// Test setter methods
TEST(LogEntryTest3, Setters_UpdateFieldsCorrectly)
{
    LogEntry entry;

    entry.setActionType(LogEntry::ActionType::DELETE);
    entry.setDataLocation("server/logs");
    entry.setUserId("admin");
    entry.setDataSubjectId("subject789");
    entry.setSequenceNumber(100);

    EXPECT_EQ(entry.getActionType(), LogEntry::ActionType::DELETE);
    EXPECT_EQ(entry.getDataLocation(), "server/logs");
    EXPECT_EQ(entry.getUserId(), "admin");
    EXPECT_EQ(entry.getDataSubjectId(), "subject789");
    EXPECT_EQ(entry.getSequenceNumber(), 100);
}

// Test serialization and deserialization
TEST(LogEntryTest4, SerializationDeserialization_WorksCorrectly)
{
    LogEntry entry(LogEntry::ActionType::READ, "storage/files", "userABC", "subjectXYZ");
    entry.setSequenceNumber(42);

    std::vector<uint8_t> serializedData = entry.serialize();
    LogEntry newEntry;
    bool success = newEntry.deserialize(serializedData);

    EXPECT_TRUE(success);
    EXPECT_EQ(newEntry.getActionType(), LogEntry::ActionType::READ);
    EXPECT_EQ(newEntry.getDataLocation(), "storage/files");
    EXPECT_EQ(newEntry.getUserId(), "userABC");
    EXPECT_EQ(newEntry.getDataSubjectId(), "subjectXYZ");
    EXPECT_EQ(newEntry.getSequenceNumber(), 42);

    entry.setSequenceNumber(42);
    std::vector<uint8_t> prevHash = {0x12, 0x34, 0x56, 0x78};
    entry.setPreviousHash(prevHash);

    std::vector<uint8_t> serializedData2 = entry.serialize();
    success = newEntry.deserialize(serializedData2);

    EXPECT_TRUE(success);

    EXPECT_EQ(newEntry.getActionType(), LogEntry::ActionType::READ);
    EXPECT_EQ(newEntry.getDataLocation(), "storage/files");
    EXPECT_EQ(newEntry.getUserId(), "userABC");
    EXPECT_EQ(newEntry.getDataSubjectId(), "subjectXYZ");
    EXPECT_EQ(newEntry.getSequenceNumber(), 42);
    EXPECT_EQ(newEntry.getPreviousHash(), prevHash);
    EXPECT_NEAR(std::chrono::system_clock::to_time_t(newEntry.getTimestamp()),
                std::chrono::system_clock::to_time_t(entry.getTimestamp()), 1);
}

// Test hash calculation
TEST(LogEntryTest5, HashCalculation_ConsistentAndUnique)
{
    LogEntry entry(LogEntry::ActionType::UPDATE, "secure/db", "user567", "subject987");
    std::vector<uint8_t> hash = entry.calculateHash();
    EXPECT_FALSE(hash.empty());

    LogEntry entry2(LogEntry::ActionType::UPDATE, "secure/db", "user567", "subject987");
    EXPECT_EQ(entry.calculateHash(), entry2.calculateHash());

    LogEntry entry3(LogEntry::ActionType::DELETE, "secure/db", "user567", "subject987");
    EXPECT_NE(entry.calculateHash(), entry3.calculateHash());
}

// Test previous hash functionality
TEST(LogEntryTest6, PreviousHash_SetAndRetrieve)
{
    LogEntry entry;
    std::vector<uint8_t> prevHash = {0x12, 0x34, 0x56, 0x78};

    entry.setPreviousHash(prevHash);
    EXPECT_EQ(entry.getPreviousHash(), prevHash);

    std::vector<uint8_t> emptyHash;
    entry.setPreviousHash(emptyHash);
    EXPECT_EQ(entry.getPreviousHash().size(), 0);
}

// Test action type conversion functions
TEST(LogEntryTest7, ActionTypeConversion_ValidAndInvalidCases)
{
    EXPECT_EQ(actionTypeToString(LogEntry::ActionType::CREATE), "CREATE");
    EXPECT_EQ(actionTypeToString(LogEntry::ActionType::READ), "READ");
    EXPECT_EQ(actionTypeToString(LogEntry::ActionType::UPDATE), "UPDATE");
    EXPECT_EQ(actionTypeToString(LogEntry::ActionType::DELETE), "DELETE");

    EXPECT_THROW(actionTypeToString(static_cast<LogEntry::ActionType>(99)), std::invalid_argument);
}