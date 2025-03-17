#include <gtest/gtest.h>
#include "LogEntry.hpp"
#include <vector>
#include <iostream>
#include <chrono>

// Test default constructor
TEST(LogEntryTest1, DefaultConstructor)
{
    LogEntry entry;

    EXPECT_EQ(entry.getActionType(), LogEntry::ActionType::CREATE);
    EXPECT_EQ(entry.getDataLocation(), "");
    EXPECT_EQ(entry.getUserId(), "");
    EXPECT_EQ(entry.getDataSubjectId(), "");
    EXPECT_EQ(entry.getSequenceNumber(), 0);
}

// Test parameterized constructor
TEST(LogEntryTest2, ParameterizedConstructor)
{
    LogEntry entry(LogEntry::ActionType::UPDATE, "database/users", "user123", "subject456");

    EXPECT_EQ(entry.getActionType(), LogEntry::ActionType::UPDATE);
    EXPECT_EQ(entry.getDataLocation(), "database/users");
    EXPECT_EQ(entry.getUserId(), "user123");
    EXPECT_EQ(entry.getDataSubjectId(), "subject456");
}

// Test setter methods
TEST(LogEntryTest3, Setters)
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
TEST(LogEntryTest4, SerializationDeserialization)
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
}

// Test hash calculation
TEST(LogEntryTest5, HashCalculation)
{
    LogEntry entry(LogEntry::ActionType::UPDATE, "secure/db", "user567", "subject987");
    std::vector<uint8_t> hash = entry.calculateHash();

    EXPECT_FALSE(hash.empty());
}

// Test previous hash functionality
TEST(LogEntryTest6, PreviousHash)
{
    LogEntry entry;
    std::vector<uint8_t> prevHash = {0x12, 0x34, 0x56, 0x78};

    entry.setPreviousHash(prevHash);
    EXPECT_EQ(entry.getPreviousHash(), prevHash);
}

// Test action type conversion functions
TEST(LogEntryTest7, ActionTypeConversion)
{
    EXPECT_EQ(actionTypeToString(LogEntry::ActionType::CREATE), "CREATE");
    EXPECT_EQ(actionTypeToString(LogEntry::ActionType::READ), "READ");
    EXPECT_EQ(actionTypeToString(LogEntry::ActionType::UPDATE), "UPDATE");
    EXPECT_EQ(actionTypeToString(LogEntry::ActionType::DELETE), "DELETE");
}