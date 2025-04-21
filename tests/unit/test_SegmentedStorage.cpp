#include <gtest/gtest.h>
#include "SegmentedStorage.hpp"
#include <filesystem>
#include <fstream>
#include <thread>
#include <vector>
#include <random>
#include <algorithm>

class SegmentedStorageTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        m_testDir = "segmented_storage_test";

        // Clean up any previous test data
        if (std::filesystem::exists(m_testDir))
        {
            std::filesystem::remove_all(m_testDir);
        }

        // Create small segments for testing
        m_maxSegmentSize = 1024; // 1KB segments for faster testing
        m_bufferSize = 128;      // Small buffer for testing
    }

    // Helper to read file content for verification
    std::vector<uint8_t> readFileContent(const std::string &path)
    {
        std::ifstream file(path, std::ios::binary);
        EXPECT_TRUE(file.is_open()) << "Failed to open file: " << path;

        // Get file size
        file.seekg(0, std::ios::end);
        size_t size = file.tellg();
        file.seekg(0, std::ios::beg);

        // Read content
        std::vector<uint8_t> content(size);
        if (size > 0)
        {
            file.read(reinterpret_cast<char *>(content.data()), size);
        }

        return content;
    }

    // Helper to create random test data
    std::vector<uint8_t> generateRandomData(size_t size)
    {
        std::vector<uint8_t> data(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 255);

        std::generate(data.begin(), data.end(), [&]()
                      { return dis(gen); });
        return data;
    }

    std::filesystem::path m_testDir;
    size_t m_maxSegmentSize;
    size_t m_bufferSize;
};

// Test basic writing
TEST_F(SegmentedStorageTest, BasicWriting)
{
    SegmentedStorage storage(m_testDir.string(), "test_log", m_maxSegmentSize, m_bufferSize);

    std::vector<uint8_t> testData = generateRandomData(256);
    size_t bytesWritten = storage.write(testData);
    EXPECT_EQ(bytesWritten, testData.size());
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}