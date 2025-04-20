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
        // Create a test directory
        m_testDir = std::filesystem::temp_directory_path() / "segmented_storage_test";

        // Clean up any previous test data
        if (std::filesystem::exists(m_testDir))
        {
            std::filesystem::remove_all(m_testDir);
        }

        std::filesystem::create_directories(m_testDir);

        // Create small segments for testing
        m_maxSegmentSize = 1024; // 1KB segments for faster testing
        m_bufferSize = 128;      // Small buffer for testing
    }

    void TearDown() override
    {
        // Clean up test directory
        if (std::filesystem::exists(m_testDir))
        {
            std::filesystem::remove_all(m_testDir);
        }
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

// Test basic initialization
TEST_F(SegmentedStorageTest, Initialization)
{
    SegmentedStorage storage(m_testDir.string(), "test_log", m_maxSegmentSize, m_bufferSize);

    // Verify initial state
    EXPECT_EQ(storage.getCurrentSegmentIndex(), 0);
    EXPECT_EQ(storage.getCurrentSegmentSize(), 0);
    EXPECT_TRUE(std::filesystem::exists(storage.getCurrentSegmentPath()));
}

// Test basic writing
TEST_F(SegmentedStorageTest, BasicWriting)
{
    SegmentedStorage storage(m_testDir.string(), "test_log", m_maxSegmentSize, m_bufferSize);

    std::vector<uint8_t> testData = generateRandomData(256);
    size_t bytesWritten = storage.write(testData);
    EXPECT_EQ(bytesWritten, testData.size());
    EXPECT_EQ(storage.getCurrentSegmentSize(), testData.size());

    // Flush to make sure it's written to disk
    storage.flush();

    // Read back data for verification
    std::vector<uint8_t> fileContent = readFileContent(storage.getCurrentSegmentPath());
    EXPECT_EQ(fileContent.size(), testData.size());
    EXPECT_TRUE(std::equal(testData.begin(), testData.end(), fileContent.begin()));
}

// Test segment rotation
TEST_F(SegmentedStorageTest, SegmentRotation)
{
    SegmentedStorage storage(m_testDir.string(), "test_log", m_maxSegmentSize, m_bufferSize);

    // Write enough data to force rotation (write slightly more than max segment size)
    std::vector<uint8_t> testData = generateRandomData(m_maxSegmentSize / 2);

    // First write - should be in segment 0
    storage.write(testData);
    std::string firstSegmentPath = storage.getCurrentSegmentPath();

    // Second write - should still be in segment 0
    storage.write(testData);

    // Third write - should cause rotation to segment 1
    storage.write(testData);

    // Verify segment rotation
    EXPECT_EQ(storage.getCurrentSegmentIndex(), 1);
    EXPECT_NE(storage.getCurrentSegmentPath(), firstSegmentPath);

    // Verify both segment files exist
    EXPECT_TRUE(std::filesystem::exists(firstSegmentPath));
    EXPECT_TRUE(std::filesystem::exists(storage.getCurrentSegmentPath()));

    // Verify content sizes in both files
    std::vector<uint8_t> firstFileContent = readFileContent(firstSegmentPath);
    std::vector<uint8_t> secondFileContent = readFileContent(storage.getCurrentSegmentPath());

    // First file should be approximately full
    EXPECT_GE(firstFileContent.size(), m_maxSegmentSize);

    // Second file should have the remainder of the data
    EXPECT_EQ(secondFileContent.size(), testData.size());
}

// Test manual segment rotation
TEST_F(SegmentedStorageTest, ManualRotation)
{
    SegmentedStorage storage(m_testDir.string(), "test_log", m_maxSegmentSize, m_bufferSize);

    // Write some data
    std::vector<uint8_t> testData = generateRandomData(100);
    storage.write(testData);

    // Get current state
    size_t initialSegmentIndex = storage.getCurrentSegmentIndex();
    std::string initialSegmentPath = storage.getCurrentSegmentPath();

    // Manually rotate segment
    std::string newSegmentPath = storage.rotateSegment();

    // Verify rotation
    EXPECT_EQ(storage.getCurrentSegmentIndex(), initialSegmentIndex + 1);
    EXPECT_NE(newSegmentPath, initialSegmentPath);
    EXPECT_TRUE(std::filesystem::exists(newSegmentPath));

    // Write to new segment
    storage.write(testData);
    storage.flush(); // Ensure data is written to disk

    // Verify content in both segments
    std::vector<uint8_t> firstFileContent = readFileContent(initialSegmentPath);
    std::vector<uint8_t> secondFileContent = readFileContent(newSegmentPath);
    EXPECT_EQ(firstFileContent.size(), testData.size());
    EXPECT_EQ(secondFileContent.size(), testData.size());
}

// Test concurrent writing from multiple threads
TEST_F(SegmentedStorageTest, ConcurrentWriting)
{
    SegmentedStorage storage(m_testDir.string(), "test_log", m_maxSegmentSize * 10, m_bufferSize);

    // Number of threads and writes per thread
    const int numThreads = 8;
    const int writesPerThread = 50;
    const size_t writeSize = 128; // bytes per write

    // Data to track what each thread wrote
    std::vector<std::vector<std::vector<uint8_t>>> threadData(numThreads);

    // Launch multiple threads that write concurrently
    std::vector<std::thread> threads;

    for (int i = 0; i < numThreads; i++)
    {
        threads.emplace_back([i, writesPerThread, writeSize, &storage, &threadData]()
                             {
            threadData[i].reserve(writesPerThread);

            for (int j = 0; j < writesPerThread; j++)
            {
                // Generate data with a pattern to identify which thread wrote it
                std::vector<uint8_t> data(writeSize);
                // Fill with thread ID so we can identify which thread wrote what
                std::fill(data.begin(), data.end(), static_cast<uint8_t>(i));
                // Add write number at the start to differentiate between writes
                data[0] = static_cast<uint8_t>(j);

                // Store what we're writing
                threadData[i].push_back(data);

                // Write to storage
                size_t bytesWritten = storage.write(data);
                EXPECT_EQ(bytesWritten, writeSize);

                // Small delay to increase chance of interleaving
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } });
    }

    // Wait for all threads to complete
    for (auto &thread : threads)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }

    // Flush to ensure all data is written
    storage.flush();

    // Calculate expected total size
    size_t expectedTotalSize = numThreads * writesPerThread * writeSize;

    // Verify total data written across all segments
    size_t totalBytesWritten = 0;
    for (const auto &entry : std::filesystem::directory_iterator(m_testDir))
    {
        if (entry.is_regular_file())
        {
            totalBytesWritten += std::filesystem::file_size(entry.path());
        }
    }

    EXPECT_EQ(totalBytesWritten, expectedTotalSize);

    // Verify the content of the current segment (or all segments if needed)
    // This is a simplified verification - in a real test you might want to read all segments
    // and verify that all the data from all threads was written correctly
    if (totalBytesWritten == expectedTotalSize)
    {
        std::cout << "Successfully wrote " << totalBytesWritten << " bytes from "
                  << numThreads << " concurrent threads" << std::endl;
    }
}

// Test proper cleanup on destruction
TEST_F(SegmentedStorageTest, DestructorCleanup)
{
    std::string segmentPath;

    {
        // Create storage in a scope that will end
        SegmentedStorage storage(m_testDir.string(), "test_log", m_maxSegmentSize, m_bufferSize);

        // Write some data
        std::vector<uint8_t> testData = generateRandomData(100);
        storage.write(testData);

        // Store current segment path
        segmentPath = storage.getCurrentSegmentPath();

        // Let the destructor run
    }

    // Verify the file exists and is properly closed
    EXPECT_TRUE(std::filesystem::exists(segmentPath));
    EXPECT_EQ(std::filesystem::file_size(segmentPath), 100);

    // Try to open it for reading
    std::ifstream testRead(segmentPath, std::ios::binary);
    EXPECT_TRUE(testRead.is_open());

    // Should be able to read the full content
    std::vector<char> content(100);
    testRead.read(content.data(), 100);
    EXPECT_EQ(testRead.gcount(), 100);
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}