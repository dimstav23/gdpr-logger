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

// Test writing to a client-specified file
TEST_F(SegmentedStorageTest, WriteToSpecificFile)
{
    SegmentedStorage storage(m_testDir.string(), "test_log", m_maxSegmentSize, m_bufferSize);

    const std::string customFilename = "custom_log";
    std::vector<uint8_t> testData = generateRandomData(256);

    // Write to the custom file
    size_t bytesWritten = storage.writeToFile(customFilename, testData);
    EXPECT_EQ(bytesWritten, testData.size());
    EXPECT_EQ(storage.getCurrentSegmentSize(customFilename), testData.size());

    // Ensure the custom file exists
    std::string customPath = storage.getCurrentSegmentPath(customFilename);
    EXPECT_TRUE(std::filesystem::exists(customPath));

    // Flush to make sure it's written to disk
    storage.flushFile(customFilename);

    // Read back data for verification
    std::vector<uint8_t> fileContent = readFileContent(customPath);
    EXPECT_EQ(fileContent.size(), testData.size());
    EXPECT_TRUE(std::equal(testData.begin(), testData.end(), fileContent.begin()));

    // Verify that default file is still empty and separate
    EXPECT_EQ(storage.getCurrentSegmentSize(), 0);
}

// Test rotating a client-specified file
TEST_F(SegmentedStorageTest, RotateSpecificFile)
{
    SegmentedStorage storage(m_testDir.string(), "test_log", m_maxSegmentSize, m_bufferSize);

    const std::string customFilename = "custom_log";
    std::vector<uint8_t> testData = generateRandomData(m_maxSegmentSize / 2);

    // First write to custom file
    storage.writeToFile(customFilename, testData);
    std::string firstSegmentPath = storage.getCurrentSegmentPath(customFilename);

    // Second write to custom file
    storage.writeToFile(customFilename, testData);

    // Third write should trigger rotation
    storage.writeToFile(customFilename, testData);

    // Verify segment rotation
    EXPECT_EQ(storage.getCurrentSegmentIndex(customFilename), 1);
    EXPECT_NE(storage.getCurrentSegmentPath(customFilename), firstSegmentPath);

    // Verify both segments exist
    EXPECT_TRUE(std::filesystem::exists(firstSegmentPath));
    EXPECT_TRUE(std::filesystem::exists(storage.getCurrentSegmentPath(customFilename)));

    // Read content from both files
    std::vector<uint8_t> firstContent = readFileContent(firstSegmentPath);
    std::vector<uint8_t> secondContent = readFileContent(storage.getCurrentSegmentPath(customFilename));

    // First file should be approximately maxSegmentSize
    EXPECT_GE(firstContent.size(), m_maxSegmentSize);

    // Second file should have the remainder
    EXPECT_EQ(secondContent.size(), testData.size());

    // Default file should remain untouched
    EXPECT_EQ(storage.getCurrentSegmentIndex(), 0);
    EXPECT_EQ(storage.getCurrentSegmentSize(), 0);
}

// Test concurrent writing to multiple files
TEST_F(SegmentedStorageTest, ConcurrentMultipleFiles)
{
    SegmentedStorage storage(m_testDir.string(), "test_log", m_maxSegmentSize * 10, m_bufferSize);

    // Number of files and threads
    const int numFiles = 4;
    const int numThreadsPerFile = 4;
    const int writesPerThread = 50;
    const size_t writeSize = 128;

    // Vector of file names
    std::vector<std::string> filenames;
    for (int i = 0; i < numFiles; i++)
    {
        filenames.push_back("custom_log_" + std::to_string(i));
    }

    // Launch threads for each file
    std::vector<std::thread> threads;

    for (int fileIdx = 0; fileIdx < numFiles; fileIdx++)
    {
        for (int threadIdx = 0; threadIdx < numThreadsPerFile; threadIdx++)
        {
            threads.emplace_back([fileIdx, threadIdx, &filenames, writesPerThread, writeSize, &storage]()
                                 {
                for (int i = 0; i < writesPerThread; i++) {
                    // Generate data
                    std::vector<uint8_t> data(writeSize);
                    std::fill(data.begin(), data.end(), static_cast<uint8_t>(threadIdx));
                    data[0] = static_cast<uint8_t>(i);

                    // Write to file
                    size_t bytesWritten = storage.writeToFile(filenames[fileIdx], data);
                    EXPECT_EQ(bytesWritten, writeSize);

                    // Small delay
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                } });
        }
    }

    // Wait for all threads
    for (auto &thread : threads)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }

    // Flush all files
    storage.flush();

    // Calculate expected size per file
    size_t expectedSizePerFile = numThreadsPerFile * writesPerThread * writeSize;

    // Verify sizes for each file (across all segments)
    for (const std::string &filename : filenames)
    {
        // Get all segments for this file
        size_t totalSize = 0;
        for (const auto &entry : std::filesystem::directory_iterator(m_testDir))
        {
            if (entry.is_regular_file() &&
                entry.path().filename().string().find(filename) != std::string::npos)
            {
                totalSize += std::filesystem::file_size(entry.path());
            }
        }

        EXPECT_EQ(totalSize, expectedSizePerFile)
            << "File " << filename << " has incorrect size";
    }

    // Default file should still be empty
    EXPECT_EQ(storage.getCurrentSegmentSize(), 0);
}

// Test error handling for non-existent files
TEST_F(SegmentedStorageTest, NonExistentFileHandling)
{
    SegmentedStorage storage(m_testDir.string(), "test_log", m_maxSegmentSize, m_bufferSize);

    // This should succeed because getOrCreateSegment will create the file
    storage.writeToFile("new_file", generateRandomData(10));

    // These should also succeed
    EXPECT_NO_THROW(storage.getCurrentSegmentIndex("new_file"));
    EXPECT_NO_THROW(storage.getCurrentSegmentSize("new_file"));
    EXPECT_NO_THROW(storage.getCurrentSegmentPath("new_file"));
    EXPECT_NO_THROW(storage.flushFile("new_file"));

    // Test a non-existent file - these should throw exceptions per your implementation
    EXPECT_THROW(storage.getCurrentSegmentIndex("nonexistent_file"), std::runtime_error);
    EXPECT_THROW(storage.getCurrentSegmentSize("nonexistent_file"), std::runtime_error);
    EXPECT_THROW(storage.getCurrentSegmentPath("nonexistent_file"), std::runtime_error);

    // These should create the file if it doesn't exist
    EXPECT_NO_THROW(storage.writeToFile("nonexistent_file", generateRandomData(10)));
    EXPECT_NO_THROW(storage.flushFile("nonexistent_file"));
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}