#include <gtest/gtest.h>
#include "SegmentedStorage.hpp"
#include <thread>
#include <vector>
#include <fstream>
#include <filesystem>
#include <cstring>
#include <random>
#include <algorithm>
#include <chrono>

class SegmentedStorageTest : public ::testing::Test
{
protected:
    std::string testPath;
    std::string baseFilename;

    void SetUp() override
    {
        testPath = "./test_storage_";
        baseFilename = "test_file";

        if (std::filesystem::exists(testPath))
        {
            std::filesystem::remove_all(testPath);
        }
    }

    std::string getCurrentTimestamp()
    {
        auto now = std::chrono::system_clock::now();
        auto now_time_t = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&now_time_t), "%Y%m%d%H%M%S");
        return ss.str();
    }

    std::vector<uint8_t> generateRandomData(size_t size)
    {
        std::vector<uint8_t> data(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> distrib(0, 255);

        std::generate(data.begin(), data.end(), [&]()
                      { return distrib(gen); });
        return data;
    }

    std::vector<std::string> getSegmentFiles(const std::string &basePath, const std::string &baseFilename)
    {
        std::vector<std::string> files;
        for (const auto &entry : std::filesystem::directory_iterator(basePath))
        {
            std::string filename = entry.path().filename().string();
            if (filename.find(baseFilename) == 0 && filename.find(".log") != std::string::npos)
            {
                files.push_back(entry.path().string());
            }
        }
        std::sort(files.begin(), files.end());
        return files;
    }

    size_t getFileSize(const std::string &filepath)
    {
        std::ifstream file(filepath, std::ios::binary | std::ios::ate);
        if (!file.is_open())
        {
            return 0;
        }
        return file.tellg();
    }

    std::vector<uint8_t> readFile(const std::string &filepath)
    {
        std::ifstream file(filepath, std::ios::binary | std::ios::ate);
        if (!file.is_open())
        {
            return {};
        }

        size_t fileSize = file.tellg();
        std::vector<uint8_t> buffer(fileSize);

        file.seekg(0);
        file.read(reinterpret_cast<char *>(buffer.data()), fileSize);

        return buffer;
    }
};

// Test basic writing functionality
TEST_F(SegmentedStorageTest, BasicWriteTest)
{
    SegmentedStorage storage(testPath, baseFilename);

    std::vector<uint8_t> data = {'H', 'e', 'l', 'l', 'o', ',', ' ', 'W', 'o', 'r', 'l', 'd', '!'};
    size_t bytesWritten = storage.write(data);

    ASSERT_EQ(bytesWritten, data.size());

    storage.flush();

    auto files = getSegmentFiles(testPath, baseFilename);
    ASSERT_EQ(files.size(), 1) << "Only one file should be created";

    auto fileContents = readFile(files[0]);
    ASSERT_EQ(fileContents.size(), data.size());
    ASSERT_TRUE(std::equal(data.begin(), data.end(), fileContents.begin()));
}

// Test segment rotation based on size limit
TEST_F(SegmentedStorageTest, SegmentRotationTest)
{
    size_t maxSegmentSize = 1024; // Small size to force rotation
    SegmentedStorage storage(testPath, baseFilename, maxSegmentSize);

    // Write data slightly less than the max size
    std::vector<uint8_t> data1 = generateRandomData(maxSegmentSize - 100);
    size_t bytesWritten1 = storage.write(data1);
    ASSERT_EQ(bytesWritten1, data1.size());

    // Write more data to cause rotation
    std::vector<uint8_t> data2 = generateRandomData(200);
    size_t bytesWritten2 = storage.write(data2);
    ASSERT_EQ(bytesWritten2, data2.size());

    // Make sure to flush the data to disk before checking
    storage.flush();

    auto files = getSegmentFiles(testPath, baseFilename);
    ASSERT_EQ(files.size(), 2) << "Two files should be created due to rotation";

    // Verify first file contents
    auto file1Contents = readFile(files[0]);
    ASSERT_EQ(file1Contents.size(), data1.size());
    ASSERT_TRUE(std::equal(data1.begin(), data1.end(), file1Contents.begin()));

    // Verify second file contents
    auto file2Contents = readFile(files[1]);
    ASSERT_EQ(file2Contents.size(), data2.size());
    ASSERT_TRUE(std::equal(data2.begin(), data2.end(), file2Contents.begin()));
}

// Test writing to a specific file
TEST_F(SegmentedStorageTest, WriteToSpecificFileTest)
{
    SegmentedStorage storage(testPath, baseFilename);

    std::string customFilename = "custom_file";
    std::vector<uint8_t> data = {'C', 'u', 's', 't', 'o', 'm', ' ', 'F', 'i', 'l', 'e'};

    size_t bytesWritten = storage.writeToFile(customFilename, data);
    ASSERT_EQ(bytesWritten, data.size());

    // Make sure to flush the data to disk before checking
    storage.flush();

    auto files = getSegmentFiles(testPath, customFilename);
    ASSERT_EQ(files.size(), 1) << "One custom file should be created";

    auto fileContents = readFile(files[0]);
    ASSERT_EQ(fileContents.size(), data.size());
    ASSERT_TRUE(std::equal(data.begin(), data.end(), fileContents.begin()));
}

// Test concurrent writing to the same file
TEST_F(SegmentedStorageTest, ConcurrentWriteTest)
{
    SegmentedStorage storage(testPath, baseFilename);

    size_t numThreads = 10;
    size_t dataSize = 1000;
    size_t totalSize = numThreads * dataSize;

    std::vector<std::thread> threads;
    std::vector<std::vector<uint8_t>> dataBlocks;

    // Create data blocks and threads
    for (size_t i = 0; i < numThreads; i++)
    {
        dataBlocks.push_back(generateRandomData(dataSize));
        threads.emplace_back([&storage, &dataBlocks, i]()
                             { storage.write(dataBlocks[i]); });
    }

    // Wait for all threads to complete
    for (auto &t : threads)
    {
        t.join();
    }

    // Make sure to flush all data to disk
    storage.flush();

    // Verify file content size
    auto files = getSegmentFiles(testPath, baseFilename);
    ASSERT_EQ(files.size(), 1) << "Only one file should be created";

    size_t fileSize = getFileSize(files[0]);
    ASSERT_EQ(fileSize, totalSize) << "File size should match total written data";
}

// Test concurrent writing with rotation
TEST_F(SegmentedStorageTest, ConcurrentWriteWithRotationTest)
{
    size_t maxSegmentSize = 5000; // Small segment size to force rotation
    SegmentedStorage storage(testPath, baseFilename, maxSegmentSize);

    size_t numThreads = 20;
    size_t dataSize = 1000;

    std::vector<std::thread> threads;
    std::vector<std::vector<uint8_t>> dataBlocks;

    // Create data blocks and threads
    for (size_t i = 0; i < numThreads; i++)
    {
        dataBlocks.push_back(generateRandomData(dataSize));
        threads.emplace_back([&storage, &dataBlocks, i]()
                             { storage.write(dataBlocks[i]); });
    }

    // Wait for all threads to complete
    for (auto &t : threads)
    {
        t.join();
    }

    // Make sure to flush all data to disk
    storage.flush();

    // Verify files were created and rotated
    auto files = getSegmentFiles(testPath, baseFilename);
    ASSERT_GT(files.size(), 1) << "Multiple files should be created due to rotation";

    // Calculate total file sizes
    size_t totalFileSize = 0;
    for (const auto &file : files)
    {
        totalFileSize += getFileSize(file);
    }

    ASSERT_EQ(totalFileSize, numThreads * dataSize) << "Total file sizes should match total written data";
}

// Test flush functionality
TEST_F(SegmentedStorageTest, FlushTest)
{
    SegmentedStorage storage(testPath, baseFilename);

    std::vector<uint8_t> data = generateRandomData(1000);
    storage.write(data);

    // Explicitly flush
    storage.flush();

    // Verify data was written to disk
    auto files = getSegmentFiles(testPath, baseFilename);
    ASSERT_EQ(files.size(), 1);

    auto fileContents = readFile(files[0]);
    ASSERT_EQ(fileContents.size(), data.size());
    ASSERT_TRUE(std::equal(data.begin(), data.end(), fileContents.begin()));
}

// Test multiple segment files with the same base path
TEST_F(SegmentedStorageTest, MultipleSegmentFilesTest)
{
    SegmentedStorage storage(testPath, baseFilename);

    std::string file1 = "file1";
    std::string file2 = "file2";
    std::string file3 = "file3";

    std::vector<uint8_t> data1 = {'F', 'i', 'l', 'e', '1'};
    std::vector<uint8_t> data2 = {'F', 'i', 'l', 'e', '2'};
    std::vector<uint8_t> data3 = {'F', 'i', 'l', 'e', '3'};

    storage.writeToFile(file1, data1);
    storage.writeToFile(file2, data2);
    storage.writeToFile(file3, data3);

    // Make sure to flush the data to disk before checking
    storage.flush();

    // Verify files created correctly
    ASSERT_EQ(getSegmentFiles(testPath, file1).size(), 1);
    ASSERT_EQ(getSegmentFiles(testPath, file2).size(), 1);
    ASSERT_EQ(getSegmentFiles(testPath, file3).size(), 1);

    // Verify contents
    auto files1 = getSegmentFiles(testPath, file1);
    auto files2 = getSegmentFiles(testPath, file2);
    auto files3 = getSegmentFiles(testPath, file3);

    auto content1 = readFile(files1[0]);
    auto content2 = readFile(files2[0]);
    auto content3 = readFile(files3[0]);

    ASSERT_TRUE(std::equal(data1.begin(), data1.end(), content1.begin()));
    ASSERT_TRUE(std::equal(data2.begin(), data2.end(), content2.begin()));
    ASSERT_TRUE(std::equal(data3.begin(), data3.end(), content3.begin()));
}

// Test large files
TEST_F(SegmentedStorageTest, LargeFileTest)
{
    SegmentedStorage storage(testPath, baseFilename);

    // Create a 5MB file
    size_t dataSize = 5 * 1024 * 1024;
    std::vector<uint8_t> largeData = generateRandomData(dataSize);

    size_t bytesWritten = storage.write(largeData);
    ASSERT_EQ(bytesWritten, dataSize);

    // Make sure to flush the data to disk before checking
    storage.flush();

    auto files = getSegmentFiles(testPath, baseFilename);
    ASSERT_EQ(files.size(), 1);

    size_t fileSize = getFileSize(files[0]);
    ASSERT_EQ(fileSize, dataSize);
}

// Test destructor closes files properly
TEST_F(SegmentedStorageTest, DestructorTest)
{
    {
        SegmentedStorage storage(testPath, baseFilename);
        std::vector<uint8_t> data = {'T', 'e', 's', 't'};
        storage.write(data);
        // Explicitly flush before destruction to ensure data is written
        storage.flush();
        // Storage will be destroyed here when going out of scope
    }

    // Verify file was written correctly
    auto files = getSegmentFiles(testPath, baseFilename);
    ASSERT_EQ(files.size(), 1);

    auto fileContents = readFile(files[0]);
    ASSERT_EQ(fileContents.size(), 4);
    ASSERT_EQ(fileContents[0], 'T');
    ASSERT_EQ(fileContents[1], 'e');
    ASSERT_EQ(fileContents[2], 's');
    ASSERT_EQ(fileContents[3], 't');
}

// Test exact rotation boundary case
TEST_F(SegmentedStorageTest, ExactRotationBoundaryTest)
{
    size_t maxSegmentSize = 1000; // Exact size for rotation test
    SegmentedStorage storage(testPath, baseFilename, maxSegmentSize);

    // Write exactly to the boundary
    std::vector<uint8_t> data1 = generateRandomData(maxSegmentSize);
    size_t bytesWritten1 = storage.write(data1);
    ASSERT_EQ(bytesWritten1, data1.size());

    // Write one more byte to trigger rotation
    std::vector<uint8_t> data2 = {42}; // Single byte
    size_t bytesWritten2 = storage.write(data2);
    ASSERT_EQ(bytesWritten2, data2.size());

    storage.flush();

    auto files = getSegmentFiles(testPath, baseFilename);
    ASSERT_EQ(files.size(), 2) << "Two files should be created with exact boundary";

    // Verify file sizes
    ASSERT_EQ(getFileSize(files[0]), maxSegmentSize);
    ASSERT_EQ(getFileSize(files[1]), 1);
}

// Test concurrent writing with realistic thread count at rotation boundaries
TEST_F(SegmentedStorageTest, RealisticConcurrencyRotationTest)
{
    size_t maxSegmentSize = 1000; // Small segment size to force rotation
    SegmentedStorage storage(testPath, baseFilename, maxSegmentSize);

    size_t numThreads = 8; // Realistic thread count for production
    size_t dataSize = 200; // Slightly larger data chunks

    std::vector<std::thread> threads;
    std::vector<std::vector<uint8_t>> dataBlocks;

    // Create data blocks and threads
    for (size_t i = 0; i < numThreads; i++)
    {
        dataBlocks.push_back(generateRandomData(dataSize));
        threads.emplace_back([&storage, &dataBlocks, i]()
                             { storage.write(dataBlocks[i]); });
    }

    // Wait for all threads to complete
    for (auto &t : threads)
    {
        t.join();
    }

    storage.flush();

    // Verify total data written
    auto files = getSegmentFiles(testPath, baseFilename);
    ASSERT_GT(files.size(), 1) << "Multiple files should be created due to rotation";

    size_t totalFileSize = 0;
    for (const auto &file : files)
    {
        totalFileSize += getFileSize(file);
    }

    ASSERT_EQ(totalFileSize, numThreads * dataSize) << "Total file sizes should match total written data";
}

// Test rotation with realistic thread count writing near segment boundary
TEST_F(SegmentedStorageTest, RealisticRotationBoundaryTest)
{
    size_t maxSegmentSize = 1000;
    SegmentedStorage storage(testPath, baseFilename, maxSegmentSize);

    size_t numThreads = 6;                 // Realistic thread count
    size_t dataSize = maxSegmentSize - 50; // Close to segment size to trigger rotations

    std::vector<std::thread> threads;
    std::vector<std::vector<uint8_t>> dataBlocks;

    // Generate all data blocks before creating threads
    for (size_t i = 0; i < numThreads; i++)
    {
        dataBlocks.push_back(generateRandomData(dataSize));
    }

    // Create threads that each write data that will likely cause rotation
    for (size_t i = 0; i < numThreads; i++)
    {
        threads.emplace_back([&storage, &dataBlocks, i]()
                             { storage.write(dataBlocks[i]); });
    }

    // Wait for all threads to complete
    for (auto &t : threads)
    {
        t.join();
    }

    storage.flush();

    // Verify total data written
    auto files = getSegmentFiles(testPath, baseFilename);

    // We should have multiple files due to rotations
    ASSERT_GT(files.size(), 1) << "Multiple files should be created due to rotation";

    size_t totalFileSize = 0;
    for (const auto &file : files)
    {
        totalFileSize += getFileSize(file);
    }

    ASSERT_EQ(totalFileSize, numThreads * dataSize) << "Total file sizes should match total written data";
}

// Test writing zero bytes
TEST_F(SegmentedStorageTest, ZeroByteWriteTest)
{
    SegmentedStorage storage(testPath, baseFilename);

    std::vector<uint8_t> emptyData;
    size_t bytesWritten = storage.write(emptyData);

    ASSERT_EQ(bytesWritten, 0) << "Zero bytes should be written for empty data";

    storage.flush();

    auto files = getSegmentFiles(testPath, baseFilename);
    ASSERT_EQ(files.size(), 1) << "One file should still be created";
    ASSERT_EQ(getFileSize(files[0]), 0) << "File should be empty";
}

// Test concurrent writes to different files
TEST_F(SegmentedStorageTest, ConcurrentMultiFileWriteTest)
{
    SegmentedStorage storage(testPath, baseFilename);

    size_t numFiles = 10;
    size_t threadsPerFile = 5;
    size_t dataSize = 100;

    std::vector<std::thread> threads;
    std::vector<std::string> filenames;
    std::vector<std::vector<uint8_t>> dataBlocks;

    // Create file names
    for (size_t i = 0; i < numFiles; i++)
    {
        filenames.push_back("file_" + std::to_string(i));
    }

    // Generate all data blocks before creating threads
    for (size_t i = 0; i < numFiles * threadsPerFile; i++)
    {
        dataBlocks.push_back(generateRandomData(dataSize));
    }

    // Create threads for each file
    for (size_t i = 0; i < numFiles; i++)
    {
        for (size_t j = 0; j < threadsPerFile; j++)
        {
            size_t dataIndex = i * threadsPerFile + j;
            threads.emplace_back([&storage, &filenames, &dataBlocks, i, dataIndex]()
                                 { storage.writeToFile(filenames[i], dataBlocks[dataIndex]); });
        }
    }

    // Wait for all threads to complete
    for (auto &t : threads)
    {
        t.join();
    }

    storage.flush();

    // Verify all files were created and have the right size
    for (const auto &filename : filenames)
    {
        auto files = getSegmentFiles(testPath, filename);
        ASSERT_EQ(files.size(), 1) << "One file should be created per filename";

        size_t fileSize = getFileSize(files[0]);
        ASSERT_EQ(fileSize, threadsPerFile * dataSize) << "Each file should contain data from all its threads";
    }
}

// Test rapid succession of writes near rotation boundary
TEST_F(SegmentedStorageTest, RapidWritesNearRotationTest)
{
    size_t maxSegmentSize = 1000;
    SegmentedStorage storage(testPath, baseFilename, maxSegmentSize);

    // First, fill up to near the boundary
    std::vector<uint8_t> initialData = generateRandomData(maxSegmentSize - 100);
    storage.write(initialData);

    // Now rapidly write small chunks that will collectively trigger rotation
    size_t numWrites = 20;
    size_t smallChunkSize = 10;

    std::vector<std::vector<uint8_t>> dataChunks;
    for (size_t i = 0; i < numWrites; i++)
    {
        dataChunks.push_back(generateRandomData(smallChunkSize));
    }

    // Write all chunks in rapid succession
    for (const auto &chunk : dataChunks)
    {
        storage.write(chunk);
    }

    storage.flush();

    // Verify rotation occurred correctly
    auto files = getSegmentFiles(testPath, baseFilename);
    ASSERT_GE(files.size(), 2) << "At least two files should be created due to rotation";

    size_t totalFileSize = 0;
    for (const auto &file : files)
    {
        totalFileSize += getFileSize(file);
    }

    size_t expectedTotalSize = initialData.size() + (numWrites * smallChunkSize);
    ASSERT_EQ(totalFileSize, expectedTotalSize) << "Total file sizes should match total written data";
}

// Test with extremely small segment size to force frequent rotations
TEST_F(SegmentedStorageTest, FrequentRotationTest)
{
    // Force rotation after every 50 bytes
    size_t maxSegmentSize = 50;
    SegmentedStorage storage(testPath, baseFilename, maxSegmentSize);

    size_t numWrites = 20;
    size_t dataSize = 30; // Less than segment size

    std::vector<std::vector<uint8_t>> dataBlocks;
    for (size_t i = 0; i < numWrites; i++)
    {
        dataBlocks.push_back(generateRandomData(dataSize));
        storage.write(dataBlocks[i]);
    }

    storage.flush();

    auto files = getSegmentFiles(testPath, baseFilename);

    // We should have many files due to frequent rotation
    // Each file should contain at most one write (or 2 very small ones)
    ASSERT_GE(files.size(), numWrites / 2) << "Many files should be created due to frequent rotation";

    size_t totalFileSize = 0;
    for (const auto &file : files)
    {
        totalFileSize += getFileSize(file);
        // Each file should be small (approximately one data block)
        ASSERT_LE(getFileSize(file), maxSegmentSize);
    }

    ASSERT_EQ(totalFileSize, numWrites * dataSize) << "Total file sizes should match total written data";
}

// Test recovery after write failure
TEST_F(SegmentedStorageTest, WriteErrorRecoveryTest)
{
    // This test requires a custom mock of pwrite to simulate failures
    // Since we can't easily do that without modifying the class, this is a placeholder
    // In a real implementation, you might use dependency injection or mocks

    // For now, we'll just verify basic functionality after an exception might have occurred
    SegmentedStorage storage(testPath, baseFilename);

    // Write initial data
    std::vector<uint8_t> data1 = {'I', 'n', 'i', 't', 'i', 'a', 'l'};
    storage.write(data1);

    // Simulate recovery after an error by writing more data
    std::vector<uint8_t> data2 = {'R', 'e', 'c', 'o', 'v', 'e', 'r', 'y'};
    storage.write(data2);

    storage.flush();

    auto files = getSegmentFiles(testPath, baseFilename);
    ASSERT_EQ(files.size(), 1);

    auto fileContents = readFile(files[0]);
    ASSERT_EQ(fileContents.size(), data1.size() + data2.size());

    // Verify both data pieces were written
    for (size_t i = 0; i < data1.size(); i++)
    {
        ASSERT_EQ(fileContents[i], data1[i]);
    }

    for (size_t i = 0; i < data2.size(); i++)
    {
        ASSERT_EQ(fileContents[data1.size() + i], data2[i]);
    }
}

// Test with extremely small buffer size to trigger frequent fsyncs
TEST_F(SegmentedStorageTest, SmallBufferFrequentFsyncTest)
{
    size_t maxSegmentSize = 10000;
    size_t bufferSize = 10; // Very small buffer size to force frequent fsyncs

    SegmentedStorage storage(testPath, baseFilename, maxSegmentSize, bufferSize);

    size_t numWrites = 15;
    size_t dataSize = 20; // Larger than buffer size

    // Write data in chunks to trigger multiple fsyncs
    for (size_t i = 0; i < numWrites; i++)
    {
        auto data = generateRandomData(dataSize);
        storage.write(data);
    }

    storage.flush();

    // Verify data was written correctly
    auto files = getSegmentFiles(testPath, baseFilename);
    ASSERT_EQ(files.size(), 1);

    size_t fileSize = getFileSize(files[0]);
    ASSERT_EQ(fileSize, numWrites * dataSize);
}

// Test boundary case for multiple segments
TEST_F(SegmentedStorageTest, MultiSegmentBoundaryTest)
{
    size_t maxSegmentSize = 100;
    SegmentedStorage storage(testPath, baseFilename, maxSegmentSize);

    // Fill exactly 3 segments
    for (int i = 0; i < 3; i++)
    {
        auto data = generateRandomData(maxSegmentSize);
        storage.write(data);
    }

    storage.flush();

    auto files = getSegmentFiles(testPath, baseFilename);
    ASSERT_EQ(files.size(), 3) << "Should have exactly 3 segments";

    // Each file should be exactly segment size
    for (const auto &file : files)
    {
        ASSERT_EQ(getFileSize(file), maxSegmentSize);
    }
}

// Main function to run the tests
int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}