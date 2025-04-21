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

// Main function to run the tests
int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}