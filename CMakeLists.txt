cmake_minimum_required(VERSION 3.10)
project(GDPR_Logging)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Enable testing
enable_testing()

# Find dependencies
find_package(OpenSSL REQUIRED)
find_package(GTest REQUIRED)

# Include directories
include_directories(include)

# Create a library for the core functionality
add_library(GDPR_Logging_lib
    src/LogEntry.cpp
    src/LoggingAPI.cpp
    src/LockFreeBuffer.cpp
)

# Create the main executable for production code
add_executable(GDPR_Logging_exe src/main.cpp)
target_link_libraries(GDPR_Logging_exe GDPR_Logging_lib OpenSSL::SSL OpenSSL::Crypto)

# Create a test executable for LogEntry
add_executable(test_log_entry tests/unit/test_LogEntry.cpp src/LogEntry.cpp)
target_link_libraries(test_log_entry GDPR_Logging_lib GTest::GTest GTest::Main pthread OpenSSL::SSL OpenSSL::Crypto)

# Create a test executable for LoggingAPI
add_executable(test_logging_api tests/unit/test_LoggingAPI.cpp src/LoggingAPI.cpp src/LogEntry.cpp)
target_link_libraries(test_logging_api GDPR_Logging_lib GTest::GTest GTest::Main GTest::gmock GTest::gmock_main  pthread OpenSSL::SSL OpenSSL::Crypto)

# Create a test executable for LockFreeBuffer
add_executable(test_lock_free_buffer tests/unit/test_LockFreeBuffer.cpp src/LockFreeBuffer.cpp src/LogEntry.cpp)
target_link_libraries(test_lock_free_buffer GDPR_Logging_lib GTest::GTest GTest::Main GTest::gmock GTest::gmock_main  pthread OpenSSL::SSL OpenSSL::Crypto)

# Register the tests with CTest
add_test(NAME LogEntryTests COMMAND test_log_entry)
add_test(NAME LoggingAPITests COMMAND test_logging_api)
add_test(NAME LockFreeBufferTests COMMAND test_lock_free_buffer)