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

# Create a library for your core functionality
add_library(GDPR_Logging_lib
    src/LogEntry.cpp
)

# Create the main executable for your production code
add_executable(GDPR_Logging_exe src/main.cpp)
target_link_libraries(GDPR_Logging_exe GDPR_Logging_lib OpenSSL::SSL OpenSSL::Crypto)

# Create a separate test executable
add_executable(test_log_entry tests/unit/test_LogEntry.cpp src/LogEntry.cpp)
target_link_libraries(test_log_entry GDPR_Logging_lib GTest::GTest GTest::Main pthread OpenSSL::SSL OpenSSL::Crypto)

# Register the test with CTest
add_test(NAME LogEntryTests COMMAND test_log_entry)
