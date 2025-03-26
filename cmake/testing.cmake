enable_testing()

set(TEST_SOURCES
    # unit tests
    tests/unit/test_LogEntry.cpp
    tests/unit/test_LoggingAPI.cpp
    tests/unit/test_LockFreeBuffer.cpp
    tests/unit/test_Compression.cpp
    tests/unit/test_Crypto.cpp
    tests/unit/test_Writer.cpp
    # integration tests
    tests/integration/test_CompressionCrypto.cpp
    tests/integration/test_WriterBuffer.cpp
    # performance tests
    tests/performance/test_LockFreeBuffer_Performance.cpp
)

macro(add_test_suite TEST_NAME TEST_SOURCE)
    add_executable(${TEST_NAME} ${TEST_SOURCE})
    target_link_libraries(${TEST_NAME}
        PRIVATE
        GDPR_Logging_lib
        GTest::GTest 
        GTest::Main 
        GTest::gmock 
        GTest::gmock_main 
        pthread 
        OpenSSL::SSL 
        OpenSSL::Crypto 
        ZLIB::ZLIB
    )
    add_test(NAME ${TEST_NAME}_test COMMAND ${TEST_NAME})
endmacro()

# unit tests
add_test_suite(test_log_entry tests/unit/test_LogEntry.cpp)
add_test_suite(test_logging_api tests/unit/test_LoggingAPI.cpp)
add_test_suite(test_lock_free_buffer tests/unit/test_LockFreeBuffer.cpp)
add_test_suite(test_compression tests/unit/test_Compression.cpp)
add_test_suite(test_crypto tests/unit/test_Crypto.cpp)
add_test_suite(test_writer tests/unit/test_Writer.cpp)
# integration tests
add_test_suite(test_compression_crypto tests/integration/test_CompressionCrypto.cpp)
add_test_suite(test_writer_buffer tests/integration/test_WriterBuffer.cpp)
# performance tests
add_test_suite(test_buffer_performance tests/performance/test_LockFreeBuffer_Performance.cpp)