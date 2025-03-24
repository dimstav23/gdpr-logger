enable_testing()

set(TEST_SOURCES
    tests/unit/test_LogEntry.cpp
    tests/unit/test_LoggingAPI.cpp
    tests/unit/test_LockFreeBuffer.cpp
    tests/unit/test_Compression.cpp
    tests/unit/test_Crypto.cpp
    tests/unit/test_Writer.cpp
    tests/integration/test_CompressionCrypto.cpp
    tests/integration/test_WriterBuffer.cpp
)

macro(add_gdpr_test TEST_NAME TEST_SOURCE)
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

add_gdpr_test(test_log_entry tests/unit/test_LogEntry.cpp)
add_gdpr_test(test_logging_api tests/unit/test_LoggingAPI.cpp)
add_gdpr_test(test_lock_free_buffer tests/unit/test_LockFreeBuffer.cpp)
add_gdpr_test(test_compression tests/unit/test_Compression.cpp)
add_gdpr_test(test_crypto tests/unit/test_Crypto.cpp)
add_gdpr_test(test_writer tests/unit/test_Writer.cpp)
add_gdpr_test(test_compression_crypto tests/integration/test_CompressionCrypto.cpp)
add_gdpr_test(writer_buffer tests/integration/test_WriterBuffer.cpp)