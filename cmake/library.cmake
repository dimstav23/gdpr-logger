set(LIBRARY_SOURCES
    src/LogEntry.cpp
    src/LoggingAPI.cpp
    src/LockFreeQueue.cpp
    src/Compression.cpp
    src/Crypto.cpp
    src/Writer.cpp
    src/SegmentedStorage.cpp
    src/LoggingSystem.cpp
)

add_library(GDPR_Logging_lib ${LIBRARY_SOURCES})

target_link_libraries(GDPR_Logging_lib 
    PUBLIC 
    OpenSSL::SSL 
    OpenSSL::Crypto 
    ZLIB::ZLIB
)