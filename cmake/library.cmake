set(LIBRARY_SOURCES
    src/LogEntry.cpp
    src/LoggingAPI.cpp
    src/LockFreeBuffer.cpp
    src/Compression.cpp
    src/Crypto.cpp
)

add_library(GDPR_Logging_lib ${LIBRARY_SOURCES})

target_link_libraries(GDPR_Logging_lib 
    PUBLIC 
    OpenSSL::SSL 
    OpenSSL::Crypto 
    ZLIB::ZLIB
)