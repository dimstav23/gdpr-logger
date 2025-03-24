add_executable(GDPR_Logging_exe src/main.cpp)

target_link_libraries(GDPR_Logging_exe 
    PRIVATE 
    GDPR_Logging_lib 
    OpenSSL::SSL 
    OpenSSL::Crypto 
    ZLIB::ZLIB
)