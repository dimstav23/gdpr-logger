set(BENCHMARK_LIBS
    PRIVATE
    GDPR_Logging_lib
    OpenSSL::SSL
    OpenSSL::Crypto
    ZLIB::ZLIB
)

set(BENCHMARK_NAMES
    batch_size
    concurrency
    file_rotation
    filepath_diversity
    queue_capacity
)

foreach(benchmark ${BENCHMARK_NAMES})
    add_executable(${benchmark}_benchmark benchmarks/${benchmark}_benchmark.cpp)
    target_link_libraries(${benchmark}_benchmark ${BENCHMARK_LIBS})
endforeach()