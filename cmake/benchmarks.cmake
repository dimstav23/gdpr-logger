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
    encryption_usage
    small_batch_storm
    large_batch_steady
    multi_producer_small_batch
    burst
    main
)

foreach(benchmark ${BENCHMARK_NAMES})
    add_executable(${benchmark}_benchmark benchmarks/${benchmark}_benchmark.cpp)
    target_link_libraries(${benchmark}_benchmark ${BENCHMARK_LIBS})
endforeach()