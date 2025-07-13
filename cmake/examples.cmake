set(EXAMPLE_SOURCES
    examples/main.cpp
)

add_executable(logging_example ${EXAMPLE_SOURCES})

target_link_libraries(logging_example
    PRIVATE
    GDPR_Logging_lib
)

target_include_directories(logging_example
    PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/external/concurrentqueue
)
