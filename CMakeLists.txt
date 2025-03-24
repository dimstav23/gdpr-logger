cmake_minimum_required(VERSION 3.10)
project(GDPR_Logging)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(project_settings)
include(dependencies)
include(library)
include(executable)
include(testing)