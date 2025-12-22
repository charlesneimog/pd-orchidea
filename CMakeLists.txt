cmake_minimum_required(VERSION 3.30)
project(orchidea)

set(PDCMAKE_FILE ${CMAKE_BINARY_DIR}/pd.cmake)
set(PDCMAKE_VERSION "v0.2.11")
if(NOT EXISTS "${PDCMAKE_FILE}")
    file(DOWNLOAD https://raw.githubusercontent.com/pure-data/pd.cmake/refs/tags/${PDCMAKE_VERSION}/pd.cmake ${PDCMAKE_FILE})
endif()
include(${PDCMAKE_FILE})

add_custom_target(orchidea COMMAND echo "Installing dependencies from orchidea...")
file(GLOB UPIC_LUA_FILES "${CMAKE_CURRENT_SOURCE_DIR}/src/*")
pd_add_datafile(orchidea "${UPIC_LUA_FILES}")
