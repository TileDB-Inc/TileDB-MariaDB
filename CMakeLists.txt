cmake_minimum_required(VERSION 3.3)

project(mytile)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/")

if (CMAKE_VERSION VERSION_LESS "3.1")
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++14")
    endif ()
else ()
    set (CMAKE_CXX_STANDARD 14)
endif ()


############################################################
# "make format" and "make check-format" targets
############################################################

set(SCRIPTS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/scripts")

find_package(ClangTools)
if (${CLANG_FORMAT_FOUND})
    # runs clang format and updates files in place.
    add_custom_target(format ${SCRIPTS_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 1
            `find ${CMAKE_CURRENT_SOURCE_DIR}/tiledb
            ${CMAKE_CURRENT_SOURCE_DIR}/test/src
            ${CMAKE_CURRENT_SOURCE_DIR}/examples
            ${CMAKE_CURRENT_SOURCE_DIR}/tools
            -name \\*.cc -or -name \\*.c -or -name \\*.h`)

    # runs clang format and exits with a non-zero exit code if any files need to be reformatted
    add_custom_target(check-format ${SCRIPTS_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 0
            `find ${CMAKE_CURRENT_SOURCE_DIR}/tiledb
            ${CMAKE_CURRENT_SOURCE_DIR}/test/src
            ${CMAKE_CURRENT_SOURCE_DIR}/examples
            ${CMAKE_CURRENT_SOURCE_DIR}/tools
            -name \\*.cc -or -name \\*.c -or -name \\*.h`)
endif()

####################
# Add MYSQL Plugin #
####################

# Get library directory for multiarch linux distros
include(GNUInstallDirs)

find_package(TileDB)

# Does not actually work currently
if(NOT TileDB_FOUND)
    include(ExternalProject)

    set(TILEDB_INSTALL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/externals/install")

    ExternalProject_Add(tiledb
            PREFIX "externals"
            SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/TileDB"
            DOWNLOAD_COMMAND ""
            #  CONFIGURE_COMMAND ""
            #  BUILD_IN_SOURCE TRUE
            INSTALL_COMMAND $(MAKE) -C tiledb install
            #  UPDATE_COMMAND ""
            CMAKE_ARGS
            -DTILEDB_VERBOSE=ON
            -DTILEDB_S3=ON
            -DTILEDB_FORCE_ALL_DEPS=ON
            -DCMAKE_INSTALL_PREFIX=${TILEDB_INSTALL_PREFIX}
            -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
            LOG_DOWNLOAD TRUE
            LOG_CONFIGURE TRUE
            LOG_BUILD TRUE
            LOG_INSTALL TRUE
            )

    ExternalProject_Get_Property(tiledb install_dir)
    target_include_directories(mytile PUBLIC "${TILEDB_INSTALL_PREFIX}/include" "mytile")

    add_dependencies(mytile tiledb)

    # Manually link static merged library
    # Must use full library name else cmake things its the external target of `tiledb`
    SET(TILEDB_LIBRARIES "${TILEDB_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}/libtbb${CMAKE_SHARED_LIBRARY_SUFFIX}")
else()
    # First check for release lib details
    GET_TARGET_PROPERTY(TILEDB_LIBRARIES TileDB::tiledb_shared IMPORTED_LOCATION_RELEASE)
    # If no release lib try debug lib
    if(${TILEDB_LIBRARIES} STREQUAL "NOTFOUND")
        GET_TARGET_PROPERTY(TILEDB_LIBRARIES TileDB::tiledb_shared IMPORTED_LOCATION_DEBUG)
    endif()
    message(STATUS ${TILEDB_LIBRARIES})
endif()

file(GLOB SOURCE_FILES mytile/*.cc)

# A the storage engine plugin
#if(WITH_EMBEDDED_SERVER)
MYSQL_ADD_PLUGIN(mytile ${SOURCE_FILES} STORAGE_ENGINE DEFAULT RECOMPILE_FOR_EMBEDDED LINK_LIBRARIES ${TILEDB_LIBRARIES})
#else()
#MYSQL_ADD_PLUGIN(mytile ${SOURCE_FILES} STORAGE_ENGINE DEFAULT MODULE_ONLY RECOMPILE_FOR_EMBEDDED LINK_LIBRARIES ${TILEDB_LIBRARIES})
#endif()
