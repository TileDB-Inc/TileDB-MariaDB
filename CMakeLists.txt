cmake_minimum_required(VERSION 3.3)

project(mytile)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/")

if (CMAKE_VERSION VERSION_LESS "3.1")
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++17")
    endif ()
else ()
    set (CMAKE_CXX_STANDARD 20)
endif ()

option(SUPERBUILD "Build missing dependencies" ON)
option(TILEDB_FORCE_ALL_DEPS "Build all dependencies" ON)


############################################################
# "make format" and "make check-format" targets
############################################################

set(SCRIPTS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/scripts")

find_package(ClangTools)
if (${CLANG_FORMAT_FOUND})
    # runs clang format and updates files in place.
    add_custom_target(format ${SCRIPTS_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 1
            `find ${CMAKE_CURRENT_SOURCE_DIR}/mytile
            -name \\*.cc -or -name \\*.c -or -name \\*.h`)

    # runs clang format and exits with a non-zero exit code if any files need to be reformatted
    add_custom_target(check-format ${SCRIPTS_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 0
            `find ${CMAKE_CURRENT_SOURCE_DIR}/mytile
            -name \\*.cc -or -name \\*.c -or -name \\*.h`)
endif()

####################
# Add MYSQL Plugin #
####################

# Get library directory for multiarch linux distros
include(GNUInstallDirs)

set(EP_INSTALL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/externals/install")

find_package(TileDB QUIET)

if(NOT TileDB_FOUND AND SUPERBUILD)
    MESSAGE(STATUS "TileDB not found for MyTile. Building as dependency from source")
    include(ExternalProject)

    SET(TILEDB_VERSION_MAJOR "2")
    SET(TILEDB_VERSION_MINOR "28")
    SET(TILEDB_VERSION_PATCH "0")
    SET(TILEDB_VERSION "${TILEDB_VERSION_MAJOR}.${TILEDB_VERSION_MINOR}.${TILEDB_VERSION_PATCH}")

    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=non-virtual-dtor -Wno-error=missing-braces")
    ExternalProject_Add(tiledb
            PREFIX "externals"
            URL "https://github.com/TileDB-Inc/TileDB/archive/${TILEDB_VERSION}.zip"
            URL_HASH SHA1=ec633e4440ff7d16f2668e37f24e80e3b7d168cd
            DOWNLOAD_NAME "tiledb.zip"
            CMAKE_ARGS
            -DCMAKE_INSTALL_PREFIX=${EP_INSTALL_PREFIX}
            -DCMAKE_PREFIX_PATH=${EP_INSTALL_PREFIX}
            -DCMAKE_BUILD_TYPE=RelWithDebInfo
            -DTILEDB_VERBOSE=ON
            -DTILEDB_S3=ON
            -DTILEDB_AZURE=ON
            -DBUILD_SHARED_LIBS=ON
            -DTILEDB_SERIALIZATION=ON
            -DTILEDB_FORCE_ALL_DEPS=${TILEDB_FORCE_ALL_DEPS}
            UPDATE_COMMAND ""
            INSTALL_COMMAND
            ${CMAKE_COMMAND} --build . --target install-tiledb
            LOG_DOWNLOAD TRUE
            LOG_CONFIGURE TRUE
            LOG_BUILD TRUE
            LOG_INSTALL TRUE
            LOG_OUTPUT_ON_FAILURE TRUE
            )

    # Manually link library
    # Must use full library name else cmake things its the external target of `tiledb`
    SET(TILEDB_LIBRARIES "${EP_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}/${CMAKE_SHARED_LIBRARY_PREFIX}tiledb${CMAKE_SHARED_LIBRARY_SUFFIX}.${TILEDB_VERSION_MAJOR}.${TILEDB_VERSION_MINOR}")
    FILE(GLOB TILEDB_INSTALL_LIBRARIES "${TILEDB_LIBRARIES}")
else()
    # This whole block could be simplified to set(TILEDB_LIBRARIES TileDB::tiledb_shared),
    # but requires passing GLOBAL to find_package(TileDB) which was introduced in CMake 3.24.
    # First check for release lib details
    GET_TARGET_PROPERTY(TILEDB_LIBRARIES TileDB::tiledb_shared IMPORTED_LOCATION_RELEASE)
    GET_TARGET_PROPERTY(TILEDB_INCLUDE_DIRECTORIES TileDB::tiledb_shared INTERFACE_INCLUDE_DIRECTORIES)
    # If no release lib try debug lib
    if(${TILEDB_LIBRARIES} MATCHES "NOTFOUND")
        GET_TARGET_PROPERTY(TILEDB_LIBRARIES TileDB::tiledb_shared IMPORTED_LOCATION_DEBUG)
    endif()
    message(STATUS "TILEDB_LIBRARIES: ${TILEDB_LIBRARIES}")
    message(STATUS "TILEDB_INCLUDE_DIRECTORIES: ${TILEDB_INCLUDE_DIRECTORIES}")
endif()

file(GLOB SOURCE_FILES mytile/*.cc)

if(NOT TileDB_FOUND AND SUPERBUILD)
    # We need to set RPATH so if tiledb is built as superbuild we correctly can find it in the plugin dir after install
    SET(CMAKE_SKIP_BUILD_RPATH  FALSE)
    SET(CMAKE_SKIP_RPATH FALSE)

    # when building, don't use the install RPATH already
    # (but later on when installing)
    SET(PLUGIN_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/${INSTALL_PLUGINDIR}")
    SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
    SET(CMAKE_INSTALL_RPATH "${PLUGIN_INSTALL_DIR}")

    # add the automatically determined parts of the RPATH
    # which point to directories outside the build tree to the install RPATH
    SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
endif()

# Add the storage engine plugin
# If we are compiling embedded server make it statically linked
if(WITH_EMBEDDED_SERVER)
    MYSQL_ADD_PLUGIN(mytile ${SOURCE_FILES} STORAGE_ENGINE DEFAULT RECOMPILE_FOR_EMBEDDED LINK_LIBRARIES ${TILEDB_LIBRARIES})
# Else build as a dynamic library
else()
    MYSQL_ADD_PLUGIN(mytile ${SOURCE_FILES} STORAGE_ENGINE DEFAULT MODULE_ONLY RECOMPILE_FOR_EMBEDDED LINK_LIBRARIES ${TILEDB_LIBRARIES})
endif()

if(NOT TileDB_FOUND AND SUPERBUILD)
    add_dependencies(mytile tiledb)

    target_include_directories(mytile PUBLIC "${EP_INSTALL_PREFIX}/include" "mytile")
    INSTALL(FILES ${TILEDB_INSTALL_LIBRARIES} DESTINATION ${INSTALL_PLUGINDIR})
else()
    target_include_directories(mytile PUBLIC "${TILEDB_INCLUDE_DIRECTORIES}")
endif()

# Set my.cnf values based on build flags
if(WITH_EMBEDDED_SERVER)
    SET(MYTILE_PLUGIN_LOAD_ADD "# plugin-load-add=ha_mytile")
else()
    SET(MYTILE_PLUGIN_LOAD_ADD "plugin-load-add=ha_mytile")
endif()
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/mysql-test/mytile/my.cnf.in ${CMAKE_CURRENT_SOURCE_DIR}/mysql-test/mytile/my.cnf)
