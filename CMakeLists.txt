cmake_minimum_required(VERSION 3.3)

project(mytile)

if (CMAKE_VERSION VERSION_LESS "3.1")
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++14")
    endif ()
else ()
    set (CMAKE_CXX_STANDARD 14)
endif ()

file(GLOB SOURCE_FILES mytile/*.cc)

# A the storage engine plugin
MYSQL_ADD_PLUGIN(mytile ${SOURCE_FILES} STORAGE_ENGINE MODULE_ONLY RECOMPILE_FOR_EMBEDDED)

if(TARGET mytile)
  # Get library directory for multiarch linux distros
  include(GNUInstallDirs)

  find_package(TileDB)

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
    target_link_libraries(mytile "${TILEDB_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}/libtiledb${CMAKE_STATIC_LIBRARY_SUFFIX}")
    target_link_libraries(mytile "${TILEDB_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}/libtbb${CMAKE_SHARED_LIBRARY_SUFFIX}")
  else()
    #set(TBB_DIR "${CMAKE_CURRENT_SOURCE_DIR}external/TileDB/build/externals/src/ep_tbb/cmake/TileDB/build/externals/src/ep_tbb/cmake")
    #find_package(TBB CONFIG REQUIRED)
    target_link_libraries(mytile TileDB::tiledb_shared)
  endif()
endif()
