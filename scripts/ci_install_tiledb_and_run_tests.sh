#!/bin/bash

set -v -e -x

original_dir=$PWD
export MARIADB_VERSION=${MARIADB_VERSION:="mariadb-10.5.13"}
mkdir tmp
shopt -s extglob
mv !(tmp) tmp # Move everything but tmp
# Download mariadb using git, this has a habit of failing so let's do it first
git clone --recurse-submodules https://github.com/MariaDB/server.git -b ${MARIADB_VERSION} ${MARIADB_VERSION}

TILEDB_FORCE_ALL_DEPS=${TILEDB_FORCE_ALL_DEPS:="OFF"}

# Install TileDB using 2.0 release
if [[ -z ${SUPERBUILD+x} || "${SUPERBUILD}" == "OFF" ]]; then

  if [[ "$AGENT_OS" == "Linux" ]]; then
    curl -L -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.12.0/tiledb-linux-x86_64-2.12.0-ac8a0df.tar.gz \
    && sudo tar -C /usr/local -xvf tiledb.tar.gz
  elif [[ "$AGENT_OS" == "Darwin" ]]; then
    curl -L -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.12.0/tiledb-macos-x86_64-2.12.0-ac8a0df.tar.gz \
    && sudo tar -C /usr/local -xvf tiledb.tar.gz
  else
    mkdir build_deps && cd build_deps \
    && git clone https://github.com/TileDB-Inc/TileDB.git -b 2.12.0 && cd TileDB \
    && mkdir -p build && cd build

     # Configure and build TileDB
     cmake -DTILEDB_VERBOSE=ON -DTILEDB_S3=ON -DTILEDB_SERIALIZATION=ON -DCMAKE_BUILD_TYPE=Debug .. \
     && make -j$(nproc) \
     && sudo make -C tiledb install \
     && cd $original_dir
   fi
else # set superbuild flags
  export SUPERBUILD_FLAGS_NEEDED="-Wno-error=deprecated-declarations"
  export CXXFLAGS="${CXXFLAGS} ${SUPERBUILD_FLAGS_NEEDED}"
  export CFLAGS="${CFLAGS} ${SUPERBUILD_FLAGS_NEEDED}"
fi

mv tmp ${MARIADB_VERSION}/storage/mytile \
&& cd ${MARIADB_VERSION}
mkdir builddir \
&& cd builddir \
&& cmake -DPLUGIN_OQGRAPH=NO -DWITH_MARIABACKUP=OFF -DPLUGIN_TOKUDB=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_MROONGA=NO -DPLUGIN_SPIDER=NO -DPLUGIN_SPHINX=NO -DPLUGIN_FEDERATED=NO -DPLUGIN_FEDERATEDX=NO -DPLUGIN_CONNECT=NO -DCMAKE_BUILD_TYPE=Debug -DWITH_DEBUG=1 -DTILEDB_FORCE_ALL_DEPS=${TILEDB_FORCE_ALL_DEPS} .. \
&& make -j$(nproc) \
&& if ! ./mysql-test/mysql-test-run.pl --suite=mytile --debug; then cat ./mysql-test/var/log/mysqld.1.err && false; fi;
