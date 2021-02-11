#!/bin/bash

set -v -e -x

original_dir=$PWD
export TILEDB_VERSION="2.2.3"
export MARIADB_VERSION="mariadb-10.4.17"
mkdir tmp
shopt -s extglob
mv !(tmp) tmp # Move everything but tmp

#location of required embedded libraries
mkdir embedded-package

#build TileDB with static library
git clone https://github.com/TileDB-Inc/TileDB.git -b ${TILEDB_VERSION} TileDB-${TILEDB_VERSION}
cd TileDB-${TILEDB_VERSION}
mkdir build && cd build
#TODO turn on options
cmake -DTILEDB_VERBOSE=OFF -DTILEDB_S3=OFF -DTILEDB_AZURE=OFF -DTILEDB_GCS=OFF -DTILEDB_SERIALIZATION=ON -DTILEDB_STATIC=ON -DCMAKE_BUILD_TYPE=Release .. \
&& make -j$(nproc) \
&& cd $original_dir

#copy tiledb static library
cp TileDB-${TILEDB_VERSION}/build/tiledb/tiledb/libtiledb.a embedded-package/

#build MariaDB embedded
git clone https://github.com/MariaDB/server.git -b ${MARIADB_VERSION} ${MARIADB_VERSION}
mv tmp ${MARIADB_VERSION}/storage/mytile \
&& cd ${MARIADB_VERSION} \
&& mkdir builddir \
&& cd builddir \
&& cmake -DPLUGIN_TOKUDB=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_MROONGA=NO -DPLUGIN_SPIDER=NO -DPLUGIN_SPHINX=NO -DPLUGIN_FEDERATED=NO -DPLUGIN_FEDERATEDX=NO -DPLUGIN_CONNECT=NO -DCMAKE_BUILD_TYPE=Debug -SWITH_DEBUG=1 .. \
&& make -j$(nproc) \
cd $original_dir

# Install TileDB using 2.0 release
if [[ -z ${SUPERBUILD+x} || "${SUPERBUILD}" == "OFF" ]]; then

  if [[ "$AGENT_OS" == "Linux" ]]; then
    curl -L -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.2.3/tiledb-linux-2.2.3-dbaf5ff-full.tar.gz \
    && sudo tar -C /usr/local -xvf tiledb.tar.gz
  elif [[ "$AGENT_OS" == "Darwin" ]]; then
    curl -L -o tiledb.tar.gz https://github.com/TileDB-Inc/TileDB/releases/download/2.2.3/tiledb-macos-2.2.3-dbaf5ff-full.tar.gz \
    && sudo tar -C /usr/local -xvf tiledb.tar.gz
  else
    mkdir build_deps && cd build_deps \
    && git clone https://github.com/TileDB-Inc/TileDB.git -b 2.2.3 && cd TileDB \
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


