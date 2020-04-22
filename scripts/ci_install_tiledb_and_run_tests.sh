#!/bin/bash

set -v -e -x

original_dir=$PWD
export MARIADB_VERSION="mariadb-10.4.11"
mkdir tmp
shopt -s extglob
mv !(tmp) tmp # Move everything but tmp
# Download mariadb, this has a habit of failing so let's do it first
wget https://downloads.mariadb.org/interstitial/${MARIADB_VERSION}/source/${MARIADB_VERSION}.tar.gz
tar xf ${MARIADB_VERSION}.tar.gz

# Install tiledb using 2.0.0-rc4 release
mkdir build_deps && cd build_deps \
&& git clone https://github.com/TileDB-Inc/TileDB.git -b dev && cd TileDB \
&& mkdir -p build && cd build

# Configure and build TileDB
cmake -DTILEDB_VERBOSE=ON -DTILEDB_S3=ON -DTILEDB_SERIALIZATION=ON -DCMAKE_BUILD_TYPE=Debug .. \
&& make -j4 \
&& sudo make -C tiledb install \
&& cd $original_dir

mv tmp ${MARIADB_VERSION}/storage/mytile \
&& cd ${MARIADB_VERSION} \
&& mkdir builddir \
&& cd builddir \
&& cmake -DPLUGIN_TOKUDB=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_MROONGA=NO -DPLUGIN_SPIDER=NO -DPLUGIN_SPHINX=NO -DPLUGIN_FEDERATED=NO -DPLUGIN_FEDERATEDX=NO -DPLUGIN_CONNECT=NO -DCMAKE_BUILD_TYPE=Debug -SWITH_DEBUG=1 .. \
&& make -j$(nproc) \
&& if ! ./mysql-test/mysql-test-run.pl --suite=mytile --debug; then cat ./mysql-test/var/log/mysqld.1.err && false; fi;
