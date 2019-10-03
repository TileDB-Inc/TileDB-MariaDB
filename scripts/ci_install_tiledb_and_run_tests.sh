#!/bin/bash

set -v -e -x

original_dir=$PWD
export MARIADB_VERSION="mariadb-10.4.8"
mkdir tmp
shopt -s extglob
mv !(tmp) tmp # Move everything but tmp
# Download mariadb, this has a habit of failing so let's do it first
wget https://downloads.mariadb.org/interstitial/${MARIADB_VERSION}/source/${MARIADB_VERSION}.tar.gz

# Install tiledb using 1.6 release
mkdir build_deps && cd build_deps \
&& git clone https://github.com/TileDB-Inc/TileDB.git -b 1.6.3 && cd TileDB \
&& export deps_args="" \
&& export bootstrap_args="--enable=verbose,static-tiledb,s3,serialization,debug" \
&& mkdir -p build && cd build

# Configure and build TileDB
../bootstrap $bootstrap_args \
&& make -j4 \
&& make -C tiledb install \
&& cd $original_dir

tar xf ${MARIADB_VERSION}.tar.gz \
&& mv tmp ${MARIADB_VERSION}/storage/mytile \
&& cd ${MARIADB_VERSION} \
&& mkdir builddir \
&& cd builddir \
&& cmake -DPLUGIN_TOKUDB=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_MROONGA=NO -DPLUGIN_SPIDER=NO -DPLUGIN_SPHINX=NO -DPLUGIN_FEDERATED=NO -DPLUGIN_FEDERATEDX=NO -DPLUGIN_CONNECT=NO -DCMAKE_BUILD_TYPE=Debug -SWITH_DEBUG=1 .. \
&& make -j$(nproc) \
&& if ! ./mysql-test/mysql-test-run.pl --suite=mytile --debug; then cat ./mysql-test/var/log/mysqld.1.err && false; fi;