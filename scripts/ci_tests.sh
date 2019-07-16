#!/usr/bin/env bash

set -e -x
export MARIADB_VERSION="mariadb-10.4.6"
mkdir tmp
shopt -s extglob
mv !(tmp) tmp # Move everything but tmp
wget https://downloads.mariadb.org/interstitial/${MARIADB_VERSION}/source/${MARIADB_VERSION}.tar.gz \
&& tar xf ${MARIADB_VERSION}.tar.gz \
&& mv tmp ${MARIADB_VERSION}/storage/mytile \
&& cd ${MARIADB_VERSION} \
&& mkdir build \
&& cd build \
&& cmake -DPLUGIN_TOKUDB=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_MROONGA=NO -DPLUGIN_SPIDER=NO -DPLUGIN_SPHINX=NO -DPLUGIN_FEDERATED=NO -DPLUGIN_FEDERATEDX=NO -DPLUGIN_CONNECT=NO -DCMAKE_BUILD_TYPE=Debug .. \
&& make -j$(nproc) \
&& if ! ./mysql-test/mysql-test-run.pl  --suite=mytile --debug; then cat ./mysql-test/var/log/mysqld.1.err && false; fi;
set +e +x
