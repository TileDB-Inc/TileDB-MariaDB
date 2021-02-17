#!/bin/bash

set -v -e -x

original_dir=$PWD
export TILEDB_VERSION="2.2.3"
export MARIADB_VERSION="mariadb-10.4.17"
export CURL_VERSION="curl-7_68_0"
export OPENSSL_VERSION="OpenSSL_1_1_1f"
export ZSTD_VERSION="v1.4.8"
export CAPNPROTO_VERSION="v0.8.0"
export LZ4_VERSION="v1.9.3"
export BZ2_VERSION="bzip2-1.0.8"

mkdir tmp
shopt -s extglob
mv !(tmp) tmp # Move everything but tmp

#build everything position independent
export CFLAGS="${CXXFLAGS} -fPIC"
export CXXFLAGS="${CXXFLAGS} -fPIC"

#location of required embedded libraries
mkdir embedded-package

#build TileDB with static library
git clone https://github.com/TileDB-Inc/TileDB.git -b ${TILEDB_VERSION} TileDB-${TILEDB_VERSION}
cd TileDB-${TILEDB_VERSION}
mkdir build && cd build
#TODO turn on options
cmake -DCMAKE_INSTALL_PREFIX=$original_dir/TileDB-${TILEDB_VERSION}/dist -DTILEDB_VERBOSE=OFF -DTILEDB_S3=OFF -DTILEDB_AZURE=OFF -DTILEDB_GCS=OFF -DTILEDB_SERIALIZATION=ON -DTILEDB_STATIC=ON -DCMAKE_BUILD_TYPE=Release .. \
&& make -j$(nproc) \
&& make -C tiledb install \
&& cd $original_dir

#copy tiledb static library
cp TileDB-${TILEDB_VERSION}/build/tiledb/tiledb/libtiledb.a embedded-package/

#set tiledb include paths
export CPATH=$original_dir/TileDB-${TILEDB_VERSION}/dist/include/:${CPATH}
export CPLUS_INCLUDE_PATH=$original_dir/TileDB-${TILEDB_VERSION}/dist/include/:${CPATH}

#build MariaDB embedded
git clone https://github.com/MariaDB/server.git -b ${MARIADB_VERSION} ${MARIADB_VERSION}
mv tmp ${MARIADB_VERSION}/storage/mytile \
&& cd ${MARIADB_VERSION} \
&& mkdir build \
&& cd build \
&& cmake -DCMAKE_PREFIX_PATH=$original_dir/TileDB-${TILEDB_VERSION}/dist -DCMAKE_INSTALL_PREFIX=$original_dir/mytile_server -DWITH_EMBEDDED_SERVER=ON -DPLUGIN_INNODB=NO -DPLUGIN_INNOBASE=NO -DWITH_INNODB_BZIP2=OFF -DWITH_INNODB_LZ4=OFF -DWITH_INNODB_SNAPPY=OFF -DPLUGIN_TOKUDB=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_MROONGA=NO -DPLUGIN_SPIDER=NO -DPLUGIN_SPHINX=NO -DPLUGIN_FEDERATED=NO -DPLUGIN_FEDERATEDX=NO -DPLUGIN_CONNECT=NO -DWITH_MARIABACKUP=OFF -DWITH_NUMA=OFF -DENABLED_PROFILING=OFF -DPLUGIN_MYTILE=YES -DWITH_SYSTEMD=no -DWITH_PCRE=bundled -DWITH_ZLIB=bundled -DWITH_WSREP=OFF DCMAKE_BUILD_TYPE=Release .. \
&& make -j$(nproc) \
&& make install \
&& cd $original_dir

#copy mariadb dependencies
cp ${MARIADB_VERSION}/build/libmysqld/libmariadbd.a embedded-package/ \
&& cp ${MARIADB_VERSION}/build/sql/libsql.a         embedded-package/ \
&& cp ${MARIADB_VERSION}/build/pcre/libpcre.a       embedded-package/ \
&& cp ${MARIADB_VERSION}/build/zlib/libz.a          embedded-package/

#build curl
git clone https://github.com/curl/curl.git -b ${CURL_VERSION} curl-${CURL_VERSION}
cd curl-${CURL_VERSION} \
&& ./buildconf \
&& ./configure --disable-shared --without-ssl --disable-ares --disable-cookies --disable-crypto-auth --disable-ipv6 --disable-manual --disable-proxy --disable-unix-sockets --disable-verbose --disable-versioned-symbols --enable-hidden-symbols --without-libidn --without-librtmp --without-ssl --without-zlib \
&& make -j$(nproc) \
&& cd $original_dir

#copy curl dependencies
cp curl-${CURL_VERSION}/lib/.libs/libcurl.a embedded-package/

#build openssl
git clone https://github.com/openssl/openssl.git -b ${OPENSSL_VERSION} openssl-${OPENSSL_VERSION}
cd openssl-${OPENSSL_VERSION} \
&& ./config \
&& make -j$(nproc) \
&& cd $original_dir

#copy opensll dependencies
cp openssl-${OPENSSL_VERSION}/libssl.a       embedded-package/ \
&& cp openssl-${OPENSSL_VERSION}/libcrypto.a embedded-package/

#build zstd
git clone https://github.com/facebook/zstd.git -b ${ZSTD_VERSION} zstd-${ZSTD_VERSION}
cd zstd-${ZSTD_VERSION}/build/cmake \
&& mkdir build \
&& cd build \
&& cmake .. \
&& make -j$(nproc) \
&& cd $original_dir

#copy zstd dependencies
cp zstd-${ZSTD_VERSION}/build/cmake/build/lib/libzstd.a embedded-package/

#build capnproto
git clone https://github.com/capnproto/capnproto.git -b ${CAPNPROTO_VERSION} capnproto-${CAPNPROTO_VERSION}
cd capnproto-${CAPNPROTO_VERSION} \
&& mkdir build \
&& cd build \
&& cmake .. \
&& make -j$(nproc) \
&& cd $original_dir

#copy capnproto dependencies
cp capnproto-${CAPNPROTO_VERSION}/build/c++/src/capnp/libcapnp.a embedded-package/ \
&& cp capnproto-${CAPNPROTO_VERSION}/build/c++/src/capnp/libcapnp-json.a embedded-package/ \
&& cp capnproto-${CAPNPROTO_VERSION}/build/c++/src/kj/libkj.a embedded-package/

#build lz4
git clone https://github.com/lz4/lz4.git -b ${LZ4_VERSION} lz4-${LZ4_VERSION}
cd lz4-${LZ4_VERSION}/build/cmake \
&& mkdir build \
&& cd build \
&& cmake -DBUILD_STATIC_LIBS=ON .. \
&& make -j$(nproc) \
&& cd $original_dir

#copy lz4 dependencies
cp lz4-${LZ4_VERSION}/build/cmake/build/liblz4.a embedded-package/

#build bz2
#bz2 ignores our CFLAGS - add fPIC to their flags in Makefile
git clone git://sourceware.org/git/bzip2.git -b ${BZ2_VERSION} bz2-${BZ2_VERSION}
cd bz2-${BZ2_VERSION} \
&& sed -i 's/^CFLAGS.*/& -fPIC/g' Makefile \
&& make -j$(nproc) \
&& cd $original_dir

#copy bz2 dependencies
cp bz2-${BZ2_VERSION}/libbz2.a embedded-package/

#build the libmariadbd-mytile-embedded.so shared library
cd embedded-package/ \
&& gcc -shared -o libmariadbd-mytile-embedded.so -Wl,--whole-archive,--allow-multiple-definition libmariadbd.a libsql.a libtiledb.a libcurl.a libssl.a libcrypto.a libz.a libzstd.a libcapnp.a libcapnp-json.a libkj.a liblz4.a libbz2.a libpcre.a -Wl,--no-whole-archive -ldl -lpthread -lcrypt \
&& mkdir -p rel/lib \
&& mkdir -p rel/include \
&& cp libmariadbd-mytile-embedded.so rel/lib \
&& cp -r $original_dir/mytile_server/include/* rel/include
