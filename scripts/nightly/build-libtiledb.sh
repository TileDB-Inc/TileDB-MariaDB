#!/bin/bash
set -eux

# Build libtiledb from source

cd libtiledb
mkdir -p build
cd build
cmake \
  -DTILEDB_VERBOSE=ON \
  -DTILEDB_S3=ON \
  -DTILEDB_SERIALIZATION=ON \
  -DCMAKE_BUILD_TYPE=Debug \
  ..
make -j2
sudo make -C tiledb install
