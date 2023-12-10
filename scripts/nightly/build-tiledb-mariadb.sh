#!/bin/bash
set -eux

# Build TileDB-MariaDB

OS=$(uname)
if [[ "$OS" == "Linux" ]]
then
  FLAGS_NEEDED="-Wno-error=deprecated-declarations -Wno-error=missing-braces"
  export LD_LIBRARY_PATH=/usr/local/lib:${LD_LIBRARY_PATH-}
  echo $LD_LIBRARY_PATH
elif [[ "$OS" == "Darwin" ]]
then
  echo 'test'
  ls -l /usr/local/lib;
  FLAGS_NEEDED="-Wno-overloaded-virtual -Wno-error=deprecated-non-prototype -Wno-error=inconsistent-missing-override -Wno-error=enum-conversion -Wno-error=deprecated-declarations -Wno-error=incompatible-pointer-types-discards-qualifiers -Wno-error=incompatible-function-pointer-types -Wno-error=writable-strings -Wno-writable-strings -Wno-write-strings -Wno-error -Wno-error=pointer-sign -Wno-error=all -Wno-error=unknown-warning-option -Wno-error=unused-but-set-variable -Wno-error=deprecated-copy-with-user-provided-copy"
  export PATH="$(brew --prefix bison@2.7)/bin:$PATH"
  export DYLD_LIBRARY_PATH=/usr/local/lib:${DYLD_LIBRARY_PATH-}
  echo $DYLD_LIBRARY_PATH
else
  echo "Unrecognized OS: $OS"
  exit 1
fi

export CXXFLAGS="${CXXFLAGS-} ${FLAGS_NEEDED}"
export CFLAGS="${CFLAGS-} ${FLAGS_NEEDED}"

cd mariadb
mkdir builddir
cd builddir
cmake \
  -DPLUGIN_OQGRAPH=NO \
  -DWITH_MARIABACKUP=OFF \
  -DPLUGIN_TOKUDB=NO \
  -DPLUGIN_ROCKSDB=NO \
  -DPLUGIN_MROONGA=NO \
  -DPLUGIN_SPIDER=NO \
  -DPLUGIN_SPHINX=NO \
  -DPLUGIN_FEDERATED=NO \
  -DPLUGIN_FEDERATEDX=NO \
  -DPLUGIN_CONNECT=NO \
  -DCMAKE_BUILD_TYPE=Debug \
  -DWITH_DEBUG=1 \
  -DCMAKE_INCLUDE_PATH="/usr/local/include" \
  -DTILEDB_FORCE_ALL_DEPS=${TILEDB_FORCE_ALL_DEPS-OFF} \
  ..
make -j2
./mysql-test/mysql-test-run.pl --suite=mytile --debug
