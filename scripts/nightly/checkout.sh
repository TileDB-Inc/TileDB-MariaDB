#!/bin/bash
set -eux

# Checkout Git repositories

OWNER_TILEDB_MARIADB=${OWNER_TILEDB_MARIADB-TileDB-Inc}
REF_LIBTILEDB=${REF_LIBTILEDB-dev}
REF_MARIADB=${REF_MARIADB-mariadb-10.5.13}
REF_TILEDB_MARIADB=${REF_TILEDB_MARIADB-HEAD}

git clone https://github.com/TileDB-Inc/TileDB.git libtiledb
git -C libtiledb checkout "${REF_LIBTILEDB}"

git clone https://github.com/MariaDB/server.git mariadb
git -C mariadb checkout "${REF_MARIADB}"

git clone https://github.com/${OWNER_TILEDB_MARIADB}/TileDB-MariaDB.git mariadb/storage/mytile
git -C mariadb/storage/mytile checkout "${REF_TILEDB_MARIADB}"
