# MyTile

[![Build Status](https://img.shields.io/azure-devops/build/tiledb-inc/836549eb-f74a-4986-a18f-7fbba6bbb5f0/8?label=Azure%20Pipelines&logo=azure-pipelines&style=flat-square)](https://dev.azure.com/TileDB-Inc/CI/_build/latest?definitionId=10&branchName=master)
[![tiledb-mariadb](https://img.shields.io/static/v1?label=Docker&message=tiledb-mariadb&color=099cec&logo=docker&style=flat-square)](https://hub.docker.com/r/tiledb/tiledb-mariadb)
[![tiledb-mariadb-server](https://img.shields.io/static/v1?label=Docker&message=tiledb-mariadb-server&color=099cec&logo=docker&style=flat-square)](https://hub.docker.com/r/tiledb/tiledb-mariadb-server)

MariaDB storage engine based on [TileDB](https://tiledb.com).

## Quickstart Usage

### Docker

Docker images are available on [Docker Hub](https://hub.docker.com/u/tiledb) for quick testing of the mytile storage engine.

#### Variants

- [`tiledb-mariadb`](https://hub.docker.com/r/tiledb/tiledb-mariadb) launches an interactive shell
- [`tiledb-mariadb-server`](https://hub.docker.com/r/tiledb/tiledb-mariadb-server) provides a more traditional (non-interactive) mariadb server

#### Supported tags

* `latest`: latest stable release (*recommended*)
* `dev`: development version
* `v0.x.x` for a specific version

#### Examples

The `tiledb-mariadb` image starts a mariadb server and connects to it from the shell for you.

```
docker run --rm -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -it tiledb/tiledb-mariadb
```

If you want to access arrays on s3, you will need to add your AWS keys as env variables

```
docker run --rm -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -e AWS_ACCESS_KEY_ID="<key>" -e AWS_SECRET_ACCESS_KEY="<secret>" -it tiledb/tiledb-mariadb
```

or mount a local array into the Docker container with the -v option:

```
docker run -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -it --rm -v /local/array/path:/data/local_array tiledb/tiledb-mariadb
```

### Example SQL Usage

TileDB Arrays can be access directly via their URI.

Show create table:

```
show create table `s3://my/array`\G
```

Basic select:

```
select * from `s3://my/array`;
```

Use assisted table discovery for long uris:

```
create table my_array ENGINE=mytile uri='s3://my_bucket/arrays/sub_prefix/path_is_longer_than_64_chars/my_array_1';
select * from my_array;
```

Create table:

```
CREATE TABLE `s3://bucket/regions`(
  regionkey bigint WITH (dimension=true),
  name varchar,
  comment varchar
  ) uri = 's3://bucket/region' array_type='SPARSE';
```

## Installation

### Docker

To build either docker image locally simply use the following:

```
git clone https://github.com/TileDB-Inc/TileDB-MariaDB.git
cd TileDB-MariaDB

# tiledb-mariadb
docker build -t mytile -f docker/Dockerfile .

# tiledb-mariadb-server
docker build -t mytile -f docker/Dockerfile-server .
```

### Requirements

Requires MariaDB 10.4.8 or newer.

### Inside MariaDB Source Tree

```bash
git clone git@github.com:MariaDB/server.git -b mariadb-10.5.12
cd server
git submodule add https://github.com/TileDB-Inc/TileDB-MariaDB.git storage/mytile
mkdir build && cd build
cmake ..
make -j4
```

#### Running Unit Test

Once MariaDB has been build you can run unit tests from the build directory:

```
./mysql-test/mtr --suite mytile
```

That will run all unit tests defined for mytile

## Features

- Based on TileDB arrays
- Supports basic pushdown of predicates for dimensions
- Create arrays through CREATE TABLE syntax.
- Existing arrays can be dynamically queried
- Supports all datatypes except geometry

## Known Issues

- Condition pushdown only works for constants not sub selects
- Buffers will double in size for incomplete queries with zero results
- MyTile is not capable of binlogging currently both stmt and row based is disabled at the storage engine level
