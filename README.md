# MyTile

MariaDB storage engine based on [TileDB](https://tiledb.io/)

## Quickstart Usage

### Docker Image

A docker image is provided to allow for quick testing of the mytile storage engine.  The docker image starts a mariadb
server and connects to it from the shell for you.

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

To build the default docker image simply use the following:

```
git clone https://github.com/TileDB-Inc/TileDB-MariaDB.git
cd TileDB-MariaDB
docker build -t mytile -f docker/Dockerfile .
```

If you would prefer to build a more traditional mariadb which launches the
server and is not interactive use the Dockerfile-server

```
git clone https://github.com/TileDB-Inc/TileDB-MariaDB.git
cd TileDB-MariaDB
docker build -t mytile -f docker/Dockerfile-server .
```

### Requirements

Requires MariaDB 10.4.8 or newer.

### Inside MariaDB Source Tree

```bash
git clone git@github.com:MariaDB/server.git -b 10.4
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

## Parameters

There are three parameters currently supported for MyTile.

| Paramater | Scope | Data Type | Default Value | Description |
| --------- | ----- | --------- | ------------- | ----------- |
| read_buffer_size | global or session | integer | 100M | Read buffer size for tiledb query attribute/coordinates |
| write_buffer_size | global or session | integer | 100M | Write buffer size for tiledb query attribute/coordinates |
| delete_arrays | global or session | boolean | False | Controls if a `delete table` statement causes the array to be deleted on disk or just deregistered from mariadb. True value causes actual deletions of data |
| tiledb_config* | global or session | string | "" | Set TileDB configuration parameters, this is in csv form of `key1=value1,key2=value2`. Example: `set mytile_tiledb_config = 'vfs.s3.aws_access_key_id=abc,vfs.s3.aws_secret_access_key=123';` |
| reopen_for_every_query | global or session | boolean | True | Closes and reopen the array for every query, this allows tiledb_config parameters to take effect without forcing a table flush but any tiledb caching is removed. |
| ready_query_layout | global or session | string | "unordered" | set layout for ready queries, valid values are "row-major", "col-major", "unordered" or "global-order" |

\* If reopen_for_every_query is disabled you must `FLUSH TABLES` before any new parameters set on `tiledb_config`
will take effect on an open array (recently access arrays are not closed by MariaDB immediately).

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
- Specifying filters in create table is not supported, all filters default to zstd
