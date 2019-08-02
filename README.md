# MyTile

MariaDB storage engine based on [TileDB](https://tiledb.io/)

## Requirements

Requires MariaDB 10.4 or newer. It is untested on older versions.

## Installation

### Inside MariaDB Source Tree

```bash
git clone git@github.com:MariaDB/server.git -b 10.4
cd server
git submodule add https://github.com/TileDB-Inc/TileDB-MariaDB-Storage-Engine.git storage/mytile
mkdir build && cd build
cmake ..
make -j4
```

## Features

- Based on TileDB arrays
- Supports basic pushdown of predicates for dimensions
- Create arrays through CREATE TABLE syntax.

## Known Issues

- Writes are currently not supported
- Condition pushdown only works for constants not sub selects
- Buffers will double in size for incomplete queries with zero results
- Array URIs must be less than 64 characters

## Usage

TileDB Arrays can be access directly via their URI.

Show create table:

```
show create table `s3://my/array`\G
```

Basic select:

```
select * from `s3://my/array`;
```

Create table:

```
CREATE TABLE `s3://bucket/regions`(
  regionkey bigint WITH (dimension=true),
  name varchar,
  comment varchar
  ) uri = 's3://bucket/region' array_type='SPARSE';
```

