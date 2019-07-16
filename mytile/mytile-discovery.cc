/**
 * @file   mytile-discovery.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This implements the table discovery functions.
 */

#include "mytile-discovery.h"
#include "mytile.h"
#include "utils.h"
#include <log.h>
#include <my_global.h>
#include <mysqld_error.h>
#include <table.h>
#include <tiledb/tiledb>

tiledb_datatype_t tile::mysqlTypeToTileDBType(int type, bool signedInt) {
  switch (type) {

  case MYSQL_TYPE_DOUBLE:
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_NEWDECIMAL: {
    return tiledb_datatype_t::TILEDB_FLOAT64;
  }

  case MYSQL_TYPE_FLOAT: {
    return tiledb_datatype_t::TILEDB_FLOAT32;
  }

  case MYSQL_TYPE_TINY: {
    if (signedInt)
      return tiledb_datatype_t::TILEDB_INT8;
    return tiledb_datatype_t::TILEDB_UINT8;
  }

  case MYSQL_TYPE_SHORT: {
    if (signedInt)
      return tiledb_datatype_t::TILEDB_INT16;
    return tiledb_datatype_t::TILEDB_UINT16;
  }
  case MYSQL_TYPE_YEAR: {
    return tiledb_datatype_t::TILEDB_UINT16;
  }

  case MYSQL_TYPE_INT24: {
    if (signedInt)
      return tiledb_datatype_t::TILEDB_INT32;
    return tiledb_datatype_t::TILEDB_UINT32;
  }

  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_LONGLONG: {
    if (signedInt)
      return tiledb_datatype_t::TILEDB_INT64;
    return tiledb_datatype_t::TILEDB_UINT64;
  }

  case MYSQL_TYPE_NULL:
  case MYSQL_TYPE_BIT: {
    return tiledb_datatype_t::TILEDB_UINT8;
  }

  case MYSQL_TYPE_VARCHAR:
  case MYSQL_TYPE_STRING:
  case MYSQL_TYPE_VAR_STRING:
  case MYSQL_TYPE_SET: {
    return tiledb_datatype_t::TILEDB_CHAR;
  }

  case MYSQL_TYPE_GEOMETRY:
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_ENUM: {
    return tiledb_datatype_t::TILEDB_UINT8;
  }

  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_NEWDATE:
    return tiledb_datatype_t::TILEDB_DATETIME_DAY;

  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_DATETIME2:
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_TIMESTAMP2:
    return tiledb_datatype_t::TILEDB_DATETIME_NS;

  case MYSQL_TYPE_TIME:
  case MYSQL_TYPE_TIME2:
    return tiledb_datatype_t::TILEDB_DATETIME_NS;
  default: {
    sql_print_error("Unknown mysql data type in determining tiledb type");
  }
    return tiledb_datatype_t::TILEDB_ANY;
  }
}

std::string tile::MysqlTypeString(int type) {
  switch (type) {

  case MYSQL_TYPE_DOUBLE:
    return "DOUBLE";
  case MYSQL_TYPE_DECIMAL:
    return "DECIMAL";
  case MYSQL_TYPE_NEWDECIMAL:
    return "DECIMAL";
  case MYSQL_TYPE_FLOAT:
    return "FLOAT";
  case MYSQL_TYPE_TINY:
    return "TINY";
  case MYSQL_TYPE_SHORT:
    return "SHORT";
  case MYSQL_TYPE_YEAR:
    return "YEAR";
  case MYSQL_TYPE_INT24:
    return "MEDIUMINT";
  case MYSQL_TYPE_LONG:
    return "INTEGER";
  case MYSQL_TYPE_LONGLONG:
    return "BIGINT";
  case MYSQL_TYPE_NULL:
    return "NULL";
  case MYSQL_TYPE_BIT:
    return "BIT";
  case MYSQL_TYPE_VARCHAR:
    return "VARCHAR";
  case MYSQL_TYPE_STRING:
    return "TEXT";
  case MYSQL_TYPE_VAR_STRING:
    return "VARCHAR";
  case MYSQL_TYPE_SET:
    return "SET";

  case MYSQL_TYPE_GEOMETRY:
    return "GEOMETRY";
  case MYSQL_TYPE_BLOB:
    return "BLOB";
  case MYSQL_TYPE_LONG_BLOB:
    return "LONGBLOB";
  case MYSQL_TYPE_MEDIUM_BLOB:
    return "MEDIUMBLOB";
  case MYSQL_TYPE_TINY_BLOB:
    return "TINYBLOB";
  case MYSQL_TYPE_ENUM:
    return "ENUM";

  case MYSQL_TYPE_DATE:
    return "DATE";

  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_DATETIME2:
    return "DATETIME";

  case MYSQL_TYPE_TIME:
  case MYSQL_TYPE_TIME2:
    return "TIME";
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_TIMESTAMP2:
    return "TIMESTAMP";
  case MYSQL_TYPE_NEWDATE:
    return "DATETIME";
  default: {
    sql_print_error("Unknown mysql data type in determining string");
  }
    return "";
  }
}

int tile::TileDBTypeToMysqlType(tiledb_datatype_t type) {
  switch (type) {

  case tiledb_datatype_t::TILEDB_FLOAT64: {
    return MYSQL_TYPE_DOUBLE;
  }

  case tiledb_datatype_t::TILEDB_FLOAT32: {
    return MYSQL_TYPE_FLOAT;
  }

  case tiledb_datatype_t::TILEDB_INT8:
  case tiledb_datatype_t::TILEDB_UINT8: {
    return MYSQL_TYPE_TINY;
  }

  case tiledb_datatype_t::TILEDB_INT16:
  case tiledb_datatype_t::TILEDB_UINT16: {
    return MYSQL_TYPE_SHORT;
  }
  case tiledb_datatype_t::TILEDB_INT32:
  case tiledb_datatype_t::TILEDB_UINT32: {
    return MYSQL_TYPE_INT24;
  }

  case tiledb_datatype_t::TILEDB_INT64:
  case tiledb_datatype_t::TILEDB_UINT64: {
    return MYSQL_TYPE_LONGLONG;
  }

  case tiledb_datatype_t::TILEDB_CHAR:
  case tiledb_datatype_t::TILEDB_STRING_ASCII: {
    return MYSQL_TYPE_VARCHAR;
  }
  case tiledb_datatype_t::TILEDB_STRING_UTF8:
  case tiledb_datatype_t::TILEDB_STRING_UTF16:
  case tiledb_datatype_t::TILEDB_STRING_UTF32:
  case tiledb_datatype_t::TILEDB_STRING_UCS2:
  case tiledb_datatype_t::TILEDB_STRING_UCS4: {
    return MYSQL_TYPE_STRING;
  }

  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
    return MYSQL_TYPE_NEWDATE;

  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
    return MYSQL_TYPE_YEAR;

  case tiledb_datatype_t::TILEDB_DATETIME_NS:
    return MYSQL_TYPE_TIMESTAMP2;

  default: {
    sql_print_error("Unknown tiledb data type in determining mysql type");
  }
    return tiledb_datatype_t::TILEDB_ANY;
  }
}

int tile::mytile_discover_table(handlerton *hton, THD *thd, TABLE_SHARE *ts) {

  std::stringstream sql_string;
  sql_string << "create table `" << ts->table_name.str << "` (" << std::endl;
  try {
    tiledb::Context ctx;
    tiledb::ArraySchema schema(ctx, ts->table_name.str);
    std::stringstream table_options;

    table_options << "uri='" << ts->table_name.str << "'";

    if (schema.array_type() == tiledb_array_type_t::TILEDB_SPARSE) {
      table_options << " array_type='SPARSE'";
    } else {
      table_options << " array_type='DENSE'";
    }
    // table_options << "uri='" <<
    if (schema.array_type() == tiledb_array_type_t::TILEDB_SPARSE) {
      table_options << " capacity=" << schema.capacity();
    }

    if (schema.cell_order() == tiledb_layout_t::TILEDB_ROW_MAJOR) {
      table_options << " cell_order=ROW_MAJOR";
    } else if (schema.cell_order() == tiledb_layout_t::TILEDB_COL_MAJOR) {
      table_options << " cell_order=COL_MAJOR";
    } else {
      const char *layout;
      tiledb_layout_to_str(schema.cell_order(), &layout);
      throw std::runtime_error(
          std::string("Unknown or Unsupported cell order %s") + layout);
    }

    if (schema.tile_order() == tiledb_layout_t::TILEDB_ROW_MAJOR) {
      table_options << " tile_order=ROW_MAJOR";
    } else if (schema.tile_order() == tiledb_layout_t::TILEDB_COL_MAJOR) {
      table_options << " tile_order=COL_MAJOR";
    } else {
      const char *layout;
      tiledb_layout_to_str(schema.tile_order(), &layout);
      throw std::runtime_error(
          std::string("Unknown or Unsupported cell order %s") + layout);
    }

    for (const auto &dim : schema.domain().dimensions()) {
      std::string domain_str = dim.domain_to_str();
      domain_str = domain_str.substr(1, domain_str.size() - 2);
      auto domainSplitPosition = domain_str.find(',');

      std::string lower_domain = domain_str.substr(0, domainSplitPosition);
      std::string upper_domain = domain_str.substr(domainSplitPosition + 1);

      trim(lower_domain);
      trim(upper_domain);

      sql_string << "`" << dim.name() << "` "
                 << MysqlTypeString(TileDBTypeToMysqlType(dim.type()))
                 << " dimension=1"
                 << " lower_bound='" << lower_domain << "' upper_bound='"
                 << upper_domain << "' tile_extent='"
                 << dim.tile_extent_to_str() << "'"
                 << ", " << std::endl;
    }

    for (const auto &attributeMap : schema.attributes()) {
      auto attribute = attributeMap.second;
      sql_string << "`" << attribute.name() << "` "
                 << MysqlTypeString(TileDBTypeToMysqlType(attribute.type()))
                 << ", " << std::endl;
    }

    sql_string << "primary key(";

    for (const auto &dim : schema.domain().dimensions()) {
      sql_string << "`" << dim.name() << "`,";
    }
    // move head back one so we can override it
    sql_string.seekp(-1, std::ios_base::end);

    sql_string << ")" << std::endl << ") ENGINE=MyTile ";

    sql_string << table_options.str();
  } catch (tiledb::TileDBError &e) {
    my_printf_error(ER_UNKNOWN_ERROR, e.what(), ME_ERROR_LOG | ME_FATAL);
    return HA_ERR_NO_SUCH_TABLE;
  }

  std::string sql_statement = sql_string.str();
  int res = ts->init_from_sql_statement_string(
      thd, false, sql_statement.c_str(), sql_statement.length());

  // discover_table should returns HA_ERR_NO_SUCH_TABLE for "not exists"
  return res == ENOENT ? HA_ERR_NO_SUCH_TABLE : res;
}

int tile::mytile_discover_table_existence(handlerton *hton, const char *db,
                                          const char *name) {

  try {
    tiledb::Context ctx;
    tiledb::ArraySchema schema(ctx, name);
  } catch (tiledb::TileDBError &e) {
    return false;
  }

  return true;
}
