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
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
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

int tile::mytile_discover_table(handlerton *hton, THD *thd, TABLE_SHARE *ts) {

  std::stringstream sql_string;
  sql_string << "create table `" << ts->table_name.str << "` (";
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

      sql_string << std::endl
                 << "`" << dim.name() << "` "
                 << MysqlTypeString(TileDBTypeToMysqlType(dim.type()));

      if (TileDBTypeIsUnsigned(dim.type()))
        sql_string << " UNSIGNED";

      sql_string << " dimension=1"
                 << " lower_bound='" << lower_domain << "' upper_bound='"
                 << upper_domain << "' tile_extent='"
                 << dim.tile_extent_to_str() << "'"
                 << ", ";
    }

    for (const auto &attributeMap : schema.attributes()) {
      auto attribute = attributeMap.second;
      sql_string << std::endl << "`" << attribute.name() << "` ";

      auto type = TileDBTypeToMysqlType(attribute.type());
      if (type == MYSQL_TYPE_VARCHAR) {
        sql_string << "TEXT";
      } else {
        sql_string << MysqlTypeString(type);
      }

      if (TileDBTypeIsUnsigned(attribute.type()))
        sql_string << " UNSIGNED";
      sql_string << ",";
    }

    /*
     * TODO: primary key support caused rnd index to be required.. need to add
    in eventually
     * sql_string << "primary key(";

    for (const auto &dim : schema.domain().dimensions()) {
      sql_string << "`" << dim.name() << "`,";
    }
    // move head back one so we can override it

    sql_string << ")";*/
    sql_string.seekp(-1, std::ios_base::end);

    sql_string << std::endl << ") ENGINE=MyTile ";

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
