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
#include <table.h>
#include <tiledb/tiledb>

int tile::mytile_discover_table_structure(handlerton *hton, THD *thd,
                                          TABLE_SHARE *share,
                                          HA_CREATE_INFO *info) {
  DBUG_ENTER("tile::mytile_discover_table_structure");
  DBUG_RETURN(discover_array(hton, thd, share, info));
}

int tile::mytile_discover_table(handlerton *hton, THD *thd, TABLE_SHARE *ts) {
  DBUG_ENTER("tile::mytile_discover_table");
  DBUG_RETURN(discover_array(hton, thd, ts, nullptr));
}

int tile::discover_array(handlerton *hton, THD *thd, TABLE_SHARE *ts,
                         HA_CREATE_INFO *info) {
  DBUG_ENTER("tile::discover_array");
  std::stringstream sql_string;
  tiledb::Context ctx;
  std::string array_uri;
  std::unique_ptr<tiledb::ArraySchema> schema;

  // First try if the array_uri option is set
  if (info != nullptr && info->option_struct != nullptr &&
      info->option_struct->array_uri != nullptr) {
    try {
      array_uri = info->option_struct->array_uri;
      schema = std::make_unique<tiledb::ArraySchema>(ctx, array_uri);
    } catch (tiledb::TileDBError &e) {
      DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
    }
  } else if (ts != nullptr && ts->option_struct != nullptr &&
             ts->option_struct->array_uri != nullptr) {
    try {
      array_uri = ts->option_struct->array_uri;
      schema = std::make_unique<tiledb::ArraySchema>(ctx, array_uri);
    } catch (tiledb::TileDBError &e) {
      DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
    }
  } else {
    try {
      array_uri = ts->table_name.str;
      schema = std::make_unique<tiledb::ArraySchema>(ctx, array_uri);
    } catch (tiledb::TileDBError &e) {
      try {
        array_uri =
            std::string(ts->db.str) + PATH_SEPARATOR + ts->table_name.str;
        schema = std::make_unique<tiledb::ArraySchema>(ctx, array_uri);
      } catch (tiledb::TileDBError &e) {
        DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
      }
    }
  }

  if (schema == nullptr) {
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
  }

  try {
    std::stringstream table_options;

    sql_string << "create table `" << ts->table_name.str << "` (";

    table_options << "uri='" << array_uri << "'";

    if (schema->array_type() == tiledb_array_type_t::TILEDB_SPARSE) {
      table_options << " array_type='SPARSE'";
    } else {
      table_options << " array_type='DENSE'";
    }
    // table_options << "uri='" <<
    if (schema->array_type() == tiledb_array_type_t::TILEDB_SPARSE) {
      table_options << " capacity=" << schema->capacity();
    }

    if (schema->cell_order() == tiledb_layout_t::TILEDB_ROW_MAJOR) {
      table_options << " cell_order=ROW_MAJOR";
    } else if (schema->cell_order() == tiledb_layout_t::TILEDB_COL_MAJOR) {
      table_options << " cell_order=COL_MAJOR";
    } else {
      const char *layout;
      tiledb_layout_to_str(schema->cell_order(), &layout);
      throw std::runtime_error(
          std::string("Unknown or Unsupported cell order %s") + layout);
    }

    if (schema->tile_order() == tiledb_layout_t::TILEDB_ROW_MAJOR) {
      table_options << " tile_order=ROW_MAJOR";
    } else if (schema->tile_order() == tiledb_layout_t::TILEDB_COL_MAJOR) {
      table_options << " tile_order=COL_MAJOR";
    } else {
      const char *layout;
      tiledb_layout_to_str(schema->tile_order(), &layout);
      throw std::runtime_error(
          std::string("Unknown or Unsupported cell order %s") + layout);
    }

    for (const auto &dim : schema->domain().dimensions()) {
      std::string domain_str = dim.domain_to_str();
      domain_str = domain_str.substr(1, domain_str.size() - 2);
      auto domainSplitPosition = domain_str.find(',');

      std::string lower_domain = domain_str.substr(0, domainSplitPosition);
      std::string upper_domain = domain_str.substr(domainSplitPosition + 1);

      trim(lower_domain);
      trim(upper_domain);

      int mysql_type = TileDBTypeToMysqlType(dim.type(), false);
      sql_string << std::endl
                 << "`" << dim.name() << "` " << MysqlTypeString(mysql_type);

      if (!MysqlBlobType(enum_field_types(mysql_type)) &&
          TileDBTypeIsUnsigned(dim.type()))
        sql_string << " UNSIGNED";

      sql_string << " dimension=1"
                 << " lower_bound='" << lower_domain << "' upper_bound='"
                 << upper_domain << "' tile_extent='"
                 << dim.tile_extent_to_str() << "'"
                 << ",";
    }

    for (const auto &attributeMap : schema->attributes()) {
      auto attribute = attributeMap.second;
      sql_string << std::endl << "`" << attribute.name() << "` ";

      auto mysql_type =
          TileDBTypeToMysqlType(attribute.type(), attribute.cell_size() > 1);
      if (mysql_type == MYSQL_TYPE_VARCHAR) {
        sql_string << "TEXT";
      } else {
        sql_string << MysqlTypeString(mysql_type);
      }

      if (!MysqlBlobType(enum_field_types(mysql_type)) &&
          TileDBTypeIsUnsigned(attribute.type()))
        sql_string << " UNSIGNED";
      sql_string << ",";
    }

    /*
     * TODO: primary key support caused rnd index to be required.. need to add
    in eventually
     * sql_string << "primary key(";

    for (const auto &dim : schema->domain().dimensions()) {
      sql_string << "`" << dim.name() << "`,";
    }
    // move head back one so we can override it

    sql_string << ")";*/
    sql_string.seekp(-1, std::ios_base::end);

    sql_string << std::endl << ") ENGINE=MyTile ";

    sql_string << table_options.str();
  } catch (tiledb::TileDBError &e) {
    my_printf_error(ER_UNKNOWN_ERROR, "Error in table discovery: %s",
                    ME_ERROR_LOG, e.what());
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
  }

  std::string sql_statement = sql_string.str();
  int res = ts->init_from_sql_statement_string(
      thd, info == nullptr ? false : true, sql_statement.c_str(),
      sql_statement.length());

  // discover_table should returns HA_ERR_NO_SUCH_TABLE for "not exists"
  DBUG_RETURN(res == ENOENT ? HA_ERR_NO_SUCH_TABLE : res);
}
