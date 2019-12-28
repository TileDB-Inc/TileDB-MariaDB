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
#include "mytile-sysvars.h"
#include "utils.h"
#include <log.h>
#include <my_global.h>
#include <table.h>
#include <tiledb/tiledb>

int tile::mytile_discover_table_structure(handlerton *hton, THD *thd,
                                          TABLE_SHARE *share,
                                          HA_CREATE_INFO *info) {
  DBUG_ENTER("tile::mytile_discover_table_structure");
  DBUG_RETURN(discover_array(thd, share, info));
}

int tile::mytile_discover_table(handlerton *hton, THD *thd, TABLE_SHARE *ts) {
  DBUG_ENTER("tile::mytile_discover_table");
  DBUG_RETURN(discover_array(thd, ts, nullptr));
}

int tile::discover_array(THD *thd, TABLE_SHARE *ts, HA_CREATE_INFO *info) {
  DBUG_ENTER("tile::discover_array");
  std::stringstream sql_string;
  tiledb::Config config = build_config(thd);
  tiledb::Context ctx = build_context(config);
  std::string array_uri;
  std::unique_ptr<tiledb::ArraySchema> schema;

  bool dimensions_are_primary_keys =
      tile::sysvars::dimensions_are_primary_keys(thd);

  std::string encryption_key;
  if (info != nullptr && info->option_struct != nullptr &&
      info->option_struct->encryption_key != nullptr) {
    encryption_key = std::string(info->option_struct->encryption_key);
  } else if (ts != nullptr && ts->option_struct != nullptr &&
             ts->option_struct->encryption_key != nullptr) {
    encryption_key = std::string(ts->option_struct->encryption_key);
  }

  tiledb::VFS vfs(ctx);

  bool array_found = false;
  try {
    // First try if the array_uri option is set
    if (info != nullptr && info->option_struct != nullptr &&
        info->option_struct->array_uri != nullptr) {
      array_uri = info->option_struct->array_uri;

      if (vfs.is_dir(array_uri)) {
        tiledb::Object obj = tiledb::Object::object(ctx, array_uri);
        if (obj.type() == tiledb::Object::Type::Array) {
          array_found = true;
        }
      }
      // Next try the table share if it is non-null
    } else if (ts != nullptr && ts->option_struct != nullptr &&
               ts->option_struct->array_uri != nullptr) {
      array_uri = ts->option_struct->array_uri;
      if (vfs.is_dir(array_uri)) {
        tiledb::Object obj = tiledb::Object::object(ctx, array_uri);
        if (obj.type() == tiledb::Object::Type::Array) {
          array_found = true;
        }
      }
      // Lastly try accessing the name directly, the name might be a uri
    } else if (ts != nullptr) {
      array_uri = ts->table_name.str;
      if (vfs.is_dir(array_uri)) {
        tiledb::Object obj = tiledb::Object::object(ctx, array_uri);
        if (obj.type() == tiledb::Object::Type::Array) {
          array_found = true;
        }
      }
      // If the name isn't a URI perhaps the array was created like a normal
      // table and the proper location is under <db>/<table_name> like normal
      // tables.
      array_uri = std::string(ts->db.str) + PATH_SEPARATOR + ts->table_name.str;
      if (vfs.is_dir(array_uri)) {
        tiledb::Object obj = tiledb::Object::object(ctx, array_uri);
        if (obj.type() == tiledb::Object::Type::Array) {
          array_found = true;
        }
      }
    }
  } catch (tiledb::TileDBError &e) {
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
  }

  // If we found the array, load the schema
  if (array_found) {
    schema = std::make_unique<tiledb::ArraySchema>(
        ctx, array_uri,
        encryption_key.empty() ? TILEDB_NO_ENCRYPTION : TILEDB_AES_256_GCM,
        encryption_key);
  } else {
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
  }

  // Catch incase we don't properly return above in the event the schema wasn't
  // actually opened
  if (schema == nullptr) {
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
  }

  try {
    // Now that we have the schema opened we need to build the create table
    // statement Its easier to build the create table query string than to try
    // to create the fmt data.
    std::stringstream table_options;

    sql_string << "create table `" << ts->table_name.str << "` (";

    table_options << "uri='" << array_uri << "'";

    if (schema->array_type() == tiledb_array_type_t::TILEDB_SPARSE) {
      table_options << " array_type='SPARSE'";
    } else {
      table_options << " array_type='DENSE'";
    }
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

    // Check for open_at
    ulonglong open_at = UINT64_MAX;
    if (info != nullptr && info->option_struct != nullptr) {
      open_at = info->option_struct->open_at;
    }
    if (open_at == UINT64_MAX && ts != nullptr &&
        ts->option_struct != nullptr) {
      open_at = ts->option_struct->open_at;
    }
    if (open_at != UINT64_MAX) {
      table_options << " open_at=" << open_at;
    }

    if (!encryption_key.empty()) {
      table_options << " encryption_key=" << encryption_key;
    }

    // Check for coordinate filters
    tiledb::FilterList coordinate_filters = schema->coords_filter_list();
    if (coordinate_filters.nfilters() > 0) {
      table_options << " coordinate_filters='"
                    << filter_list_to_str(coordinate_filters) << "'";
    }

    // Check for offset filters
    tiledb::FilterList offset_filters = schema->offsets_filter_list();
    if (offset_filters.nfilters() > 0) {
      table_options << " offset_filters='" << filter_list_to_str(offset_filters)
                    << "'";
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

      // Check for filters
      tiledb::FilterList filters = attribute.filter_list();
      if (filters.nfilters() > 0) {
        sql_string << " filters='" << filter_list_to_str(filters) << "'";
      }
      sql_string << ",";
    }

    // Add primary key indicator
    if (dimensions_are_primary_keys) {
      // In the future we might want to set index instead of primary key when
      // TileDB supports duplicates
      std::vector<std::string> index_types = {"PRIMARY KEY"};
      for (auto &index_type : index_types) {
        sql_string << std::endl << index_type << "(";
        for (const auto &dim : schema->domain().dimensions()) {
          sql_string << "`" << dim.name() << "`,";
        }
        // move head back one so we can override last command
        sql_string.seekp(-1, std::ios_base::end);

        sql_string << "),"; // Closing parentheses for key
      }

      // move head back one so we can override last command
      sql_string.seekp(-1, std::ios_base::end);
    } else {
      // move head back one so we can override last command
      sql_string.seekp(-1, std::ios_base::end);
    }

    sql_string << std::endl << ") ENGINE=MyTile ";

    sql_string << table_options.str();
  } catch (tiledb::TileDBError &e) {
    my_printf_error(ER_UNKNOWN_ERROR, "Error in table discovery: %s",
                    ME_ERROR_LOG, e.what());
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
  }

  std::string sql_statement = sql_string.str();
  int res = ts->init_from_sql_statement_string(
      thd, info != nullptr, sql_statement.c_str(), sql_statement.length());

  // discover_table should returns HA_ERR_NO_SUCH_TABLE for "not exists"
  DBUG_RETURN(res == ENOENT ? HA_ERR_NO_SUCH_TABLE : res);
}
