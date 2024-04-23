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
  std::pair<std::string, uint64_t> array_uri_timestamp("", UINT64_MAX);
  std::unique_ptr<tiledb::ArraySchema> schema;

  bool dimensions_are_keys = tile::sysvars::dimensions_are_keys(thd);

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
  bool metadata_query = false;
  try {
    // First try if the array_uri option is set
    if (info != nullptr && info->option_struct != nullptr &&
        info->option_struct->array_uri != nullptr) {
      array_uri_timestamp =
          get_real_uri_and_timestamp(info->option_struct->array_uri);

      // Check if @metadata is ending of uri, if so the user is trying to query
      // the metadata we need to remove the keyword for checking if the array
      // exists
      if (tile::has_ending(array_uri_timestamp.first, METADATA_ENDING)) {
        array_uri_timestamp.first = array_uri_timestamp.first.substr(
            0, array_uri_timestamp.first.length() - METADATA_ENDING.length());
        metadata_query = true;
      } else {
      }

      array_found = check_array_exists(vfs, ctx, array_uri_timestamp.first,
                                       encryption_key, schema);
      // Next try the table share if it is non-null
    } else if (ts != nullptr && ts->option_struct != nullptr &&
               ts->option_struct->array_uri != nullptr) {
      array_uri_timestamp =
          get_real_uri_and_timestamp(ts->option_struct->array_uri);

      // Check if @metadata is ending of uri, if so the user is trying to query
      // the metadata we need to remove the keyword for checking if the array
      // exists
      if (tile::has_ending(array_uri_timestamp.first, METADATA_ENDING)) {
        array_uri_timestamp.first = array_uri_timestamp.first.substr(
            0, array_uri_timestamp.first.length() - METADATA_ENDING.length());
        metadata_query = true;
      }

      array_found = check_array_exists(vfs, ctx, array_uri_timestamp.first,
                                       encryption_key, schema);
      // Lastly try accessing the name directly, the name might be a uri
    } else if (ts != nullptr) {
      array_uri_timestamp = get_real_uri_and_timestamp(ts->table_name.str);

      // Check if @metadata is ending of uri, if so the user is trying to query
      // the metadata we need to remove the keyword for checking if the array
      // exists
      if (tile::has_ending(array_uri_timestamp.first, METADATA_ENDING)) {
        array_uri_timestamp.first = array_uri_timestamp.first.substr(
            0, array_uri_timestamp.first.length() - METADATA_ENDING.length());
        metadata_query = true;
      }

      array_found = check_array_exists(vfs, ctx, array_uri_timestamp.first,
                                       encryption_key, schema);

      if (!array_found) {
        // If the name isn't a URI perhaps the array was created like a normal
        // table and the proper location is under <db>/<table_name> like normal
        // tables.
        // We don't check for metadata here because in this case it isn't
        // supported, since the user is accessing by table name not uri they
        // can't give us the metadata keyword
        std::string array_uri =
            std::string(ts->db.str) + PATH_SEPARATOR + ts->table_name.str;
        array_uri_timestamp = get_real_uri_and_timestamp(array_uri);
        array_found =
            check_array_exists(vfs, ctx, array_uri, encryption_key, schema);
      }
    }
  } catch (tiledb::TileDBError &e) {
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
  }

  // If we found the array, load the schema
  if (array_found) {
    if (schema == nullptr) {
      schema = std::make_unique<tiledb::ArraySchema>(
          ctx, array_uri_timestamp.first,
          encryption_key.empty() ? TILEDB_NO_ENCRYPTION : TILEDB_AES_256_GCM,
          encryption_key);
    }
  } else {
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
  }

  // Catch incase we don't properly return above in the event the schema wasn't
  // actually opened
  if (schema == nullptr) {
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
  }

  if (metadata_query) {
    DBUG_RETURN(discover_array_metadata(thd, ts, info,
                                        array_uri_timestamp.first,
                                        std::move(schema), encryption_key));
  }

  try {
    // Now that we have the schema opened we need to build the create table
    // statement Its easier to build the create table query string than to try
    // to create the fmt data.
    std::stringstream table_options;

    sql_string << "create table `" << ts->table_name.str << "` (";

    table_options << "uri='" << array_uri_timestamp.first << "'";

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
      table_options << " cell_order=COLUMN_MAJOR";
    } else if (schema->cell_order() == tiledb_layout_t::TILEDB_HILBERT) {
      table_options << " cell_order=HILBERT";
    } else {
      const char *layout;
      tiledb_layout_to_str(schema->cell_order(), &layout);
      throw tiledb::TileDBError(
          std::string("Unknown or Unsupported cell order %s") + layout);
    }

    // If the cell order is hilbert than we ignore tile_order
    if (schema->cell_order() != tiledb_layout_t::TILEDB_HILBERT) {
      if (schema->tile_order() == tiledb_layout_t::TILEDB_ROW_MAJOR) {
        table_options << " tile_order=ROW_MAJOR";
      } else if (schema->tile_order() == tiledb_layout_t::TILEDB_COL_MAJOR) {
        table_options << " tile_order=COLUMN_MAJOR";
      } else {
        const char *layout;
        tiledb_layout_to_str(schema->tile_order(), &layout);
        throw tiledb::TileDBError(
            std::string("Unknown or Unsupported cell order %s") + layout);
      }
    }

    // Check for open_at
    if (array_uri_timestamp.second == UINT64_MAX && info != nullptr &&
        info->option_struct != nullptr) {
      array_uri_timestamp.second = info->option_struct->open_at;
    }
    if (array_uri_timestamp.second == UINT64_MAX && ts != nullptr &&
        ts->option_struct != nullptr) {
      array_uri_timestamp.second = ts->option_struct->open_at;
    }
    if (array_uri_timestamp.second != UINT64_MAX) {
      table_options << " open_at=" << array_uri_timestamp.second;
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

    // Check for validity filters
    tiledb::FilterList validity_filters = schema->validity_filter_list();
    if (validity_filters.nfilters() > 0) {
      table_options << " validity_filters='"
                    << filter_list_to_str(validity_filters) << "'";
    }

    for (const auto &dim : schema->domain().dimensions()) {
      int mysql_type =
          TileDBTypeToMysqlType(dim.type(), false, dim.cell_val_num());

      sql_string << std::endl
                 << "`" << dim.name() << "` " << MysqlTypeString(mysql_type);

      if (!MysqlBlobType(enum_field_types(mysql_type)) &&
          TileDBTypeIsUnsigned(dim.type()))
        sql_string << " UNSIGNED";

      // nullable dimensions not allowed
      sql_string << " NOT NULL";

      // Only set the domain and tile extent for non string dimensions
      if (dim.type() != TILEDB_STRING_ASCII) {
        std::string domain_str = dim.domain_to_str();
        domain_str = domain_str.substr(1, domain_str.size() - 2);
        auto domainSplitPosition = domain_str.find(',');

        std::string lower_domain = domain_str.substr(0, domainSplitPosition);
        std::string upper_domain = domain_str.substr(domainSplitPosition + 1);

        trim(lower_domain);
        trim(upper_domain);

        sql_string << " dimension=1"
                   << " lower_bound='" << lower_domain << "' upper_bound='"
                   << upper_domain << "' tile_extent='"
                   << dim.tile_extent_to_str() << "'"
                   << ",";
      } else {
        sql_string << " dimension=1"
                   << ",";
      }
    }
    auto array = tiledb::Array(ctx, array_uri_timestamp.first, TILEDB_READ,
                               encryption_key.empty() ? TILEDB_NO_ENCRYPTION
                                                      : TILEDB_AES_256_GCM,
                               encryption_key);

    for (const auto &attributeMap : schema->attributes()) {
      auto attribute = attributeMap.second;
      sql_string << std::endl << "`" << attribute.name() << "` ";

      auto enmr_name =
          tiledb::AttributeExperimental::get_enumeration_name(ctx, attribute);
      bool is_enum = enmr_name.has_value();

      auto mysql_type =
          TileDBTypeToMysqlType(attribute.type(), attribute.cell_size() > 1,
                                attribute.cell_val_num());

      if (is_enum) {
        // if the attribute has an enum
        auto enmr = tiledb::ArrayExperimental::get_enumeration(
            ctx, array, enmr_name.value());

        auto enum_vec_string = enmr.as_vector<std::string>();

        if (enum_vec_string.size() != 0) {
          sql_string << "ENUM"
                     << "(";
          for (size_t i = 0; i < enum_vec_string.size(); ++i) {
            sql_string << "'" << enum_vec_string[i] << "'";
            if (i < enum_vec_string.size() - 1) {
              sql_string << ", ";
            }
          }
          sql_string << ")";
        } else {
          sql_string << MysqlTypeString(mysql_type);
        }
      } else {
        if (mysql_type == MYSQL_TYPE_VARCHAR) {
          sql_string << "TEXT";
        } else {
          sql_string << MysqlTypeString(mysql_type);
        }
      }
      if (!MysqlBlobType(enum_field_types(mysql_type)) &&
          TileDBTypeIsUnsigned(attribute.type()))
        sql_string << " UNSIGNED";

      uint8_t nullable = 0;
      tiledb_attribute_get_nullable(ctx.ptr().get(), attribute.ptr().get(),
                                    &nullable);

      if (nullable == 0) {
        sql_string << " NOT NULL ";
      } else {
        sql_string << " NULL ";
      }

      const void *default_value = nullptr;
      uint64_t default_value_size = tiledb_datatype_size(attribute.type());
      uint8_t valid = 1;
      if (attribute.nullable()) {
        attribute.get_fill_value(&default_value, &default_value_size, &valid);
      } else {
        attribute.get_fill_value(&default_value, &default_value_size);
      }

      if (!is_enum) {
        if (valid == 0) {
          sql_string << " DEFAULT NULL";
        } else {
          // Only set the default value if its set by the user or we include
          // defaults for non-string type. TileDB strings uses default fill
          // values that mariadb doesn't like (i.e. \200) so we just avoid
          // setting those.
          if (!tile::is_fill_value_default(attribute.type(), default_value,
                                           default_value_size) ||
              !tile::is_string_datatype(attribute.type())) {
            auto default_value_str = TileDBTypeValueToString(
                attribute.type(), default_value, default_value_size);

            if (!default_value_str.empty())
              sql_string << " DEFAULT " << default_value_str;
          }
        }
      }

      // Check for filters
      tiledb::FilterList filters = attribute.filter_list();
      if (filters.nfilters() > 0) {
        sql_string << " filters='" << filter_list_to_str(filters) << "'";
      }
      sql_string << ",";
    }

    // Add primary key indicator
    if (dimensions_are_keys) {
      // In the future we might want to set index instead of primary key when
      // TileDB supports duplicates
      std::vector<std::string> index_types = {"PRIMARY KEY"};
      if (schema->allows_dups()) {
        index_types = {"INDEX"};
      }
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

int tile::discover_array_metadata(THD *thd, TABLE_SHARE *ts,
                                  HA_CREATE_INFO *info,
                                  const std::string &array_uri,
                                  std::unique_ptr<tiledb::ArraySchema> schema,
                                  const std::string &encryption_key) {
  DBUG_ENTER("tile::discover_array_metadata");
  std::stringstream sql_string;
  try {
    // Now that we have the schema opened we need to build the create table
    // statement Its easier to build the create table query string than to try
    // to create the fmt data.
    std::stringstream table_options;

    sql_string << "create table `" << ts->table_name.str << "` (";

    table_options << "uri='" << array_uri << METADATA_ENDING << "'";

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
      table_options << " cell_order=COLUMN_MAJOR";
    } else if (schema->cell_order() == tiledb_layout_t::TILEDB_HILBERT) {
      table_options << " cell_order=HILBERT";
    } else {
      const char *layout;
      tiledb_layout_to_str(schema->cell_order(), &layout);
      throw tiledb::TileDBError(
          std::string("Unknown or Unsupported cell order %s") + layout);
    }

    // If the cell order is hilbert than we ignore tile_order
    if (schema->cell_order() != tiledb_layout_t::TILEDB_HILBERT) {
      if (schema->tile_order() == tiledb_layout_t::TILEDB_ROW_MAJOR) {
        table_options << " tile_order=ROW_MAJOR";
      } else if (schema->tile_order() == tiledb_layout_t::TILEDB_COL_MAJOR) {
        table_options << " tile_order=COLUMN_MAJOR";
      } else {
        const char *layout;
        tiledb_layout_to_str(schema->tile_order(), &layout);
        throw tiledb::TileDBError(
            std::string("Unknown or Unsupported cell order %s") + layout);
      }
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

    // Check for validity filters
    tiledb::FilterList validity_filters = schema->validity_filter_list();
    if (validity_filters.nfilters() > 0) {
      table_options << " validity_filters='"
                    << filter_list_to_str(validity_filters) << "'";
    }

    sql_string << "`key` varchar(8000)," << std::endl;
    sql_string << "`value` longtext" << std::endl;
    sql_string << ") ENGINE=MyTile ";
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

bool tile::check_array_exists(
    const tiledb::VFS &vfs, const tiledb::Context &ctx,
    const std::string &array_uri, const std::string &encryption_key,
    std::unique_ptr<tiledb::ArraySchema> &array_schema) {

  if (tile::has_prefix(array_uri, "tiledb://")) {
    return check_cloud_array_exists(ctx, array_uri, encryption_key,
                                    array_schema);
  }

  if (vfs.is_dir(array_uri)) {
    tiledb::Object obj = tiledb::Object::object(ctx, array_uri);
    if (obj.type() == tiledb::Object::Type::Array) {
      return true;
    }
  }

  return false;
}

bool tile::check_cloud_array_exists(
    const tiledb::Context &ctx, const std::string &array_uri,
    const std::string &encryption_key,
    std::unique_ptr<tiledb::ArraySchema> &array_schema) {
  try {
    // check to see if we can load the schema
    array_schema = std::make_unique<tiledb::ArraySchema>(
        ctx, array_uri,
        encryption_key.empty() ? TILEDB_NO_ENCRYPTION : TILEDB_AES_256_GCM,
        encryption_key);
    return true;
  } catch (tiledb::TileDBError &e) {
    return false;
  }

  return true;
}
