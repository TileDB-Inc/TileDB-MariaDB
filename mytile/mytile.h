/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#pragma once

#include "my_global.h" /* ulonglong */
#include <field.h>
#include <mysqld_error.h> /* ER_UNKNOWN_ERROR */
#include <tiledb/tiledb>

struct ha_table_option_struct {
  const char *array_uri;
  ulonglong capacity;
  uint array_type;
  ulonglong cell_order;
  ulonglong tile_order;
};

struct ha_field_option_struct {
  bool dimension;
  const char *lower_bound;
  const char *upper_bound;
  const char *tile_extent;
};

namespace tile {
typedef struct ::ha_table_option_struct ha_table_option_struct;
typedef struct ::ha_field_option_struct ha_field_option_struct;

/**
 * Converts a mysql type to a tiledb_datatype_t
 * @param type
 * @param signedInt
 * @return
 */
tiledb_datatype_t mysqlTypeToTileDBType(int type, bool signedInt);

/**
 * Converts a tiledb_datatype_t to a mysql type
 * @param type
 * @return
 */
int TileDBTypeToMysqlType(tiledb_datatype_t type);

/**
 * Create the text string for a mysql type
 * e.g. MYSQL_TYPE_TINY -> "TINY"
 * @param type
 * @return
 */
std::string MysqlTypeString(int type);

tiledb::Attribute create_field_attribute(tiledb::Context &ctx, Field *field,
                                         const tiledb::FilterList &filterList);

tiledb::Dimension create_field_dimension(tiledb::Context &ctx, Field *field);

// -- helpers --

template <typename T> T parse_value(const std::string &s) {
  T result;
  std::istringstream ss(s);
  ss >> result;
  if (ss.fail())
    throw std::invalid_argument("Cannot parse value from '" + s + "'");
  return result;
}

template <typename T> std::array<T, 2> get_dim_domain(Field *field) {
  std::array<T, 2> domain = {std::numeric_limits<T>::lowest(),
                             std::numeric_limits<T>::max()};
  if (field->option_struct->lower_bound != nullptr)
    domain[0] = parse_value<T>(field->option_struct->lower_bound);
  if (field->option_struct->upper_bound != nullptr)
    domain[1] = parse_value<T>(field->option_struct->upper_bound);
  return domain;
}

template <typename T>
tiledb::Dimension create_dim(tiledb::Context &ctx, Field *field) {
  if (field->option_struct->tile_extent == nullptr) {
    my_printf_error(ER_UNKNOWN_ERROR,
                    "Invalid dimension, must specify tile extent",
                    ME_ERROR_LOG | ME_FATAL);
    throw std::invalid_argument("Must specify tile extent");
  }

  std::array<T, 2> domain = get_dim_domain<T>(field);
  T tile_extent = parse_value<T>(field->option_struct->tile_extent);
  return tiledb::Dimension::create<T>(ctx, field->field_name.str, domain,
                                      tile_extent);
}

// -- end helpers --

} // namespace tile
