/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#pragma once

#define MYSQL_SERVER 1

#include "my_global.h" /* ulonglong */
#include "mytile-buffer.h"
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

enum errors { ERR_WRITE_FLUSH_NEEDED = 1000 };

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
int TileDBTypeToMysqlType(tiledb_datatype_t type, bool multi_value);

/**
 * Create the text string for a mysql type
 * e.g. MYSQL_TYPE_TINY -> "TINY"
 * @param type
 * @return
 */
std::string MysqlTypeString(int type);

/**
 * Returns if a tiledb datatype is unsigned or not
 * @param type
 * @return
 */
bool TileDBTypeIsUnsigned(tiledb_datatype_t type);

/**
 * Returns if a mysql type is a blob type
 * @param type
 * @return
 */
bool MysqlBlobType(enum_field_types type);

/**
 * Returns if a mysql type is a datetime type
 * @param type
 * @return
 */
bool MysqlDatetimeType(enum_field_types type);

tiledb::Attribute create_field_attribute(tiledb::Context &ctx, Field *field,
                                         const tiledb::FilterList &filterList);

tiledb::Dimension create_field_dimension(tiledb::Context &ctx, Field *field);

// -- helpers --

template <typename T> T parse_value(const std::string &s) {
  T result;
  std::istringstream ss(s);
  if (typeid(T) == typeid(int8_t) || typeid(T) == typeid(uint8_t)) {
    int32_t tmp = 0;
    ss >> tmp;
    result = static_cast<T>(tmp);
  } else {
    ss >> result;
  }
  if (ss.fail())
    throw std::invalid_argument("Cannot parse value from '" + s + "'");
  return result;
}

template <typename T> std::array<T, 2> get_dim_domain(Field *field) {
  std::array<T, 2> domain = {std::numeric_limits<T>::lowest(),
                             std::numeric_limits<T>::max()};
  domain[1] -= parse_value<T>(field->option_struct->tile_extent);
  if (field->option_struct->lower_bound != nullptr)
    domain[0] = parse_value<T>(field->option_struct->lower_bound);
  if (field->option_struct->upper_bound != nullptr)
    domain[1] = parse_value<T>(field->option_struct->upper_bound);
  return domain;
}

template <typename T>
tiledb::Dimension create_dim(tiledb::Context &ctx, Field *field,
                             tiledb_datatype_t datatype) {
  if (field->option_struct->tile_extent == nullptr) {
    my_printf_error(ER_UNKNOWN_ERROR,
                    "Invalid dimension, must specify tile extent",
                    ME_ERROR_LOG | ME_FATAL);
    throw std::invalid_argument("Must specify tile extent");
  }

  std::array<T, 2> domain = get_dim_domain<T>(field);
  T tile_extent = parse_value<T>(field->option_struct->tile_extent);
  return tiledb::Dimension::create(ctx, field->field_name.str, datatype,
                                   domain.data(), &tile_extent);
}

void *alloc_buffer(tiledb_datatype_t type, uint64_t size);

template <typename T> T *alloc_buffer(uint64_t size) {
  return static_cast<T *>(malloc(size));
}

uint64_t computeRecordsUB(std::shared_ptr<tiledb::Array> &array,
                          void *subarray);

template <typename T>
uint64_t computeRecordsUB(std::shared_ptr<tiledb::Array> &array,
                          void *subarray) {
  T *s = static_cast<T *>(subarray);
  size_t elements = array->schema().domain().ndim() * 2;
  // Get max buffer sizes to build
  const std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> &
      maxSizes = array->max_buffer_elements<T>(std::vector<T>(s, s + elements));

  // Compute an upper bound on the number of results in the subarray.
  return maxSizes.find(TILEDB_COORDS)->second.second /
         array->schema().domain().ndim();
}

/**
 * Set datetime field
 * @param thd
 * @param field
 * @param interval
 */
// int set_datetime_field(THD *thd, Field *field, INTERVAL interval,
// interval_type interval_type);

/**
 *
 * @param thd
 * @param field
 * @param seconds
 * @param second_part
 * @return
 */
int set_datetime_field(THD *thd, Field *field, uint64_t seconds,
                       uint64_t second_part);

/**
 * Set field, stores the value from a tiledb read in the mariadb field
 * @param thd
 * @param field
 * @param buff
 * @param i
 * @return
 */
int set_field(THD *thd, Field *field, std::shared_ptr<buffer> &buff,
              uint64_t i);

/**
 * Handle variable length varchars
 * @tparam T
 * @param field
 * @param offset_buffer
 * @param offset_buffer_size
 * @param buffer
 * @param i
 * @param charset_info
 */
template <typename T>
int set_string_field(Field *field, const uint64_t *offset_buffer,
                     uint64_t offset_buffer_size, T *buffer,
                     uint64_t buffer_size, uint64_t i,
                     charset_info_st *charset_info) {
  uint64_t end_position = i + 1;
  uint64_t start_position = 0;
  // If its not the first value, we need to see where the previous position
  // ended to know where to start.
  if (i > 0) {
    start_position = offset_buffer[i - 1];
  }
  // If the current position is equal to the number of results - 1 then we are
  // at the last varchar value
  if (i >= (offset_buffer_size / sizeof(uint64_t)) - 1) {
    end_position = buffer_size / sizeof(T);
  } else { // Else read the end from the next offset.
    end_position = offset_buffer[i + 1];
  }
  size_t size = end_position - start_position;
  return field->store(static_cast<char *>(&buffer[start_position]), size,
                      charset_info);
}

/**
 * Handle fixed size (1) varchar
 * @tparam T
 * @param field
 * @param buffer
 * @param i
 * @param charset_info
 */
template <typename T>
int set_string_field(Field *field, T *buffer, uint64_t fixed_size_elements,
                     uint64_t i, charset_info_st *charset_info) {
  return field->store(static_cast<char *>(&buffer[i]), fixed_size_elements,
                      charset_info);
}

template <typename T>
int set_string_field(Field *field, std::shared_ptr<buffer> &buff, uint64_t i,
                     charset_info_st *charset_info) {
  if (buff->offset_buffer == nullptr) {
    return set_string_field<char>(field, static_cast<T *>(buff->buffer),
                                  buff->fixed_size_elements, i, charset_info);
  }
  return set_string_field<T>(
      field, buff->offset_buffer, buff->offset_buffer_size,
      static_cast<T *>(buff->buffer), buff->buffer_size, i, charset_info);
}

template <typename T> int set_field(Field *field, uint64_t i, void *buffer) {
  T val = static_cast<T *>(buffer)[i];
  if (std::is_floating_point<T>())
    return field->store(val);
  return field->store(val, std::is_signed<T>());
}

template <typename T>
int set_field(Field *field, std::shared_ptr<buffer> &buff, uint64_t i) {
  return set_field<T>(field, i, buff->buffer);
}

template <typename T>
int set_string_buffer_from_field(Field *field, std::shared_ptr<buffer> &buff,
                                 uint64_t i) {

  // Validate we are not over the offset size
  if ((i * sizeof(uint64_t)) > buff->allocated_offset_buffer_size) {
    return ERR_WRITE_FLUSH_NEEDED;
  }

  char strbuff[MAX_FIELD_WIDTH];
  String str(strbuff, sizeof(strbuff), field->charset()), *res;

  res = field->val_str(&str);

  // Find start position to copy buffer to
  uint64_t start = 0;
  if (i > 0) {
    start = buff->buffer_size / sizeof(T);
  }

  // Validate there is enough space on the buffer to copy the field into
  if ((start + res->length()) * sizeof(T) > buff->allocated_buffer_size) {
    return ERR_WRITE_FLUSH_NEEDED;
  }

  // Copy string
  memcpy(static_cast<T *>(buff->buffer) + start, res->ptr(), res->length());

  buff->buffer_size += res->length() * sizeof(T);
  buff->offset_buffer_size += sizeof(uint64_t);
  // if (i < buff->offset_buffer_size / sizeof(uint64_t))
  buff->offset_buffer[i] = start;

  return 0;
}

template <typename T>
int set_fixed_string_buffer_from_field(Field *field,
                                       std::shared_ptr<buffer> &buff,
                                       uint64_t i) {

  // Validate there is enough space on the buffer to copy the field into
  if ((((i * buff->fixed_size_elements) + buff->buffer_offset) * sizeof(T)) >
      buff->allocated_buffer_size) {
    return ERR_WRITE_FLUSH_NEEDED;
  }

  char strbuff[MAX_FIELD_WIDTH];
  String str(strbuff, sizeof(strbuff), field->charset()), *res;

  res = field->val_str(&str);

  // Find start position to copy buffer to
  uint64_t start = i;
  if (buff->fixed_size_elements > 1) {
    start *= buff->fixed_size_elements;
  }

  // Copy string
  memcpy(static_cast<T *>(buff->buffer) + start, res->ptr(),
         buff->fixed_size_elements);

  buff->buffer_size += buff->fixed_size_elements * sizeof(char);

  return 0;
}

template <typename T>
int set_buffer_from_field(T val, std::shared_ptr<buffer> &buff, uint64_t i) {

  // Validate there is enough space on the buffer to copy the field into
  if ((((i * buff->fixed_size_elements) + buff->buffer_offset) * sizeof(T)) >
      buff->allocated_buffer_size) {
    return ERR_WRITE_FLUSH_NEEDED;
  }

  static_cast<T *>(
      buff->buffer)[(i * buff->fixed_size_elements) + buff->buffer_offset] =
      val;
  buff->buffer_size += sizeof(T);
  return 0;
}

int set_buffer_from_field(Field *field, std::shared_ptr<buffer> &buff,
                          uint64_t i, THD *thd);

// -- end helpers --

} // namespace tile
