/**
 * @file   mytile-metadata.h
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
 * Handles array metadata related items such as building metadata map, searching
 * metadata map
 */
#pragma once

#define MYSQL_SERVER 1

#include <my_global.h> /* ulonglong */
#include <unordered_map>
#include <string>
#include <tiledb/tiledb>

#define METADATA_DELIMITER ','

namespace tile {
/**
 * Generic function for building strings from metadata
 * @param thd
 * @param data
 * @param num
 * @param type
 * @return
 */
std::string build_metadata_value_string(THD *thd, const void *data,
                                        uint32_t num, tiledb_datatype_t type);

/**
 * Convert numeric metadata to string
 * @tparam T
 * @param data
 * @param num
 * @return
 */
template <typename T>
std::string build_metadata_numeric_value_string(const void *data,
                                                uint32_t num) {
  std::stringstream ss;
  const T *data_typed = static_cast<const T *>(data);
  for (uint32_t i = 0; i < num; i++) {
    ss << data_typed[i] << METADATA_DELIMITER;
  }
  std::string s = ss.str();
  // We use a substring to remove the last delimiter
  return s.substr(0, s.length() - 1);
}

/**
 * Build string of strings
 * @tparam T
 * @param data
 * @param num
 * @return
 */
template <typename T>
std::string build_metadata_string_value_string(const void *data, uint32_t num) {
  std::stringstream ss;
  const T *data_typed = static_cast<const T *>(data);
  for (uint32_t i = 0; i < num; i++) {
    ss << data_typed[i];
  }
  return ss.str();
}

/**
 * Convert datetimes to csv list of datetimes
 * @param thd
 * @param data
 * @param num
 * @param type
 * @return
 */
std::string build_metadata_datetime_value_string(THD *thd, const void *data,
                                                 uint32_t num,
                                                 tiledb_datatype_t type);

/**
 * Convert times to csv list of times
 * @param thd
 * @param data
 * @param num
 * @param type
 * @return
 */
std::string build_metadata_time_value_string(THD *thd, const void *data,
                                             uint32_t num,
                                             tiledb_datatype_t type);
/**
 * Convert datetime to mysql standard string
 * @param thd
 * @param seconds
 * @param second_part
 * @param type
 * @return
 */
std::string build_datetime_string(THD *thd, uint64_t seconds,
                                  uint64_t second_part,
                                  enum_mysql_timestamp_type type);

/**
 * Build a metadata map
 * @param thd
 * @param array
 * @return
 */
std::unordered_map<std::string, std::string>
build_metadata_map(THD *thd, const std::shared_ptr<tiledb::Array> &array,
                   uint64_t *longest_key);
} // namespace tile