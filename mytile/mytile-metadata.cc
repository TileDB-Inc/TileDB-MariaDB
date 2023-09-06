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

#include "mytile-metadata.h"
#include <field.h>
#include <tztime.h>
#include <mysqld_error.h>
#include <sql_class.h>

std::string tile::build_metadata_value_string(THD *thd, const void *data,
                                              uint32_t num,
                                              tiledb_datatype_t type) {
  switch (type) {
  case TILEDB_INT8:
    return tile::build_metadata_numeric_value_string<int8_t>(data, num);
  case TILEDB_UINT8:
    return tile::build_metadata_numeric_value_string<uint8_t>(data, num);
  case TILEDB_INT16:
    return tile::build_metadata_numeric_value_string<int16_t>(data, num);
  case TILEDB_UINT16:
    return tile::build_metadata_numeric_value_string<uint16_t>(data, num);
  case TILEDB_INT32:
    return tile::build_metadata_numeric_value_string<int32_t>(data, num);
  case TILEDB_UINT32:
    return tile::build_metadata_numeric_value_string<uint32_t>(data, num);
  case TILEDB_INT64:
    return tile::build_metadata_numeric_value_string<int64_t>(data, num);
  case TILEDB_UINT64:
    return tile::build_metadata_numeric_value_string<uint64_t>(data, num);
  case TILEDB_FLOAT32:
    return tile::build_metadata_numeric_value_string<float>(data, num);
  case TILEDB_FLOAT64:
    return tile::build_metadata_numeric_value_string<double>(data, num);
  case TILEDB_CHAR:
  case TILEDB_STRING_ASCII:
  case TILEDB_STRING_UTF8:
  case TILEDB_STRING_UTF16:
  case TILEDB_STRING_UTF32:
  case TILEDB_STRING_UCS2:
  case TILEDB_STRING_UCS4:
  case TILEDB_ANY:
  case TILEDB_BLOB:
    return tile::build_metadata_string_value_string<char>(data, num);
  case TILEDB_DATETIME_YEAR:
    return tile::build_metadata_numeric_value_string<int64_t>(data, num);
  case TILEDB_DATETIME_MONTH:
  case TILEDB_DATETIME_WEEK:
  case TILEDB_DATETIME_DAY:
  case TILEDB_DATETIME_HR:
  case TILEDB_DATETIME_MIN:
  case TILEDB_DATETIME_SEC:
  case TILEDB_DATETIME_MS:
  case TILEDB_DATETIME_US:
  case TILEDB_DATETIME_NS:
  case TILEDB_DATETIME_PS:
  case TILEDB_DATETIME_FS:
  case TILEDB_DATETIME_AS:
    return tile::build_metadata_datetime_value_string(thd, data, num, type);
  case TILEDB_TIME_HR:
  case TILEDB_TIME_MIN:
  case TILEDB_TIME_SEC:
  case TILEDB_TIME_MS:
  case TILEDB_TIME_US:
  case TILEDB_TIME_NS:
  case TILEDB_TIME_PS:
  case TILEDB_TIME_FS:
  case TILEDB_TIME_AS:
    return tile::build_metadata_time_value_string(thd, data, num, type);
  case TILEDB_BOOL:
    return tile::build_metadata_numeric_value_string<bool>(data, num);
  }
  return "";
}

std::string tile::build_metadata_datetime_value_string(THD *thd,
                                                       const void *data,
                                                       uint32_t num,
                                                       tiledb_datatype_t type) {
  const uint64_t *data_typed = static_cast<const uint64_t *>(data);
  std::stringstream ss;
  for (uint32_t i = 0; i < num; i++) {
    const uint64_t value = data_typed[i];
    switch (type) {
    case tiledb_datatype_t::TILEDB_DATETIME_MONTH: {
      // TODO: This isn't a good calculation we should fix it
      uint64_t seconds = value * (60 * 60 * 24 * 365) / 12;
      ss << build_datetime_string(thd, seconds, 0, MYSQL_TIMESTAMP_DATE);
      break;
    }
    case tiledb_datatype_t::TILEDB_DATETIME_WEEK: {
      uint64_t seconds = value * (60 * 60 * 24 * 7);
      ss << build_datetime_string(thd, seconds, 0, MYSQL_TIMESTAMP_DATE);
      break;
    }
    case tiledb_datatype_t::TILEDB_DATETIME_DAY: {
      uint64_t seconds = value * (60 * 60 * 24);
      ss << build_datetime_string(thd, seconds, 0, MYSQL_TIMESTAMP_DATE);
      break;
    }
    case tiledb_datatype_t::TILEDB_DATETIME_HR: {
      uint64_t seconds = value * (60 * 60);
      ss << build_datetime_string(thd, seconds, 0, MYSQL_TIMESTAMP_DATETIME);
      break;
    }
    case tiledb_datatype_t::TILEDB_DATETIME_MIN: {
      uint64_t seconds = value * 60;
      ss << build_datetime_string(thd, seconds, 0, MYSQL_TIMESTAMP_DATETIME);
      break;
    }
    case tiledb_datatype_t::TILEDB_DATETIME_SEC: {
      uint64_t seconds = value;
      ss << build_datetime_string(thd, seconds, 0, MYSQL_TIMESTAMP_DATETIME);
      break;
    }
    case tiledb_datatype_t::TILEDB_DATETIME_MS: {
      uint64_t ms = value;
      uint64_t seconds = ms / 1000;
      ss << build_datetime_string(thd, seconds, ms - (seconds * 1000),
                                  MYSQL_TIMESTAMP_DATETIME);
      break;
    }
    case tiledb_datatype_t::TILEDB_DATETIME_US: {
      uint64_t us = value;
      uint64_t seconds = us / 1000000;
      ss << build_datetime_string(thd, seconds, us - (seconds * 1000000),
                                  MYSQL_TIMESTAMP_DATETIME);
      break;
    }
    case tiledb_datatype_t::TILEDB_DATETIME_NS: {
      uint64_t ns = value;
      uint64_t seconds = ns / 1000000000;
      ss << build_datetime_string(thd, seconds,
                                  (ns - (seconds * 1000000000)) / 1000,
                                  MYSQL_TIMESTAMP_DATETIME);
      break;
    }
    case tiledb_datatype_t::TILEDB_DATETIME_PS: {
      uint64_t ps = value;
      uint64_t seconds = ps / 1000000000000;
      ss << build_datetime_string(thd, seconds,
                                  (ps - (seconds * 1000000000)) / 1000000,
                                  MYSQL_TIMESTAMP_DATETIME);
      break;
    }
    case tiledb_datatype_t::TILEDB_DATETIME_FS: {
      uint64_t fs = value;
      uint64_t seconds = fs / 1000000000000000;
      ss << build_datetime_string(thd, seconds,
                                  (fs - (seconds * 1000000000)) / 1000000000,
                                  MYSQL_TIMESTAMP_DATETIME);
      break;
    }
    case tiledb_datatype_t::TILEDB_DATETIME_AS: {
      uint64_t as = value;
      uint64_t seconds = as / 1000000000000000000;
      ss << build_datetime_string(thd, seconds,
                                  (as - (seconds * 1000000000)) / 1000000000000,
                                  MYSQL_TIMESTAMP_DATETIME);
      break;
    }
    default: {
      const char *type_str = nullptr;
      tiledb_datatype_to_str(type, &type_str);
      my_printf_error(
          ER_UNKNOWN_ERROR,
          "Unknown or unsupported datatype for converting to string: %s",
          ME_ERROR_LOG | ME_FATAL, type_str);
      break;
    }
    }
    ss << METADATA_DELIMITER;
  }
  std::string s = ss.str();
  // We use a substring to remove the last delimiter
  return s.substr(0, s.length() - 1);
}

std::string tile::build_metadata_time_value_string(THD *thd, const void *data,
                                                   uint32_t num,
                                                   tiledb_datatype_t type) {
  const int64_t *data_typed = static_cast<const int64_t *>(data);
  std::stringstream ss;
  for (uint32_t i = 0; i < num; i++) {
    const uint64_t value = data_typed[i];
    MYSQL_TIME to;
    to.time_type = MYSQL_TIMESTAMP_TIME;
    switch (type) {
    case tiledb_datatype_t::TILEDB_TIME_HR: {
      to.hour = value;
      char *str = nullptr;
      my_TIME_to_str(&to, str, 6);
      ss << str;
      //        uint64_t seconds = value * (60 * 60);
      //        ss << build_time_string(thd, seconds, 0, MYSQL_TIMESTAMP_TIME);
      break;
    }
    case tiledb_datatype_t::TILEDB_TIME_MIN: {
      to.minute = value;
      char *str = nullptr;
      my_TIME_to_str(&to, str, 6);
      ss << str;
      //        uint64_t seconds = value * 60;
      //        ss << build_time_string(thd, seconds, 0, MYSQL_TIMESTAMP_TIME);
      break;
    }
    case tiledb_datatype_t::TILEDB_TIME_SEC: {
      to.second = value;
      char *str = nullptr;
      my_TIME_to_str(&to, str, 6);
      ss << str;
      //        uint64_t seconds = value;
      //        ss << build_time_string(thd, seconds, 0, MYSQL_TIMESTAMP_TIME);
      break;
    }
    case tiledb_datatype_t::TILEDB_TIME_MS: {
      to.second = value / 1000;
      to.second_part = value - (to.second * 1000);
      char *str = nullptr;
      my_TIME_to_str(&to, str, 6);
      ss << str;
      break;
    }
    case tiledb_datatype_t::TILEDB_TIME_US: {
      uint64_t us = value;
      uint64_t seconds = us / 1000000;
      uint64_t second_part = us - (seconds * 1000000);

      to.second = seconds;
      to.second_part = second_part;
      char *str = nullptr;
      my_TIME_to_str(&to, str, 6);
      ss << str;
      break;
    }
    case tiledb_datatype_t::TILEDB_TIME_NS: {
      uint64_t ns = value;
      uint64_t seconds = ns / 1000000000;
      uint64_t us = (ns - (seconds * 1000000000)) / 1000;
      to.second = seconds;
      to.second_part = us;
      char *str = nullptr;
      my_TIME_to_str(&to, str, 6);
      ss << str;
      break;
    }
    case tiledb_datatype_t::TILEDB_TIME_PS: {
      uint64_t ps = value;
      uint64_t seconds = ps / 1000000000000;
      uint64_t us = (ps - (seconds * 1000000000)) / 1000000;
      to.second = seconds;
      to.second_part = us;
      char *str = nullptr;
      my_TIME_to_str(&to, str, 6);
      ss << str;
      break;
    }
    case tiledb_datatype_t::TILEDB_TIME_FS: {
      uint64_t fs = value;
      uint64_t seconds = fs / 1000000000000000;
      uint64_t us = (fs - (seconds * 1000000000)) / 1000000000;
      to.second = seconds;
      to.second_part = us;
      char *str = nullptr;
      my_TIME_to_str(&to, str, 6);
      ss << str;
      break;
    }
    case tiledb_datatype_t::TILEDB_TIME_AS: {
      uint64_t as = value;
      uint64_t seconds = as / 1000000000000000000;
      uint64_t us = (as - (seconds * 1000000000)) / 1000000000000;
      to.second = seconds;
      to.second_part = us;
      char *str = nullptr;
      my_TIME_to_str(&to, str, 6);
      ss << str;

      break;
    }
    default: {
      const char *type_str = nullptr;
      tiledb_datatype_to_str(type, &type_str);
      my_printf_error(
          ER_UNKNOWN_ERROR,
          "Unknown or unsupported datatype for converting to string: %s",
          ME_ERROR_LOG | ME_FATAL, type_str);
      break;
    }
    }
    ss << METADATA_DELIMITER;
  }
  std::string s = ss.str();
  // We use a substring to remove the last delimiter
  return s.substr(0, s.length() - 1);
}

std::string tile::build_datetime_string(THD *thd, uint64_t seconds,
                                        uint64_t second_part,
                                        enum_mysql_timestamp_type type) {
  MYSQL_TIME to;
  if (type != MYSQL_TIMESTAMP_DATE) {
    thd->variables.time_zone->gmt_sec_to_TIME(&to, seconds);
    to.second_part = second_part;
    adjust_time_range_with_warn(thd, &to, TIME_SECOND_PART_DIGITS);
  } else {
    // For dates we can't convert tz, so use UTC
    my_tz_OFFSET0->gmt_sec_to_TIME(&to, seconds);
  }
  to.time_type = type;
  char *str = nullptr;
  my_TIME_to_str(&to, str, 6);

  return str;
}

std::unordered_map<std::string, std::string>
tile::build_metadata_map(THD *thd, const std::shared_ptr<tiledb::Array> &array,
                         uint64_t *longest_key) {
  std::unordered_map<std::string, std::string> metadata_map;
  *longest_key = 0;

  for (uint64_t i = 0; i < array->metadata_num(); i++) {
    std::string key;
    const void *data;
    tiledb_datatype_t type;
    uint32_t num;
    array->get_metadata_from_index(i, &key, &type, &num, &data);
    std::string value = build_metadata_value_string(thd, data, num, type);

    if (key.length() > *longest_key) {
      *longest_key = key.length();
    }

    metadata_map.emplace(std::move(key), std::move(value));
  }

  return metadata_map;
}
