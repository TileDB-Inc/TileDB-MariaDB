/**
 * @file   utils.h
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
 * Contains utility functions
 */

#include "utils.h"
#include "mytile-sysvars.h"
#include <string>
#include <vector>
#include <sstream>
#include <sql_class.h>

/**
 *
 * Split a string by delimeter
 *
 * @param str string to split
 * @param delim delimeter to split by
 * @return vector of split strings
 */
std::vector<std::string> tile::split(const std::string &str, char delim) {
  std::vector<std::string> res;
  std::stringstream ss(str);
  std::string token;
  while (std::getline(ss, token, delim)) {
    res.push_back(token);
  }
  return res;
}

bool tile::is_numeric_type(const tiledb_datatype_t &datatype) {
  switch (datatype) {
  case TILEDB_INT8:
  case TILEDB_UINT8:
  case TILEDB_UINT16:
  case TILEDB_INT16:
  case TILEDB_INT32:
  case TILEDB_UINT32:
  case TILEDB_INT64:
  case TILEDB_UINT64:
  case TILEDB_FLOAT32:
  case TILEDB_FLOAT64:
    return true;
  default:
    return false;
  }
}

bool tile::is_signed_type(const tiledb_datatype_t &datatype) {
  switch (datatype) {
  case TILEDB_INT8:
  case TILEDB_INT16:
  case TILEDB_INT32:
  case TILEDB_INT64:
  case TILEDB_FLOAT32:
  case TILEDB_FLOAT64:
    return true;
  default:
    return false;
  }
}

bool tile::is_string_type(const tiledb_datatype_t &datatype) {
  switch (datatype) {
  case TILEDB_STRING_UTF8:
  case TILEDB_STRING_ASCII:
  case TILEDB_STRING_UCS2:
  case TILEDB_STRING_UCS4:
  case TILEDB_STRING_UTF16:
  case TILEDB_STRING_UTF32:
  case TILEDB_CHAR:
    return true;
  default:
    return false;
  }
}

void tile::log_error(THD *thd, const char *msg, ...) {

  if (tile::sysvars::log_level(thd) > tile::sysvars::LOG_LEVEL::ERROR)
    return;

  va_list args;
  va_start(args, msg);
  error_log_print(ERROR_LEVEL, msg, args);
  va_end(args);
}

void tile::log_warning(THD *thd, const char *msg, ...) {

  if (tile::sysvars::log_level(thd) > tile::sysvars::LOG_LEVEL::WARNING)
    return;

  va_list args;
  va_start(args, msg);
  error_log_print(WARNING_LEVEL, msg, args);
  va_end(args);
}

void tile::log_info(THD *thd, const char *msg, ...) {

  if (tile::sysvars::log_level(thd) > tile::sysvars::LOG_LEVEL::INFORMATION)
    return;

  va_list args;
  va_start(args, msg);
  error_log_print(INFORMATION_LEVEL, msg, args);
  va_end(args);
}

void tile::log_debug(THD *thd, const char *msg, ...) {

  if (tile::sysvars::log_level(thd) != tile::sysvars::LOG_LEVEL::DEBUG)
    return;

  va_list args;
  va_start(args, msg);
  error_log_print(INFORMATION_LEVEL, msg, args);
  va_end(args);
}

bool tile::has_ending(std::string const &s, std::string const &ending) {
  if (s.length() >= ending.length()) {
    return (0 ==
            s.compare(s.length() - ending.length(), ending.length(), ending));
  } else {
    return false;
  }
}

bool tile::has_prefix(std::string const &s, std::string const &prefix) {
  if (s.length() >= prefix.length()) {
    return (0 == s.compare(0, prefix.length(), prefix));
  } else {
    return false;
  }
}

std::string tile::regex_match(std::string const &s, std::regex const &regex) {
  std::smatch base_match;
  if (std::regex_search(s, base_match, regex)) {
    // The first sub_match is the whole string; the next
    // sub_match is the first parenthesized expression.
    if (base_match.size() == 2) {
      return base_match[1].str();
    }
  }

  return std::string();
}

std::pair<std::string, uint64_t>
tile::get_real_uri_and_timestamp(const std::string &uri) {
  std::pair<std::string, uint64_t> ret(uri, UINT64_MAX);

  std::string timestamp_str = regex_match(uri, TIME_TRAVEL_ENDING);

  if (!timestamp_str.empty()) {
    ret.first = uri.substr(0, uri.length() - (timestamp_str.length() + 1));
    ret.second = std::stoull(timestamp_str);
  }

  return ret;
}