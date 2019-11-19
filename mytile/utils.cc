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

/**
 * compares two config
 * @param rhs
 * @param lhs
 * @return true is identical, false otherwise
 */
bool tile::compare_configs(tiledb::Config &rhs, tiledb::Config &lhs) {
  // Check every parameter to see if they are the same or different
  for (auto &it : rhs) {
    try {
      if (lhs.get(it.first) != it.second) {
        return false;
      }
    } catch (tiledb::TileDBError &e) {
      return false;
    }
  }

  return true;
}

void tile::dbg_print_key(const char *key, uint length,
                         tiledb_datatype_t datatype) {
  //  const char *end_ptr;
  //  uint roop_count = 1;
  DBUG_ENTER("dbg_print_key");
  DBUG_PRINT("info", ("mytile key_length=%u", length));

  std::string skey;
  switch (datatype) {
  case TILEDB_FLOAT32: {
    float key1;
    memcpy(&key1, key, sizeof(key1));
    skey = std::to_string(key1);
    break;
  }
  case TILEDB_FLOAT64: {
    double key1;
    memcpy(&key1, key, sizeof(key1));
    skey = std::to_string(key1);
    break;
  }
  case TILEDB_INT8: {
    int8_t key1;
    memcpy(&key1, key, sizeof(key1));
    skey = std::to_string(key1);
    break;
  }
  case TILEDB_UINT8: {
    uint8_t key1;
    memcpy(&key1, key, sizeof(key1));
    skey = std::to_string(key1);
    break;
  }
  case TILEDB_INT16: {
    int16_t key1;
    memcpy(&key1, key, sizeof(key1));
    skey = std::to_string(key1);
    break;
  }
  case TILEDB_UINT16: {
    uint16_t key1;
    memcpy(&key1, key, sizeof(key1));
    skey = std::to_string(key1);
    break;
  }
  case TILEDB_INT32: {
    int32_t key1;
    memcpy(&key1, key, sizeof(key1));
    skey = std::to_string(key1);
    break;
  }
  case TILEDB_UINT32: {
    uint32_t key1;
    memcpy(&key1, key, sizeof(key1));
    skey = std::to_string(key1);
    break;
  }
  case TILEDB_INT64: {
    int64_t key1;
    memcpy(&key1, key, sizeof(key1));
    skey = std::to_string(key1);
    break;
  }
  case TILEDB_UINT64: {
    uint64_t key1;
    memcpy(&key1, key, sizeof(key1));
    skey = std::to_string(key1);
    break;
  }
  default:
    // Log errors
    const char *datatype_str;
    tiledb_datatype_to_str(datatype, &datatype_str);
    my_printf_error(ER_UNKNOWN_ERROR, "[dbg_print_key] unsupported datatype %s",
                    ME_ERROR_LOG | ME_FATAL, datatype_str);
    break;
  }
  DBUG_PRINT("info", ("mytile key=%s", skey.c_str()));
  std::cerr << "mytile key=" << skey << std::endl;
  /*end_ptr = key + length;
  while (key < end_ptr)
  {
    DBUG_PRINT("info",("mytile key[%u]=%s", roop_count, key));
    std::cerr << "mytile key[" << roop_count << "]=" << key << std::endl;
    key = strchr(key, '\0') + 1;
    roop_count++;
  }*/
  DBUG_VOID_RETURN;
} /* print_key */