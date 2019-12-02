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

#pragma once

#define MYSQL_SERVER 1

#include <string>
#include <algorithm>
#include <tiledb/tiledb>
#include "my_global.h" /* ulonglong */
#include <handler.h>

namespace tile {
const char PATH_SEPARATOR =
#ifdef _WIN32
    '\\';
#else
    '/';
#endif

// trim from start (in place)
static inline void ltrim(std::string &s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(),
                                  [](int ch) { return !std::isspace(ch); }));
}

// trim from end (in place)
static inline void rtrim(std::string &s) {
  s.erase(std::find_if(s.rbegin(), s.rend(),
                       [](int ch) { return !std::isspace(ch); })
              .base(),
          s.end());
}

// trim from both ends (in place)
static inline void trim(std::string &s) {
  ltrim(s);
  rtrim(s);
}

/**
 *
 * Split a string by delimeter
 *
 * @param str string to split
 * @param delim delimeter to split by
 * @return vector of split strings
 */
std::vector<std::string> split(const std::string &str, char delim = ',');

/**
 *
 * Fetches the tiledb_config server/session parameter and builds a tiledb config
 * object
 *
 * @param thd
 * @return config with any parameters set
 */
tiledb::Config build_config(THD *thd);

/**
 *
 * Builds a tiledb config from a config
 *
 * @param cfg
 * @return context
 */
tiledb::Context build_context(tiledb::Config &cfg);

/**
 * compares two config
 * @param rhs
 * @param lhs
 * @return true is identical, false otherwise
 */
bool compare_configs(tiledb::Config &rhs, tiledb::Config &lhs);

/**
 * Log errors only if log level is set to error or higher
 * @param thd thd to get log level from
 * @param msg message to log
 * @param ... formatting parameters for message
 */
void log_error(THD *thd, const char *msg, ...);

/**
 * Log errors only if log level is set to warning or higher
 * @param thd thd to get log level from
 * @param msg message to log
 * @param ... formatting parameters for message
 */
void log_warning(THD *thd, const char *msg, ...);

/**
 * Log errors only if log level is set to info or higher
 * @param thd thd to get log level from
 * @param msg message to log
 * @param ... formatting parameters for message
 */
void log_info(THD *thd, const char *msg, ...);

/**
 * Log errors only if log level is set to debug or higher
 * @param thd thd to get log level from
 * @param msg message to log
 * @param ... formatting parameters for message
 */
void log_debug(THD *thd, const char *msg, ...);
} // namespace tile
