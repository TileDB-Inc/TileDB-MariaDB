/**
 * @file   mytile-statusvars.cc
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
 * This declares the status variables
 */

#include <my_global.h> // ulonglong
#include <handler.h>
#include <tiledb/tiledb.h>
#include "mytile-statusvars.h"
#include <tiledb/tiledb>

namespace tile {
namespace statusvars {
static int show_tiledb_version(MYSQL_THD thd, struct st_mysql_show_var *var,
                               char *buf) {
  auto version = tiledb::version();
  var->type = SHOW_CHAR;
  var->value = buf; // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  my_snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE, "%lu.%lu.%lu", std::get<0>(version),
              std::get<1>(version), std::get<2>(version));

  return 0;
}

struct st_mysql_show_var mytile_status_variables[] = {
    {"mytile_tiledb_version", (char *)show_tiledb_version, SHOW_SIMPLE_FUNC},
};
} // namespace statusvars
} // namespace tile