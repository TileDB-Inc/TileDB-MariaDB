/**
 * @file   mytile-sysvars.h
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
 * This declares the system variables for the storage engine
 */

#pragma once

#ifndef MYTILE_SYSVARS_H
#define MYTILE_SYSVARS_H

#define MYSQL_SERVER 1 // required for THD class

#include <handler.h>
#include <my_global.h>

namespace tile {
namespace sysvars {
enum class LOG_LEVEL { ERROR = 0, WARNING = 1, INFORMATION = 2, DEBUG = 3 };

// list of system parameters
extern struct st_mysql_sys_var *mytile_system_variables[];

ulonglong read_buffer_size(THD *thd);

ulonglong write_buffer_size(THD *thd);

my_bool delete_arrays(THD *thd);

char *tiledb_config(THD *thd);

my_bool reopen_for_every_query(THD *thd);

const char *read_query_layout(THD *thd);

my_bool dimensions_are_primary_keys(THD *thd);

my_bool enable_pushdown(THD *thd);

my_bool compute_table_records(THD *thd);

LOG_LEVEL log_level(THD *thd);
} // namespace sysvars
} // namespace tile

#endif // MYTILE_SYSVARS_H
