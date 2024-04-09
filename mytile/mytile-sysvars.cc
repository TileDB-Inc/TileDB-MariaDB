/**
 * @file   mytile-sysvars.cc
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
 * This declares the sysvars
 */

#include <my_global.h> // ulonglong
#include <handler.h>
#include <tiledb/tiledb.h>
#include "mytile-sysvars.h"

namespace tile {
namespace sysvars {
// Read buffer size
static MYSQL_THDVAR_ULONGLONG(read_buffer_size,
                              PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_THDLOCAL,
                              "Total read buffer size for TileDB queries. This "
                              "will be shared across all fields being queried.",
                              NULL, NULL, 104857600, 0, ~0UL, 0);

// Write buffer size
static MYSQL_THDVAR_ULONGLONG(
    write_buffer_size, PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_THDLOCAL,
    "Total write buffer size for TileDB queries. This will be shared across "
    "all fields being queried.",
    NULL, NULL, 104857600, 0, ~0UL, 0);

// Physically delete arrays
static MYSQL_THDVAR_BOOL(delete_arrays,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_THDLOCAL,
                         "Should drop table delete TileDB arrays", NULL, NULL,
                         false);

// Set TileDB Configuration parameters
static MYSQL_THDVAR_STR(tiledb_config,
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
                        "TileDB configuration parameters, comma separated",
                        NULL, NULL, "");

// Should arrays force to be reopened? This allows for new TileDB Configuration
// parameters to always take effect
static MYSQL_THDVAR_BOOL(
    reopen_for_every_query, PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_THDLOCAL,
    "Force reopen TileDB array for every query, this allows for tiledb_config "
    "paraneters to always take effect",
    NULL, NULL, true);

const char *query_layout_names[] = {"row-major", "col-major", "unordered",
                                    "global-order", NullS};

TYPELIB query_layout_typelib = {array_elements(query_layout_names) - 1,
                                "query_layout_typelib", query_layout_names,
                                NULL};

// Set read query layout
static MYSQL_THDVAR_ENUM(read_query_layout, PLUGIN_VAR_OPCMDARG,
                         "TileDB query layout, valid layouts are row-major, "
                         "col-major, unordered, global-order",
                         NULL, NULL, 2, // default to unordered
                         &query_layout_typelib);

// Dimensions as primary keys
static MYSQL_THDVAR_BOOL(dimensions_are_keys,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_THDLOCAL,
                         "Should dimension be treated as primary keys", NULL,
                         NULL, true);

// Should predicates be pushdown where possible
static MYSQL_THDVAR_BOOL(enable_pushdown,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_THDLOCAL,
                         "Pushdown predicates where possible", NULL, NULL,
                         true);

// Should aggregates be pushdown where possible
static MYSQL_THDVAR_BOOL(enable_aggregate_pushdown,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_THDLOCAL,
                         "Pushdown aggregates where possible", NULL, NULL,
                         true);

// Computing records in array can be intensive, but sometimes the trade off is
// worth it for smarter optimizer should we compute the record upper bound on
// array open?
static MYSQL_THDVAR_BOOL(compute_table_records,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_THDLOCAL,
                         "compute size of table (record count) on opening",
                         NULL, NULL, false);

// Running a create table normally has two modes, assisted discovery or create
// new This option allows for a third mode, which is you can specify the list of
// columns in the create statement and if the array already exists MariaDB will
// just only access the columns you've specified. This is a workaround for
// MariaDB only allowing 4096 column in total. Now if your array has more
// columns then mariadb can handle you an use this option to select a smaller
// subset for querying
static MYSQL_THDVAR_BOOL(create_allow_subset_existing_array,
                         PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_THDLOCAL,
                         "Allow registering a subset of column", NULL, NULL,
                         false);

static MYSQL_THDVAR_BOOL(mrr_support, PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_THDLOCAL,
                         "Should MRR support be enabled for queries", NULL,
                         NULL, false);

const char *log_level_names[] = {"error", "warning", "info", "debug", NullS};

TYPELIB log_level_typelib = {array_elements(log_level_names) - 1,
                             "log_level_typelib", log_level_names, NULL};

static MYSQL_THDVAR_ENUM(log_level, PLUGIN_VAR_OPCMDARG, "log level for mytile",
                         NULL, NULL, 1, &log_level_typelib);

// system variables
struct st_mysql_sys_var *mytile_system_variables[] = {
    MYSQL_SYSVAR(read_buffer_size),
    MYSQL_SYSVAR(write_buffer_size),
    MYSQL_SYSVAR(delete_arrays),
    MYSQL_SYSVAR(tiledb_config),
    MYSQL_SYSVAR(reopen_for_every_query),
    MYSQL_SYSVAR(read_query_layout),
    MYSQL_SYSVAR(dimensions_are_keys),
    MYSQL_SYSVAR(enable_pushdown),
    MYSQL_SYSVAR(compute_table_records),
    MYSQL_SYSVAR(log_level),
    MYSQL_SYSVAR(create_allow_subset_existing_array),
    MYSQL_SYSVAR(mrr_support),
    MYSQL_SYSVAR(enable_aggregate_pushdown),
    NULL};

ulonglong read_buffer_size(THD *thd) { return THDVAR(thd, read_buffer_size); }

ulonglong write_buffer_size(THD *thd) { return THDVAR(thd, write_buffer_size); }

my_bool delete_arrays(THD *thd) { return THDVAR(thd, delete_arrays); }

char *tiledb_config(THD *thd) { return THDVAR(thd, tiledb_config); }

my_bool reopen_for_every_query(THD *thd) {
  return THDVAR(thd, reopen_for_every_query);
}

tiledb_layout_t read_query_layout(THD *thd) {
  uint64_t layout = THDVAR(thd, read_query_layout);
  const char *layout_str = query_layout_names[layout];

  tiledb_layout_t query_layout;
  tiledb_layout_from_str(layout_str, &query_layout);
  return query_layout;
}

my_bool dimensions_are_keys(THD *thd) {
  return THDVAR(thd, dimensions_are_keys);
}

my_bool enable_pushdown(THD *thd) { return THDVAR(thd, enable_pushdown); }

my_bool enable_aggregate_pushdown(THD *thd) { return THDVAR(thd, enable_aggregate_pushdown); }

my_bool compute_table_records(THD *thd) {
  return THDVAR(thd, compute_table_records);
}

my_bool create_allow_subset_existing_array(THD *thd) {
  return THDVAR(thd, create_allow_subset_existing_array);
}

my_bool mrr_support(THD *thd) { return THDVAR(thd, mrr_support); }

LOG_LEVEL log_level(THD *thd) { return LOG_LEVEL(THDVAR(thd, log_level)); }
} // namespace sysvars
} // namespace tile
