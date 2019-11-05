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

#include <handler.h>
#include <my_global.h>

// list of system parameters
extern struct st_mysql_sys_var *mytile_system_variables[];

// Read buffer size
static MYSQL_THDVAR_ULONGLONG(
    read_buffer_size, PLUGIN_VAR_OPCMDARG,
    "Read buffer size per attribute for TileDB queries", NULL, NULL, 104857600,
    0, ~0UL, 0);

// Write buffer size
static MYSQL_THDVAR_ULONGLONG(
    write_buffer_size, PLUGIN_VAR_OPCMDARG,
    "Write buffer size per attribute for TileDB queries", NULL, NULL, 104857600,
    0, ~0UL, 0);

// Physically delete arrays
static MYSQL_THDVAR_BOOL(delete_arrays, PLUGIN_VAR_OPCMDARG,
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
    reopen_for_every_query, PLUGIN_VAR_OPCMDARG,
    "Force reopen TileDB array for every query, this allows for tiledb_config "
    "paraneters to always take effect",
    NULL, NULL, true);

const char *query_layout_names[] = {"row-major", "col-major", "unordered",
                                    "global-order", NullS};

TYPELIB query_layout_typelib = {array_elements(query_layout_names) - 1,
                                "query_layout_typelib", query_layout_names,
                                NULL};

static MYSQL_THDVAR_ENUM(read_query_layout, PLUGIN_VAR_OPCMDARG,
                         "TileDB query layout, valid layouts are row-major, "
                         "col-major, unordered, global-order",
                         NULL, NULL,
                         2, // default to unordered
                         &query_layout_typelib);
#endif // MYTILE_SYSVARS_H