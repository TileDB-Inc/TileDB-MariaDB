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
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This declares the system variables for the storage engine
 */

#pragma once

#include <handler.h>

#include <my_global.h>

// list of system parameters
extern struct st_mysql_sys_var *mytile_system_variables[];

// Read buffer size, currently unused
static MYSQL_THDVAR_ULONGLONG(read_buffer_size, PLUGIN_VAR_OPCMDARG, "", NULL,
                              NULL, 10485760, 0, ~0UL, 0);

// Write buffer size, currently unused
static MYSQL_THDVAR_ULONGLONG(write_buffer_size, PLUGIN_VAR_OPCMDARG, "", NULL,
                              NULL, 10485760, 0, ~0UL, 0);

// system variables
struct st_mysql_sys_var *mytile_system_variables[] = {
    MYSQL_SYSVAR(read_buffer_size), MYSQL_SYSVAR(write_buffer_size), NULL};