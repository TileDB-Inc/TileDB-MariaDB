/**
 * @file   mytile-discovery.h
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
 * This declares the table discovery functions.
 */


#pragma once

#include <handler.h>

namespace tile {
    /**
     * Converts a mysql type to a tiledb_datatype_t
     * @param type
     * @param signedInt
     * @return
     */
  tiledb_datatype_t mysqlTypeToTileDBType(int type, bool signedInt);

  /**
   * Converts a tiledb_datatype_t to a mysql type
   * @param type
   * @return
   */
  int TileDBTypeToMysqlType(tiledb_datatype_t type);

  /**
   * Create the text string for a mysql type
   * e.g. MYSQL_TYPE_TINY -> "TINY"
   * @param type
   * @return
   */
  std::string MysqlTypeString(int type);

  /**
   * Discover an array structure dynamically, builds the "create table" sql strings and init's the TABLE_SHARE
   * @param hton
   * @param thd
   * @param ts
   * @return
   */
  int mytile_discover_table(handlerton *hton, THD *thd, TABLE_SHARE *ts);

  /**
   * Checks if an array exists or not
   * @param hton
   * @param db
   * @param name
   * @return
   */
  int mytile_discover_table_existence(handlerton *hton, const char *db, const char *name);
}
