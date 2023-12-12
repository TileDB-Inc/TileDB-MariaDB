/**
 * @file   mytile-errors.h
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
 * This declares the errors enum
 */

#pragma once

enum errors {
  ERR_FLUSH_WRITE_OTHER = -312,
  ERR_FLUSH_WRITE_TILEDB = -311,
  ERR_FINALIZE_WRITE_OTHER = -302,
  ERR_FINALIZE_WRITE_TILEDB = -301,
  ERR_WRITE_ROW_OTHER = -202,
  ERR_WRITE_ROW_TILEDB = -201,
  ERR_INDEX_READ_SCAN_OTHER = -134,
  ERR_INDEX_READ_SCAN_TILEDB = -133,
  ERR_TILE_TO_FIELDS_OTHER = -132,
  ERR_TILE_TO_FIELDS_TILEDB = -131,
  ERR_SCAN_RND_ROW_OTHER = -122,
  ERR_SCAN_RND_ROW_TILEDB = -121,
  ERR_RND_POS_OTHER = -114,
  ERR_RND_POS_TILEDB = -113,
  ERR_INIT_SCAN_OTHER = -112,
  ERR_INIT_SCAN_TILEDB = -111,
  ERR_ROW_TO_TILEDB_OTHER = -102,
  ERR_ROW_TO_TILEDB_TILEDB = -101,
  ERR_ROW_TO_TILEDB_DIM_NULL = -100,
  ERR_DELETE_TABLE_OTHER = -26,
  ERR_DELETE_TABLE_TILEDB = -25,
  ERR_CALC_UPPER_BOUND = -23,
  ERR_CREATE_DIM_OTHER = -22,
  ERR_CLOSE_OTHER = -21,
  ERR_CLOSE_TILEDB = -20,
  ERR_CREATE_DIM_NULL = -19,
  ERR_CREATE_TABLE = -12,
  ERR_CREATE_ARRAY = -11,
  ERR_BUILD_SCHEMA = -10,
  ERR_WRITE_FLUSH_NEEDED = 1000
};
