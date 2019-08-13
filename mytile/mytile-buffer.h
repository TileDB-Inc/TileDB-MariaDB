/**
 * @file   mytile-buffer.h
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
 * This declares the buffer struct
 */

#pragma once

#include <tiledb/tiledb>

typedef struct mytile_buffer {
  uint64_t *offset_buffer;     /* offset buffer for varlen data */
  uint64_t offset_buffer_size; /* offset buffer size for varlen data*/
  void *buffer;                /* data buffer */
  uint64_t buffer_size;        /* data buffer size */
  tiledb_datatype_t type;      /* type buffer */
  std::string name;            /* field name */
  bool dimension;              /* is this buffer for a dimension */
  int64_t buffer_offset; /* offset of buffer, used for dimensions only to split
                            coordinates */
  uint64_t fixed_size_elements; /* If the attribute is fixed size, this will
                                   indicate the element count */
} buffer;