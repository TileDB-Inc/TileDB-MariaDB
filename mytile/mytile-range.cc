/**
 * @file   mytile-range.h
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
 * This defines the range struct for handling pushdown ranges
 */

#include <mysqld_error.h>
#include "mytile-range.h"

void tile::setup_range(const std::shared_ptr<range> &range,
                       void *non_empty_domain, tiledb::Dimension dimension) {
  switch (dimension.type()) {
  case tiledb_datatype_t::TILEDB_FLOAT64:
    return setup_range<double>(range, static_cast<double *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_FLOAT32:
    return setup_range<float>(range, static_cast<float *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_INT8:
    return setup_range<int8_t>(range, static_cast<int8_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_UINT8:
    return setup_range<uint8_t>(range,
                                static_cast<uint8_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_INT16:
    return setup_range<int16_t>(range,
                                static_cast<int16_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_UINT16:
    return setup_range<uint16_t>(range,
                                 static_cast<uint16_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_INT32:
    return setup_range<int32_t>(range,
                                static_cast<int32_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_UINT32:
    return setup_range<uint32_t>(range,
                                 static_cast<uint32_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_INT64:
    return setup_range<int64_t>(range,
                                static_cast<int64_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_UINT64:
    return setup_range<uint64_t>(range,
                                 static_cast<uint64_t *>(non_empty_domain));

    /*case tiledb_datatype_t::TILEDB_CHAR:
    case tiledb_datatype_t::TILEDB_STRING_ASCII:
    case tiledb_datatype_t::TILEDB_STRING_UTF8:
    case tiledb_datatype_t::TILEDB_STRING_UTF16:
    case tiledb_datatype_t::TILEDB_STRING_UTF32:
    case tiledb_datatype_t::TILEDB_STRING_UCS2:
    case tiledb_datatype_t::TILEDB_STRING_UCS4:*/

  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
    return setup_range<int64_t>(range,
                                static_cast<int64_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
    return setup_range<int64_t>(range,
                                static_cast<int64_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_DATETIME_NS:
    return setup_range<int64_t>(range,
                                static_cast<int64_t *>(non_empty_domain));

  default: {
    const char *datatype_str;
    tiledb_datatype_to_str(range->datatype, &datatype_str);
    my_printf_error(
        ER_UNKNOWN_ERROR,
        "Unknown or unsupported tiledb data type in setup_range: %s",
        ME_ERROR_LOG | ME_FATAL, datatype_str);
  }
  }
}
