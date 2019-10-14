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

int tile::set_range_from_item_consts(Item_basic_constant *lower_const,
                                     Item_basic_constant *upper_const,
                                     std::shared_ptr<range> &range) {
  DBUG_ENTER("tile::set_range_from_item_costs");
  switch (lower_const->cmp_type()) {
  // TILED does not support string dimensions
  //                        case STRING_RESULT:
  //                            res= pval->val_str(&tmp);
  //                            pp->Value= PlugSubAllocStr(g, NULL,
  //                            res->ptr(), res->length()); pp->Type=
  //                            (pp->Value) ? TYPE_STRING : TYPE_ERROR;
  //                            break;
  case INT_RESULT: {
    range->datatype = tiledb_datatype_t::TILEDB_INT64;
    range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(longlong)), &std::free);
    range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(longlong)), &std::free);
    *static_cast<longlong *>(range->lower_value.get()) = lower_const->val_int();
    *static_cast<longlong *>(range->upper_value.get()) = upper_const->val_int();
    break;
  }
    // TODO: support time
    //                        case TIME_RESULT:
    //                            pp->Type= TYPE_DATE;
    //                            pp->Value= PlugSubAlloc(g, NULL,
    //                            sizeof(int));
    //                            *((int*)pp->Value)= (int)
    //                            Temporal_hybrid(pval).to_longlong(); break;
  case REAL_RESULT:
  case DECIMAL_RESULT: {
    range->datatype = tiledb_datatype_t::TILEDB_FLOAT64;
    range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(double)), &std::free);
    range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(double)), &std::free);
    *static_cast<double *>(range->lower_value.get()) = lower_const->val_real();
    *static_cast<double *>(range->upper_value.get()) = upper_const->val_real();
    break;
  }
  default:
    DBUG_RETURN(1);
  }
  DBUG_RETURN(0);
}