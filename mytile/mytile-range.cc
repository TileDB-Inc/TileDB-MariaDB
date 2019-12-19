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

std::shared_ptr<tile::range>
tile::merge_ranges(std::vector<std::shared_ptr<tile::range>> &ranges,
                   tiledb_datatype_t datatype) {
  if (ranges.empty() || ranges[0] == nullptr) {
    return nullptr;
  }

  switch (datatype) {
  case tiledb_datatype_t::TILEDB_FLOAT64:
    return merge_ranges<double>(ranges);

  case tiledb_datatype_t::TILEDB_FLOAT32:
    return merge_ranges<float>(ranges);

  case tiledb_datatype_t::TILEDB_INT8:
    return merge_ranges<int8_t>(ranges);

  case tiledb_datatype_t::TILEDB_UINT8:
    return merge_ranges<uint8_t>(ranges);

  case tiledb_datatype_t::TILEDB_INT16:
    return merge_ranges<int16_t>(ranges);

  case tiledb_datatype_t::TILEDB_UINT16:
    return merge_ranges<uint16_t>(ranges);

  case tiledb_datatype_t::TILEDB_INT32:
    return merge_ranges<int32_t>(ranges);

  case tiledb_datatype_t::TILEDB_UINT32:
    return merge_ranges<uint32_t>(ranges);

  case tiledb_datatype_t::TILEDB_INT64:
  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
    return merge_ranges<int64_t>(ranges);

  case tiledb_datatype_t::TILEDB_UINT64:
    return merge_ranges<uint64_t>(ranges);

  default: {
    const char *datatype_str;
    tiledb_datatype_to_str(datatype, &datatype_str);
    my_printf_error(
        ER_UNKNOWN_ERROR,
        "Unknown or unsupported tiledb data type in merge_ranges: %s",
        ME_ERROR_LOG | ME_FATAL, datatype_str);
  }
  }

  return nullptr;
}

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
                                     Item_result cmp_type,
                                     std::shared_ptr<range> &range) {
  DBUG_ENTER("tile::set_range_from_item_costs");

  switch (cmp_type) {
  // TILED does not support string dimensions
  //                        case STRING_RESULT:
  //                            res= pval->val_str(&tmp);
  //                            pp->Value= PlugSubAllocStr(g, NULL,
  //                            res->ptr(), res->length()); pp->Type=
  //                            (pp->Value) ? TYPE_STRING : TYPE_ERROR;
  //                            break;
  case INT_RESULT: {
    range->datatype = tiledb_datatype_t::TILEDB_INT64;
    if (lower_const != nullptr) {
      longlong lower = lower_const->val_int();

      // If we have greater than, lets make it greater than or equal
      // TileDB ranges are inclusive
      if (range->operation_type == Item_func::GT_FUNC) {
        range->operation_type = Item_func::GE_FUNC;
        lower += 1;
      }

      range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
          std::malloc(sizeof(longlong)), &std::free);
      *static_cast<longlong *>(range->lower_value.get()) = lower;
    }
    if (upper_const != nullptr) {
      longlong upper = upper_const->val_int();

      // If we have less than, lets make it less than or equal
      // TileDB ranges are inclusive
      if (range->operation_type == Item_func::LT_FUNC) {
        range->operation_type = Item_func::LE_FUNC;
        upper -= 1;
      }

      range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
          std::malloc(sizeof(longlong)), &std::free);
      *static_cast<longlong *>(range->upper_value.get()) = upper;
    }
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
    if (lower_const != nullptr) {
      double lower = lower_const->val_int();

      // If we have greater than, lets make it greater than or equal
      // TileDB ranges are inclusive
      if (range->operation_type == Item_func::GT_FUNC) {
        lower = std::nextafter(lower, 1.0f);
        range->operation_type = Item_func::GE_FUNC;
      }

      range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
          std::malloc(sizeof(double)), &std::free);
      *static_cast<double *>(range->lower_value.get()) = lower;
    }

    if (upper_const != nullptr) {
      double upper = upper_const->val_int();

      // If we have less than, lets make it less than or equal
      // TileDB ranges are inclusive
      if (range->operation_type == Item_func::LT_FUNC) {
        upper = std::nextafter(upper, -1.0f);
        range->operation_type = Item_func::LE_FUNC;
      }

      range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
          std::malloc(sizeof(double)), &std::free);
      *static_cast<double *>(range->upper_value.get()) = upper;
    }
    break;
  }
  default:
    DBUG_RETURN(1);
  }
  DBUG_RETURN(0);
}

std::vector<std::shared_ptr<tile::range>>
tile::get_unique_non_contained_in_ranges(
    const std::vector<std::shared_ptr<range>> &in_ranges,
    const std::shared_ptr<range> &main_range) {
  tiledb_datatype_t datatype;
  if (main_range != nullptr) {
    datatype = main_range->datatype;
  } else if (!in_ranges.empty()) {
    datatype = in_ranges[0]->datatype;
  }

  // If the in_ranges is empty so lets bail early
  if (in_ranges.empty()) {
    return {};
  }

  switch (datatype) {
  case tiledb_datatype_t::TILEDB_FLOAT64:
    return get_unique_non_contained_in_ranges<double>(in_ranges, main_range);

  case tiledb_datatype_t::TILEDB_FLOAT32:
    return get_unique_non_contained_in_ranges<float>(in_ranges, main_range);

  case tiledb_datatype_t::TILEDB_INT8:
    return get_unique_non_contained_in_ranges<int8_t>(in_ranges, main_range);

  case tiledb_datatype_t::TILEDB_UINT8:
    return get_unique_non_contained_in_ranges<uint8_t>(in_ranges, main_range);

  case tiledb_datatype_t::TILEDB_INT16:
    return get_unique_non_contained_in_ranges<int16_t>(in_ranges, main_range);

  case tiledb_datatype_t::TILEDB_UINT16:
    return get_unique_non_contained_in_ranges<uint16_t>(in_ranges, main_range);

  case tiledb_datatype_t::TILEDB_INT32:
    return get_unique_non_contained_in_ranges<int32_t>(in_ranges, main_range);

  case tiledb_datatype_t::TILEDB_UINT32:
    return get_unique_non_contained_in_ranges<uint32_t>(in_ranges, main_range);

  case tiledb_datatype_t::TILEDB_INT64:
  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
    return get_unique_non_contained_in_ranges<int64_t>(in_ranges, main_range);

  case tiledb_datatype_t::TILEDB_UINT64:
    return get_unique_non_contained_in_ranges<uint64_t>(in_ranges, main_range);

  default: {
    const char *datatype_str;
    tiledb_datatype_to_str(datatype, &datatype_str);
    my_printf_error(ER_UNKNOWN_ERROR,
                    "Unknown or unsupported tiledb data type in "
                    "get_unique_non_contained_in_ranges: %s",
                    ME_ERROR_LOG | ME_FATAL, datatype_str);
  }
  }

  return {};
}

Item_func::Functype tile::find_flag_to_func(enum ha_rkey_function find_flag) {
  switch (find_flag) {
  case HA_READ_KEY_EXACT:
    return Item_func::Functype::EQ_FUNC;
  case HA_READ_KEY_OR_NEXT:
    return Item_func::Functype::GE_FUNC;
  case HA_READ_KEY_OR_PREV:
    return Item_func::Functype::LE_FUNC;
  case HA_READ_AFTER_KEY:
    return Item_func::Functype::GT_FUNC;
  case HA_READ_BEFORE_KEY:
    return Item_func::Functype::LT_FUNC;
  case HA_READ_PREFIX:
    return Item_func::Functype::EQ_FUNC;
  case HA_READ_PREFIX_LAST:
    return Item_func::Functype::EQ_FUNC;
  case HA_READ_PREFIX_LAST_OR_PREV:
    return Item_func::Functype::EQ_FUNC;
  case HA_READ_MBR_CONTAIN:
  case HA_READ_MBR_INTERSECT:
  case HA_READ_MBR_WITHIN:
  case HA_READ_MBR_DISJOINT:
  case HA_READ_MBR_EQUAL:
  default:
    my_printf_error(ER_UNKNOWN_ERROR, "Unsupported ha_rkey_function",
                    ME_ERROR_LOG | ME_FATAL);
  }
  return Item_func::Functype::EQ_FUNC;
}

std::vector<std::shared_ptr<tile::range>>
tile::build_ranges_from_key(const uchar *key, uint length,
                            enum ha_rkey_function find_flag,
                            tiledb_datatype_t datatype) {
  // Length shouldn't be zero here but better safe then segfault!
  if (length == 0)
    return {};

  switch (datatype) {
  case tiledb_datatype_t::TILEDB_FLOAT64:
    return build_ranges_from_key<double>(key, length, find_flag, datatype);

  case tiledb_datatype_t::TILEDB_FLOAT32:
    return build_ranges_from_key<float>(key, length, find_flag, datatype);

  case tiledb_datatype_t::TILEDB_INT8:
    return build_ranges_from_key<int8_t>(key, length, find_flag, datatype);

  case tiledb_datatype_t::TILEDB_UINT8:
    return build_ranges_from_key<uint8_t>(key, length, find_flag, datatype);

  case tiledb_datatype_t::TILEDB_INT16:
    return build_ranges_from_key<int16_t>(key, length, find_flag, datatype);

  case tiledb_datatype_t::TILEDB_UINT16:
    return build_ranges_from_key<uint16_t>(key, length, find_flag, datatype);

  case tiledb_datatype_t::TILEDB_INT32:
    return build_ranges_from_key<int32_t>(key, length, find_flag, datatype);

  case tiledb_datatype_t::TILEDB_UINT32:
    return build_ranges_from_key<uint32_t>(key, length, find_flag, datatype);

  case tiledb_datatype_t::TILEDB_INT64:
  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
    return build_ranges_from_key<int64_t>(key, length, find_flag, datatype);

  case tiledb_datatype_t::TILEDB_UINT64:
    return build_ranges_from_key<uint64_t>(key, length, find_flag, datatype);

  default: {
    const char *datatype_str;
    tiledb_datatype_to_str(datatype, &datatype_str);
    my_printf_error(ER_UNKNOWN_ERROR,
                    "Unknown or unsupported tiledb data type in "
                    "build_ranges_from_key: %s",
                    ME_ERROR_LOG | ME_FATAL, datatype_str);
  }
  }
  return {};
}
