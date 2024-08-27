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
#include <limits>

std::shared_ptr<tile::range> tile::merge_ranges_str(
    const std::vector<std::shared_ptr<tile::range>> &ranges) {
  std::shared_ptr<tile::range> merged_range =
      std::make_shared<tile::range>(tile::range{
          std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
          std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
          Item_func::EQ_FUNC, tiledb_datatype_t::TILEDB_ANY, 0, 0});

  if (ranges.empty())
    return nullptr;

  // Set the first element as the default for the merged range, this gives us
  // some initial values to compare against
  merged_range->operation_type = ranges[0]->operation_type;
  merged_range->datatype = ranges[0]->datatype;

  if (ranges[0]->lower_value != nullptr) {
    merged_range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(ranges[0]->lower_value_size), &std::free);
    memcpy(merged_range->lower_value.get(), ranges[0]->lower_value.get(),
           ranges[0]->lower_value_size);
    merged_range->lower_value_size = ranges[0]->lower_value_size;
  }

  if (ranges[0]->upper_value != nullptr) {
    merged_range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(ranges[0]->upper_value_size), &std::free);
    memcpy(merged_range->upper_value.get(), ranges[0]->upper_value.get(),
           ranges[0]->upper_value_size);
    merged_range->upper_value_size = ranges[0]->upper_value_size;
  }

  // loop through ranges and set upper/lower maxima/minima
  for (auto &range : ranges) {
    if (range->lower_value != nullptr) {
      if (merged_range->lower_value == nullptr) {
        merged_range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
            std::malloc(range->lower_value_size), &std::free);
        memcpy(merged_range->lower_value.get(), range->lower_value.get(),
               range->lower_value_size);
        merged_range->lower_value_size = range->lower_value_size;
        // See if the current range has a higher low value than the "merged"
        // range, if so set the new low value, since the current range has a
        // more restrictive condition
      } else if (memcmp(merged_range->lower_value.get(),
                        range->lower_value.get(),
                        std::min(merged_range->lower_value_size,
                                 range->lower_value_size)) == 1) {
        // Check if we need to reallocate
        if (merged_range->lower_value_size < range->lower_value_size) {
          merged_range->lower_value =
              std::unique_ptr<void, decltype(&std::free)>(
                  std::malloc(range->lower_value_size), &std::free);
        }

        memcpy(merged_range->lower_value.get(), range->lower_value.get(),
               range->lower_value_size);
        merged_range->lower_value_size = range->lower_value_size;
      }
    }

    if (range->upper_value != nullptr) {
      if (merged_range->upper_value == nullptr) {
        merged_range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
            std::malloc(range->upper_value_size), &std::free);
        memcpy(merged_range->upper_value.get(), range->upper_value.get(),
               range->upper_value_size);
        merged_range->upper_value_size = range->upper_value_size;
        // See if the current range has a lower upper value than the "merged"
        // range, if so set the new upper value since the current range has a
        // more restrictive condition
      } else if (memcmp(range->upper_value.get(),
                        merged_range->upper_value.get(),
                        std::min(merged_range->upper_value_size,
                                 range->upper_value_size)) == -1) {
        // Check if we need to reallocate
        if (merged_range->upper_value_size < range->upper_value_size) {
          merged_range->upper_value =
              std::unique_ptr<void, decltype(&std::free)>(
                  std::malloc(range->upper_value_size), &std::free);
        }

        memcpy(merged_range->upper_value.get(), range->upper_value.get(),
               range->upper_value_size);
        merged_range->upper_value_size = range->upper_value_size;
      }
    }
  }

  // If we have set the upper and lower let's make it a between.
  if (merged_range != nullptr && merged_range->upper_value != nullptr &&
      merged_range->lower_value != nullptr) {
    merged_range->operation_type = Item_func::BETWEEN;
  }

  return merged_range;
}

std::shared_ptr<tile::range>
tile::merge_ranges(const std::vector<std::shared_ptr<tile::range>> &ranges,
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
  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH:
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK:
  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
  case tiledb_datatype_t::TILEDB_DATETIME_HR:
  case tiledb_datatype_t::TILEDB_DATETIME_MIN:
  case tiledb_datatype_t::TILEDB_DATETIME_SEC:
  case tiledb_datatype_t::TILEDB_DATETIME_MS:
  case tiledb_datatype_t::TILEDB_DATETIME_US:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS:
    return merge_ranges<int64_t>(ranges);

  case tiledb_datatype_t::TILEDB_UINT64:
    return merge_ranges<uint64_t>(ranges);

  case tiledb_datatype_t::TILEDB_STRING_ASCII:
    return merge_ranges_str(ranges);

    // Uncomment to support BLOB dimensions
    // case tiledb_datatype_t::TILEDB_BLOB:
    //  return merge_ranges<std::byte>(ranges);

  case tiledb_datatype_t::TILEDB_BOOL:
    return merge_ranges<bool>(ranges);

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

std::shared_ptr<tile::range> tile::merge_ranges_to_super(
    const std::vector<std::shared_ptr<tile::range>> &ranges,
    tiledb_datatype_t datatype) {
  if (ranges.empty() || ranges[0] == nullptr) {
    return nullptr;
  }

  switch (datatype) {
  case tiledb_datatype_t::TILEDB_FLOAT64:
    return merge_ranges_to_super<double>(ranges);

  case tiledb_datatype_t::TILEDB_FLOAT32:
    return merge_ranges_to_super<float>(ranges);

  case tiledb_datatype_t::TILEDB_INT8:
    return merge_ranges_to_super<int8_t>(ranges);

  case tiledb_datatype_t::TILEDB_UINT8:
    return merge_ranges_to_super<uint8_t>(ranges);

  case tiledb_datatype_t::TILEDB_INT16:
    return merge_ranges_to_super<int16_t>(ranges);

  case tiledb_datatype_t::TILEDB_UINT16:
    return merge_ranges_to_super<uint16_t>(ranges);

  case tiledb_datatype_t::TILEDB_INT32:
    return merge_ranges_to_super<int32_t>(ranges);

  case tiledb_datatype_t::TILEDB_UINT32:
    return merge_ranges_to_super<uint32_t>(ranges);

  case tiledb_datatype_t::TILEDB_INT64:
  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH:
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK:
  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
  case tiledb_datatype_t::TILEDB_DATETIME_HR:
  case tiledb_datatype_t::TILEDB_DATETIME_MIN:
  case tiledb_datatype_t::TILEDB_DATETIME_SEC:
  case tiledb_datatype_t::TILEDB_DATETIME_MS:
  case tiledb_datatype_t::TILEDB_DATETIME_US:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS:
    return merge_ranges_to_super<int64_t>(ranges);

  case tiledb_datatype_t::TILEDB_UINT64:
    return merge_ranges_to_super<uint64_t>(ranges);

  case tiledb_datatype_t::TILEDB_STRING_ASCII:
    return merge_ranges_to_super<char>(ranges);

    // Uncomment to support BLOB dimensions
    // case tiledb_datatype_t::TILEDB_BLOB:
    //  return merge_ranges_to_super<std::byte>(ranges);

  case tiledb_datatype_t::TILEDB_BOOL:
    return merge_ranges_to_super<bool>(ranges);

  default: {
    const char *datatype_str;
    tiledb_datatype_to_str(datatype, &datatype_str);
    my_printf_error(
        ER_UNKNOWN_ERROR,
        "Unknown or unsupported tiledb data type in merge_ranges_to_super: %s",
        ME_ERROR_LOG | ME_FATAL, datatype_str);
  }
  }

  return nullptr;
}

void tile::setup_range(
    THD *thd, const std::shared_ptr<range> &range,
    const std::pair<std::string, std::string> &non_empty_domain,
    const tiledb::Dimension &dimension) {
  switch (dimension.type()) {
  case TILEDB_STRING_ASCII:
    switch (range->operation_type) {
    case Item_func::IN_FUNC: /* IN is treated like equal */
    case Item_func::BETWEEN: /* BETWEEN Is treated like equal */
    case Item_func::EQUAL_FUNC:
    case Item_func::EQ_FUNC:

      // When we are dealing with equality, all we need to do is to convert the
      // value from the mysql types (double/longlong) to the actual datatypes
      // TileDB is expecting

      // cast to proper tiledb datatype
      //      final_lower_value = static_cast<char*>(range->lower_value.get());
      //
      //      range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
      //          std::malloc(range->lower_value_size), &std::free);
      //      memcpy(range->lower_value.get(), &final_lower_value,
      //      range->lower_value_size);
      //
      //      // cast to proper tiledb datatype
      //      final_upper_value = static_cast<char*>(range->upper_value.get());
      //
      //      range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
      //          std::malloc(range->upper_value_size), &std::free);
      //      memcpy(range->upper_value.get(), &final_upper_value,
      //      range->upper_value_size);

      break;
    case Item_func::LT_FUNC: {
      my_printf_error(
          ER_UNKNOWN_ERROR,
          "Range is less than, this should not happen in setup_ranges",
          ME_ERROR_LOG | ME_FATAL);
      break;
    }
    case Item_func::LE_FUNC: {
      range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
          std::malloc(sizeof(char) * non_empty_domain.first.size()),
          &std::free);
      memcpy(range->lower_value.get(), non_empty_domain.first.c_str(),
             non_empty_domain.first.size());
      range->lower_value_size = non_empty_domain.first.size();

      break;
    }
    case Item_func::GE_FUNC: {
      range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
          std::malloc(sizeof(char) * non_empty_domain.second.size()),
          &std::free);
      memcpy(range->upper_value.get(), non_empty_domain.second.c_str(),
             non_empty_domain.second.size());
      range->upper_value_size = non_empty_domain.second.size();

      break;
    }
    case Item_func::GT_FUNC: {
      my_printf_error(
          ER_UNKNOWN_ERROR,
          "Range is greater than, this should not happen in setup_ranges",
          ME_ERROR_LOG | ME_FATAL);
      break;
    }
    case Item_func::NE_FUNC: /* Not equal is not supported */
    default:
      break; // DBUG_RETURN(NULL);
    }        // endswitch functype

    // log conditions for debug
    log_debug(thd, "pushed string conditions: [%s, %s]",
              std::string(static_cast<char *>(range->lower_value.get()),
                          range->lower_value_size)
                  .c_str(),
              std::string(static_cast<char *>(range->upper_value.get()),
                          range->upper_value_size)
                  .c_str());
    break;
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

void tile::setup_range(THD *thd, const std::shared_ptr<range> &range,
                       void *non_empty_domain,
                       const tiledb::Dimension &dimension) {
  switch (dimension.type()) {
  case tiledb_datatype_t::TILEDB_FLOAT64:
    return setup_range<double>(thd, range,
                               static_cast<double *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_FLOAT32:
    return setup_range<float>(thd, range,
                              static_cast<float *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_INT8:
    return setup_range<int8_t>(thd, range,
                               static_cast<int8_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_UINT8:
    return setup_range<uint8_t>(thd, range,
                                static_cast<uint8_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_INT16:
    return setup_range<int16_t>(thd, range,
                                static_cast<int16_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_UINT16:
    return setup_range<uint16_t>(thd, range,
                                 static_cast<uint16_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_INT32:
    return setup_range<int32_t>(thd, range,
                                static_cast<int32_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_UINT32:
    return setup_range<uint32_t>(thd, range,
                                 static_cast<uint32_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_INT64:
  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH:
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK:
  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
  case tiledb_datatype_t::TILEDB_DATETIME_HR:
  case tiledb_datatype_t::TILEDB_DATETIME_MIN:
  case tiledb_datatype_t::TILEDB_DATETIME_SEC:
  case tiledb_datatype_t::TILEDB_DATETIME_MS:
  case tiledb_datatype_t::TILEDB_DATETIME_US:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS:
    return setup_range<int64_t>(thd, range,
                                static_cast<int64_t *>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_UINT64:
    return setup_range<uint64_t>(thd, range,
                                 static_cast<uint64_t *>(non_empty_domain));

    // Uncomment to support BLOB dimensions
    // case tiledb_datatype_t::TILEDB_BLOB:
    //  return setup_range<std::byte>(thd, range,
    //                             static_cast<std::byte*>(non_empty_domain));

  case tiledb_datatype_t::TILEDB_BOOL:
    return setup_range<bool>(thd, range, static_cast<bool *>(non_empty_domain));
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

int tile::set_range_from_item_consts(THD *thd, Item_basic_constant *lower_const,
                                     Item_basic_constant *upper_const,
                                     Item_result cmp_type,
                                     std::shared_ptr<range> &range,
                                     tiledb_datatype_t datatype) {
  DBUG_ENTER("tile::set_range_from_item_costs");

  switch (datatype) {
  case tiledb_datatype_t::TILEDB_FLOAT64:
    tile::set_range_from_item_consts<double>(thd, lower_const, upper_const,
                                             cmp_type, range, datatype);
    break;
  case tiledb_datatype_t::TILEDB_FLOAT32:
    tile::set_range_from_item_consts<float>(thd, lower_const, upper_const,
                                            cmp_type, range, datatype);
    break;
  case tiledb_datatype_t::TILEDB_INT8:
    tile::set_range_from_item_consts<int8_t>(thd, lower_const, upper_const,
                                             cmp_type, range, datatype);
    break;
  case tiledb_datatype_t::TILEDB_UINT8:
    tile::set_range_from_item_consts<uint8_t>(thd, lower_const, upper_const,
                                              cmp_type, range, datatype);
    break;
  case tiledb_datatype_t::TILEDB_INT16:
    tile::set_range_from_item_consts<int16_t>(thd, lower_const, upper_const,
                                              cmp_type, range, datatype);
    break;
  case tiledb_datatype_t::TILEDB_UINT16:
    tile::set_range_from_item_consts<uint16_t>(thd, lower_const, upper_const,
                                               cmp_type, range, datatype);
    break;
  case tiledb_datatype_t::TILEDB_INT32:
    tile::set_range_from_item_consts<int32_t>(thd, lower_const, upper_const,
                                              cmp_type, range, datatype);
    break;
  case tiledb_datatype_t::TILEDB_UINT32:
    tile::set_range_from_item_consts<uint32_t>(thd, lower_const, upper_const,
                                               cmp_type, range, datatype);
    break;
  case tiledb_datatype_t::TILEDB_INT64:
  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH:
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK:
  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
  case tiledb_datatype_t::TILEDB_DATETIME_HR:
  case tiledb_datatype_t::TILEDB_DATETIME_MIN:
  case tiledb_datatype_t::TILEDB_DATETIME_SEC:
  case tiledb_datatype_t::TILEDB_DATETIME_MS:
  case tiledb_datatype_t::TILEDB_DATETIME_US:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS:
    tile::set_range_from_item_consts<int64_t>(thd, lower_const, upper_const,
                                              cmp_type, range, datatype);
    break;
  case tiledb_datatype_t::TILEDB_UINT64:
    tile::set_range_from_item_consts<uint64_t>(thd, lower_const, upper_const,
                                               cmp_type, range, datatype);
    break;
  case tiledb_datatype_t::TILEDB_CHAR:
  case tiledb_datatype_t::TILEDB_STRING_ASCII:
  case tiledb_datatype_t::TILEDB_STRING_UTF8:
    tile::set_range_from_item_consts<char>(thd, lower_const, upper_const,
                                           cmp_type, range, datatype);
    break;
  // Uncomment to support BLOB dimensions
  // case tiledb_datatype_t::TILEDB_BLOB:
  //  tile::set_range_from_item_consts<std::byte>(thd, lower_const, upper_const,
  //                                           cmp_type, range, datatype);
  //  break;
  case tiledb_datatype_t::TILEDB_BOOL:
    tile::set_range_from_item_consts<bool>(thd, lower_const, upper_const,
                                           cmp_type, range, datatype);
    break;

  default: {
    DBUG_RETURN(1);
  }
  }

  DBUG_RETURN(0);
}

int tile::set_range_from_item_datetime(THD *thd, Item *lower_const,
                                       Item *upper_const, Item_result cmp_type,
                                       std::shared_ptr<range> &range,
                                       tiledb_datatype_t datatype) {
  DBUG_ENTER("tile::set_range_from_item_datetime");

  MYSQL_TIME lower_date, upper_date;
  int64_t lower = 0;
  int64_t upper = 0;
  switch (datatype) {

  case tiledb_datatype_t::TILEDB_DATETIME_YEAR: {
    if (lower_const != nullptr) {
      lower_date = {static_cast<uint32_t>(lower_const->val_int()),
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    MYSQL_TIMESTAMP_TIME};

      lower = MysqlTimeToTileDBTimeVal(thd, lower_date, datatype);
    }
    if (upper_const != nullptr) {
      upper_date = {static_cast<uint32_t>(upper_const->val_int()),
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    MYSQL_TIMESTAMP_TIME};

      upper = MysqlTimeToTileDBTimeVal(thd, upper_date, datatype);
    }

    break;
  }
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH:
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK:
  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
  case tiledb_datatype_t::TILEDB_DATETIME_HR:
  case tiledb_datatype_t::TILEDB_DATETIME_MIN:
  case tiledb_datatype_t::TILEDB_DATETIME_SEC:
  case tiledb_datatype_t::TILEDB_DATETIME_MS:
  case tiledb_datatype_t::TILEDB_DATETIME_US:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS:
  case tiledb_datatype_t::TILEDB_TIME_HR:
  case tiledb_datatype_t::TILEDB_TIME_MIN:
  case tiledb_datatype_t::TILEDB_TIME_SEC:
  case tiledb_datatype_t::TILEDB_TIME_MS:
  case tiledb_datatype_t::TILEDB_TIME_US:
  case tiledb_datatype_t::TILEDB_TIME_NS:
  case tiledb_datatype_t::TILEDB_TIME_PS:
  case tiledb_datatype_t::TILEDB_TIME_FS:
  case tiledb_datatype_t::TILEDB_TIME_AS: {
    if (lower_const != nullptr) {
      lower_const->get_date(thd, &lower_date, date_mode_t(0));
      lower = MysqlTimeToTileDBTimeVal(thd, lower_date, datatype);
    }
    if (upper_const != nullptr) {
      upper_const->get_date(thd, &upper_date, date_mode_t(0));
      upper = MysqlTimeToTileDBTimeVal(thd, upper_date, datatype);
    }
    break;
  }
  default: {
    DBUG_RETURN(1);
  }
  }

  range->datatype = tiledb_datatype_t::TILEDB_INT64;

  if (lower_const != nullptr) {
    // If we have greater than, lets make it greater than or equal
    // TileDB ranges are inclusive
    if (range->operation_type == Item_func::GT_FUNC) {
      range->operation_type = Item_func::GE_FUNC;
      lower += 1;
    }

    range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(int64_t)), &std::free);
    *static_cast<int64_t *>(range->lower_value.get()) =
        static_cast<int64_t>(lower);
    range->lower_value_size = sizeof(int64_t);

        std::cout << "lower-" << static_cast<int64_t>(lower) << std::endl;

  }

  if (upper_const != nullptr) {
    // If we have less than, lets make it less than or equal
    // TileDB ranges are inclusive
    if (range->operation_type == Item_func::LT_FUNC) {
      range->operation_type = Item_func::LE_FUNC;
      upper -= 1;
    }

    range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(int64_t)), &std::free);
    *static_cast<int64_t *>(range->upper_value.get()) =
        static_cast<int64_t>(upper);
    range->upper_value_size = sizeof(int64_t);

    std::cout << "upper-" << static_cast<int64_t>(upper) << std::endl;
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
  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH:
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK:
  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
  case tiledb_datatype_t::TILEDB_DATETIME_HR:
  case tiledb_datatype_t::TILEDB_DATETIME_MIN:
  case tiledb_datatype_t::TILEDB_DATETIME_SEC:
  case tiledb_datatype_t::TILEDB_DATETIME_MS:
  case tiledb_datatype_t::TILEDB_DATETIME_US:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS:
    return get_unique_non_contained_in_ranges<int64_t>(in_ranges, main_range);

  case tiledb_datatype_t::TILEDB_UINT64:
    return get_unique_non_contained_in_ranges<uint64_t>(in_ranges, main_range);

  case tiledb_datatype_t::TILEDB_STRING_ASCII:
    return get_unique_non_contained_in_ranges_str(in_ranges, main_range);

    // Uncomment to support BLOB dimensions
    // case tiledb_datatype_t::TILEDB_BLOB:
    //  return get_unique_non_contained_in_ranges<std::byte>(in_ranges,
    //  main_range);

  case tiledb_datatype_t::TILEDB_BOOL:
    return get_unique_non_contained_in_ranges<bool>(in_ranges, main_range);

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

std::vector<std::shared_ptr<tile::range>>
tile::get_unique_non_contained_in_ranges_str(
    const std::vector<std::shared_ptr<tile::range>> &in_ranges,
    const std::shared_ptr<tile::range> &main_range) {

  // Return unique non contained ranges
  std::vector<std::shared_ptr<tile::range>> ret;

  std::unordered_set<std::string> unique_values_set;
  std::vector<std::string> unique_values_vec;

  // get datatype
  tiledb_datatype_t datatype;
  if (main_range != nullptr) {
    datatype = main_range->datatype;
  } else if (!in_ranges.empty()) {
    datatype = in_ranges[0]->datatype;
  }

  // Only set main range value if not null
  char *main_lower_value;
  char *main_upper_value;
  uint64_t main_lower_value_size;
  uint64_t main_upper_value_size;
  if (main_range != nullptr) {
    main_lower_value = static_cast<char *>(main_range->lower_value.get());
    main_lower_value_size = main_range->lower_value_size;
    main_upper_value = static_cast<char *>(main_range->upper_value.get());
    main_upper_value_size = main_range->upper_value_size;
  }

  for (auto &range : in_ranges) {
    // lower and upper values are equal, so just grab the lower
    // for in clauses, every values is set as a equality range
    char *range_lower_value = static_cast<char *>(range->lower_value.get());

    // Check for contained range if main range is non null
    if (main_range != nullptr) {
      int cmp_lower =
          memcmp(main_lower_value, range_lower_value,
                 static_cast<size_t>(
                     std::min(range->lower_value_size, main_lower_value_size)));
      int cmp_upper =
          memcmp(range_lower_value, main_upper_value,
                 static_cast<size_t>(
                     std::min(range->lower_value_size, main_upper_value_size)));
      // If the range is contained, skip it
      if ((cmp_lower == -1 || cmp_lower == 0) &&
          (cmp_upper == 0 || cmp_upper == 1)) {
        continue;
      }
    }

    // Add value to set
    if (unique_values_set.count(
            std::string(range_lower_value, range->lower_value_size)) == 0) {
      unique_values_set.insert(
          std::string(range_lower_value, range->lower_value_size));
      unique_values_vec.emplace_back(range_lower_value,
                                     range->lower_value_size);
    }
  }

  // from unique values build final ranges
  for (std::string val : unique_values_vec) {
    // Build range pointer
    std::shared_ptr<tile::range> range =
        std::make_shared<tile::range>(tile::range{
            std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
            std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
            Item_func::EQ_FUNC, datatype, val.size(), val.size()});

    // Allocate memory for lower value
    range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(val.size()), &std::free);
    // Copy lower value
    memcpy(range->lower_value.get(), val.c_str(), val.size());

    // Allocate memory for upper value
    range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(val.size()), &std::free);
    // Copy upper value
    memcpy(range->upper_value.get(), val.c_str(), val.size());

    ret.push_back(std::move(range));
  }

  return ret;
}

Item_func::Functype tile::find_flag_to_func(enum ha_rkey_function find_flag,
                                            const bool start_key,
                                            const bool last_key_part) {
  switch (find_flag) {
  case HA_READ_KEY_EXACT:
    return Item_func::Functype::EQ_FUNC;
  case HA_READ_KEY_OR_NEXT:
    return Item_func::Functype::GE_FUNC;
  case HA_READ_KEY_OR_PREV:
    return Item_func::Functype::LE_FUNC;
  case HA_READ_AFTER_KEY:
    if (start_key)
      return Item_func::Functype::GT_FUNC;
    return Item_func::Functype::LE_FUNC;
  case HA_READ_PREFIX_LAST:
  case HA_READ_PREFIX_LAST_OR_PREV:
  case HA_READ_BEFORE_KEY:
    if (last_key_part)
      return Item_func::Functype::LT_FUNC;
    return Item_func::Functype::LE_FUNC;
  case HA_READ_PREFIX:
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

std::map<uint64_t, std::shared_ptr<tile::range>>
tile::build_ranges_from_key(THD *thd, const TABLE *table, const uchar *key,
                            uint length, enum ha_rkey_function find_flag,
                            const bool start_key,
                            const tiledb::Domain &domain) {

  // Length shouldn't be zero here but better safe then segfault!
  if (length == 0)
    return {};

  const KEY *key_info = table->key_info;

  std::map<uint64_t, std::shared_ptr<tile::range>> ranges;
  uint64_t key_offset = 0;

  for (uint64_t key_part_index = 0;
       key_part_index < key_info->user_defined_key_parts; key_part_index++) {

    if (key_offset >= length) {
      break;
    }

    const KEY_PART_INFO *key_part_info = &(key_info->key_part[key_part_index]);

    tiledb_datatype_t datatype = domain.dimension(key_part_index).type();

    uint64_t key_len = 0;
    if (datatype == tiledb_datatype_t::TILEDB_STRING_ASCII) {
      const uint16_t char_length =
          *reinterpret_cast<const uint16_t *>(key + key_offset);
      key_len += sizeof(uint16_t);
      key_len += sizeof(char) * char_length;
    } else {
      key_len = key_part_info->length;
    }

    bool last_key_part = (key_offset + key_len) >= length;

    switch (datatype) {
    case tiledb_datatype_t::TILEDB_FLOAT64: {
      ranges[key_part_index] =
          build_range_from_key<double>(key + key_offset, length, find_flag,
                                       start_key, last_key_part, datatype);
      break;
    }

    case tiledb_datatype_t::TILEDB_FLOAT32: {
      ranges[key_part_index] =
          build_range_from_key<float>(key + key_offset, length, find_flag,
                                      start_key, last_key_part, datatype);
      break;
    }

    case tiledb_datatype_t::TILEDB_INT8: {
      ranges[key_part_index] =
          build_range_from_key<int8_t>(key + key_offset, length, find_flag,
                                       start_key, last_key_part, datatype);
      break;
    }

    case tiledb_datatype_t::TILEDB_UINT8: {
      ranges[key_part_index] =
          build_range_from_key<uint8_t>(key + key_offset, length, find_flag,
                                        start_key, last_key_part, datatype);
      break;
    }

    case tiledb_datatype_t::TILEDB_INT16: {
      ranges[key_part_index] =
          build_range_from_key<int16_t>(key + key_offset, length, find_flag,
                                        start_key, last_key_part, datatype);
      break;
    }

    case tiledb_datatype_t::TILEDB_UINT16: {
      ranges[key_part_index] =
          build_range_from_key<uint16_t>(key + key_offset, length, find_flag,
                                         start_key, last_key_part, datatype);
      break;
    }

    case tiledb_datatype_t::TILEDB_INT32: {
      ranges[key_part_index] =
          build_range_from_key<int32_t>(key + key_offset, length, find_flag,
                                        start_key, last_key_part, datatype);
      break;
    }

    case tiledb_datatype_t::TILEDB_UINT32: {
      ranges[key_part_index] =
          build_range_from_key<uint32_t>(key + key_offset, length, find_flag,
                                         start_key, last_key_part, datatype);
      break;
    }

    case tiledb_datatype_t::TILEDB_INT64: {
      ranges[key_part_index] =
          build_range_from_key<int64_t>(key + key_offset, length, find_flag,
                                        start_key, last_key_part, datatype);
      break;
    }

    case tiledb_datatype_t::TILEDB_DATETIME_YEAR: {
      // XXX: for some reason maria uses year offset from 1900 here
      MYSQL_TIME mysql_time = {1900U + *((uint8_t *)(key + key_offset)),
                               0,
                               0,
                               0,
                               0,
                               0,
                               0,
                               0,
                               MYSQL_TIMESTAMP_TIME};
      int64_t xs = MysqlTimeToTileDBTimeVal(thd, mysql_time, datatype);

      ranges[key_part_index] = build_range_from_key<int64_t>(
          (uchar *)&xs, length, find_flag, start_key, last_key_part, datatype);
      break;
    }
    case tiledb_datatype_t::TILEDB_DATETIME_MONTH:
    case tiledb_datatype_t::TILEDB_DATETIME_WEEK:
    case tiledb_datatype_t::TILEDB_DATETIME_DAY:
    case tiledb_datatype_t::TILEDB_DATETIME_HR:
    case tiledb_datatype_t::TILEDB_DATETIME_MIN:
    case tiledb_datatype_t::TILEDB_DATETIME_SEC:
    case tiledb_datatype_t::TILEDB_DATETIME_MS:
    case tiledb_datatype_t::TILEDB_DATETIME_US:
    case tiledb_datatype_t::TILEDB_DATETIME_NS:
    case tiledb_datatype_t::TILEDB_DATETIME_PS:
    case tiledb_datatype_t::TILEDB_DATETIME_FS:
    case tiledb_datatype_t::TILEDB_DATETIME_AS:
    case tiledb_datatype_t::TILEDB_TIME_HR:
    case tiledb_datatype_t::TILEDB_TIME_MIN:
    case tiledb_datatype_t::TILEDB_TIME_SEC:
    case tiledb_datatype_t::TILEDB_TIME_MS:
    case tiledb_datatype_t::TILEDB_TIME_US:
    case tiledb_datatype_t::TILEDB_TIME_NS:
    case tiledb_datatype_t::TILEDB_TIME_PS:
    case tiledb_datatype_t::TILEDB_TIME_FS:
    case tiledb_datatype_t::TILEDB_TIME_AS: {
      MYSQL_TIME mysql_time;
      Field *field = key_part_info->field;

      uchar *tmp = field->ptr;
      field->ptr = (uchar *)(key + key_offset);
      field->get_date(&mysql_time, date_mode_t(0));
      field->ptr = tmp;
      int64_t xs = MysqlTimeToTileDBTimeVal(thd, mysql_time, datatype);

      ranges[key_part_index] = build_range_from_key<int64_t>(
          (uchar *)&xs, length, find_flag, start_key, last_key_part, datatype);
      break;
    }

    case tiledb_datatype_t::TILEDB_UINT64: {
      ranges[key_part_index] =
          build_range_from_key<uint64_t>(key + key_offset, length, find_flag,
                                         start_key, last_key_part, datatype);
      break;
    }

    case tiledb_datatype_t::TILEDB_STRING_ASCII: {
      const uint16_t char_length =
          *reinterpret_cast<const uint16_t *>(key + key_offset);
      // If there is no string set the range to nullptr
      if (char_length == 0) {
        ranges[key_part_index] = nullptr;
        break;
      }
      key_offset += sizeof(uint16_t);
      ranges[key_part_index] = build_range_from_key<char>(
          key + key_offset, length, find_flag, start_key, last_key_part,
          datatype, char_length);
      break;
    }

      // Uncomment to support BLOB dimensions
      //  case tiledb_datatype_t::TILEDB_BLOB: {
      //    ranges[key_part_index] =
      //        build_range_from_key<std::byte>(key + key_offset, length,
      //        find_flag,
      //                                     start_key, last_key_part,
      //                                     datatype);
      //    break;
      //  }
    case tiledb_datatype_t::TILEDB_BOOL: {
      ranges[key_part_index] =
          build_range_from_key<bool>(key + key_offset, length, find_flag,
                                     start_key, last_key_part, datatype);
      break;
    }

    default: {
      const char *datatype_str;
      tiledb_datatype_to_str(datatype, &datatype_str);
      my_printf_error(ER_UNKNOWN_ERROR,
                      "Unknown or unsupported tiledb data type in "
                      "build_ranges_from_key: %s",
                      ME_ERROR_LOG | ME_FATAL, datatype_str);
    }
    }

    key_offset += key_len;
  }
  return ranges;
}

void tile::update_range_from_key_for_super_range(
    std::shared_ptr<tile::range> &range, key_range key, uint64_t key_offset,
    const bool start_key, const bool last_key_part, tiledb_datatype_t datatype,
    THD *thd, Field *field) {
  assert(thd);
  assert(field);
  // Length shouldn't be zero here but better safe then segfault!
  if (key.length == 0)
    return;

  switch (datatype) {
  case tiledb_datatype_t::TILEDB_FLOAT64:
    return update_range_from_key_for_super_range<double>(
        range, key, key_offset, start_key, last_key_part);

  case tiledb_datatype_t::TILEDB_FLOAT32:
    return update_range_from_key_for_super_range<float>(
        range, key, key_offset, start_key, last_key_part);

  case tiledb_datatype_t::TILEDB_INT8:
    return update_range_from_key_for_super_range<int8_t>(
        range, key, key_offset, start_key, last_key_part);

  case tiledb_datatype_t::TILEDB_UINT8:
    return update_range_from_key_for_super_range<uint8_t>(
        range, key, key_offset, start_key, last_key_part);

  case tiledb_datatype_t::TILEDB_INT16:
    return update_range_from_key_for_super_range<int16_t>(
        range, key, key_offset, start_key, last_key_part);

  case tiledb_datatype_t::TILEDB_UINT16:
    return update_range_from_key_for_super_range<uint16_t>(
        range, key, key_offset, start_key, last_key_part);

  case tiledb_datatype_t::TILEDB_INT32:
    return update_range_from_key_for_super_range<int32_t>(
        range, key, key_offset, start_key, last_key_part);

  case tiledb_datatype_t::TILEDB_UINT32:
    return update_range_from_key_for_super_range<uint32_t>(
        range, key, key_offset, start_key, last_key_part);

  case tiledb_datatype_t::TILEDB_DATETIME_YEAR: {
    // XXX: for some reason maria uses year offset from 1900 here
    MYSQL_TIME mysql_time = {1900U + *((uint8_t *)(key.key + key_offset)),
                             0,
                             0,
                             0,
                             0,
                             0,
                             0,
                             0,
                             MYSQL_TIMESTAMP_TIME};
    int64_t xs = MysqlTimeToTileDBTimeVal(thd, mysql_time, datatype);
    struct st_key_range local_key {};
    local_key.length = sizeof(int64_t);
    local_key.key = reinterpret_cast<const uchar *>(&xs);
    local_key.flag = key.flag;
    local_key.keypart_map = key.keypart_map;
    return update_range_from_key_for_super_range<int64_t>(
        range, local_key, 0, start_key, last_key_part);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH:
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK:
  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
  case tiledb_datatype_t::TILEDB_DATETIME_HR:
  case tiledb_datatype_t::TILEDB_DATETIME_MIN:
  case tiledb_datatype_t::TILEDB_DATETIME_SEC:
  case tiledb_datatype_t::TILEDB_DATETIME_MS:
  case tiledb_datatype_t::TILEDB_DATETIME_US:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS: {
    MYSQL_TIME mysql_time;

    uchar *tmp = field->ptr;
    field->ptr = (uchar *)(key.key + key_offset);
    field->get_date(&mysql_time, date_mode_t(0));
    field->ptr = tmp;
    int64_t xs = MysqlTimeToTileDBTimeVal(thd, mysql_time, datatype);

    struct st_key_range local_key {};
    local_key.length = sizeof(int64_t);
    local_key.key = reinterpret_cast<const uchar *>(&xs);
    local_key.flag = key.flag;
    local_key.keypart_map = key.keypart_map;
    return update_range_from_key_for_super_range<int64_t>(
        range, local_key, 0, start_key, last_key_part);
  }
  case tiledb_datatype_t::TILEDB_INT64:
    return update_range_from_key_for_super_range<int64_t>(
        range, key, key_offset, start_key, last_key_part);

  case tiledb_datatype_t::TILEDB_UINT64:
    return update_range_from_key_for_super_range<uint64_t>(
        range, key, key_offset, start_key, last_key_part);

  case tiledb_datatype_t::TILEDB_STRING_ASCII: {
    const uint16_t char_length =
        *reinterpret_cast<const uint16_t *>(key.key + key_offset);
    key_offset += sizeof(uint16_t);
    return update_range_from_key_for_super_range<char>(
        range, key, key_offset, start_key, last_key_part, char_length);
  }

    // Uncomment to support BLOB dimensions
    // case tiledb_datatype_t::TILEDB_BLOB:
    //  return update_range_from_key_for_super_range<std::byte>(
    //      range, key, key_offset, start_key, last_key_part);

  case tiledb_datatype_t::TILEDB_BOOL:
    return update_range_from_key_for_super_range<bool>(
        range, key, key_offset, start_key, last_key_part);

  default: {
    const char *datatype_str;
    tiledb_datatype_to_str(datatype, &datatype_str);
    my_printf_error(ER_UNKNOWN_ERROR,
                    "Unknown or unsupported tiledb data type in "
                    "update_range_from_key_for_super_range: %s",
                    ME_ERROR_LOG | ME_FATAL, datatype_str);
  }
  }
}

void tile::build_subarray(
    THD *thd, const bool &valid_ranges, const bool &valid_in_ranges,
    int &empty_read, const tiledb::Domain &domain,
    const std::vector<std::vector<std::shared_ptr<tile::range>>>
        &pushdown_ranges,
    const std::vector<std::vector<std::shared_ptr<tile::range>>>
        &pushdown_in_ranges,
    std::unique_ptr<tiledb::Subarray> &subarray, tiledb::Context *ctx,
    tiledb::Array *array) {

  auto dims = domain.dimensions();
  uint64_t ndim = domain.ndim();

  std::vector<std::unique_ptr<void, decltype(&std::free)>> nonEmptyDomains;
  for (uint64_t dim_idx = 0; dim_idx < ndim; dim_idx++) {
    nonEmptyDomains.emplace_back(
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free));
  }

  std::vector<std::pair<std::string, std::string>> nonEmptyDomainVars(ndim);
  for (uint64_t dim_idx = 0; dim_idx < ndim; dim_idx++) {
    tiledb::Dimension dimension = domain.dimension(dim_idx);

    if (dimension.cell_val_num() == TILEDB_VAR_NUM) {
      nonEmptyDomainVars[dim_idx] = array->non_empty_domain_var(dim_idx);
    } else {
      uint64_t size = (tiledb_datatype_size(dimension.type()) * 2);
      auto nonEmptyDomain = std::unique_ptr<void, decltype(&std::free)>(
          std::malloc(size), &std::free);
      ctx->handle_error(tiledb_array_get_non_empty_domain_from_index(
          ctx->ptr().get(), array->ptr().get(), dim_idx, nonEmptyDomain.get(),
          &empty_read));

      nonEmptyDomains[dim_idx] = std::move(nonEmptyDomain);
    }
  }

  // if no ranges to add
  if (!valid_ranges && !valid_in_ranges) { // No pushdown
    if (empty_read) {
      return;
    }

    for (uint64_t dim_idx = 0; dim_idx < domain.ndim(); dim_idx++) {
      tiledb::Dimension dimension = domain.dimension(dim_idx);

      if (dimension.cell_val_num() == TILEDB_VAR_NUM) {
        auto &pair = nonEmptyDomainVars[dim_idx];
        subarray->add_range(dim_idx, pair.first, pair.second);
      } else {
        void *lower = static_cast<char *>(nonEmptyDomains[dim_idx].get());
        void *upper = static_cast<char *>(nonEmptyDomains[dim_idx].get()) +
                      tiledb_datatype_size(dimension.type());
        // set range
        ctx->handle_error(
            tiledb_subarray_add_range(ctx->ptr().get(), subarray->ptr().get(),
                                      dim_idx, lower, upper, nullptr));
      }
    }
  } else {
    // Loop over dimensions and build ranges for that dimension
    for (uint64_t dim_idx = 0; dim_idx < domain.ndim(); dim_idx++) {
      tiledb::Dimension dimension = domain.dimension(dim_idx);

      // This ranges vector and the ranges it contains will be manipulated in
      // merge ranges
      const auto &ranges = pushdown_ranges[dim_idx];

      // If there is valid ranges from IN clause, first we must see if they
      // are contained inside the merged super range we have
      const auto &in_ranges = pushdown_in_ranges[dim_idx];

      if (dimension.cell_val_num() == TILEDB_VAR_NUM) {
        auto &non_empty_domain = nonEmptyDomainVars[dim_idx];
        // If the ranges for this dimension are not empty, we'll push it down
        // else non empty domain is used
        if (!ranges.empty() || !in_ranges.empty()) {
          std::shared_ptr<tile::range> range = nullptr;
          if (!ranges.empty()) {
            // Merge multiple ranges into single super range
            range = merge_ranges(ranges, dims[dim_idx].type());

            if (range != nullptr) {
              // Setup the range by filling in missing values with non empty
              // domain
              setup_range(thd, range, non_empty_domain, dims[dim_idx]);

              // set range
              ctx->handle_error(tiledb_subarray_add_range_var(
                  ctx->ptr().get(), subarray->ptr().get(), dim_idx,
                  range->lower_value.get(), range->lower_value_size,
                  range->upper_value.get(), range->upper_value_size));
            }
          }

          // If there are ranges from in conditions let's build proper ranges
          // for them
          if (!in_ranges.empty()) {
            // First make the in ranges unique and remove any which are
            // contained by the main range (if it is non null)
            auto unique_in_ranges =
                get_unique_non_contained_in_ranges(in_ranges, range);

            for (auto &in_range : unique_in_ranges) {
              // setup range so values are set to correct datatypes
              setup_range(thd, in_range, non_empty_domain, dims[dim_idx]);
              // set range
              ctx->handle_error(tiledb_subarray_add_range_var(
                  ctx->ptr().get(), subarray->ptr().get(), dim_idx,
                  in_range->lower_value.get(), in_range->lower_value_size,
                  in_range->upper_value.get(), in_range->upper_value_size));
            }
          }
        } else { // If the range is empty we need to use the non-empty-domain

          subarray->add_range(dim_idx, non_empty_domain.first,
                              non_empty_domain.second);
        }
      } else {
        // get start of non empty domain
        void *lower = static_cast<char *>(nonEmptyDomains[dim_idx].get());
        // If the ranges for this dimension are not empty, we'll push it down
        // else non empty domain is used
        if (!ranges.empty() || !in_ranges.empty()) {
          std::shared_ptr<tile::range> range = nullptr;
          if (!ranges.empty()) {
            // Merge multiple ranges into single super range
            range = merge_ranges(ranges, dims[dim_idx].type());

            if (range != nullptr) {
              // Setup the range by filling in missing values with non empty
              // domain
              setup_range(thd, range, lower, dims[dim_idx]);

              // set range
              ctx->handle_error(tiledb_subarray_add_range(
                  ctx->ptr().get(), subarray->ptr().get(), dim_idx,
                  range->lower_value.get(), range->upper_value.get(), nullptr));
            }
          }

          // If there are ranges from in conditions let's build proper ranges
          // for them
          if (!in_ranges.empty()) {
            // First make the in ranges unique and remove any which are
            // contained by the main range (if it is non null)
            auto unique_in_ranges =
                get_unique_non_contained_in_ranges(in_ranges, range);

            for (auto &in_range : unique_in_ranges) {
              // setup range so values are set to correct datatypes
              setup_range(thd, in_range, lower, dims[dim_idx]);
              // set range
              ctx->handle_error(tiledb_subarray_add_range(
                  ctx->ptr().get(), subarray->ptr().get(), dim_idx,
                  in_range->lower_value.get(), in_range->upper_value.get(),
                  nullptr));
            }
          }
        } else { // If the range is empty we need to use the non-empty-domain
          void *upper = static_cast<char *>(lower) +
                        tiledb_datatype_size(dimension.type());
          ctx->handle_error(
              tiledb_subarray_add_range(ctx->ptr().get(), subarray->ptr().get(),
                                        dim_idx, lower, upper, nullptr));
        }
      }
    }
  }
}

int8_t tile::compare_typed_buffers(const void *lhs, const void *rhs,
                                   uint64_t size, tiledb_datatype_t datatype) {
  // Length shouldn't be zero here but better safe then segfault!
  if (size == 0)
    return 0;

  switch (datatype) {
  case tiledb_datatype_t::TILEDB_FLOAT64:
    return compare_typed_buffers<double>(lhs, rhs, size);

  case tiledb_datatype_t::TILEDB_FLOAT32:
    return compare_typed_buffers<float>(lhs, rhs, size);

  case tiledb_datatype_t::TILEDB_INT8:
    return compare_typed_buffers<int8_t>(lhs, rhs, size);

  case tiledb_datatype_t::TILEDB_UINT8:
    return compare_typed_buffers<uint8_t>(lhs, rhs, size);

  case tiledb_datatype_t::TILEDB_INT16:
    return compare_typed_buffers<int16_t>(lhs, rhs, size);

  case tiledb_datatype_t::TILEDB_UINT16:
    return compare_typed_buffers<uint16_t>(lhs, rhs, size);

  case tiledb_datatype_t::TILEDB_INT32:
    return compare_typed_buffers<int32_t>(lhs, rhs, size);

  case tiledb_datatype_t::TILEDB_UINT32:
    return compare_typed_buffers<uint32_t>(lhs, rhs, size);

  case tiledb_datatype_t::TILEDB_INT64:
  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH:
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK:
  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
  case tiledb_datatype_t::TILEDB_DATETIME_HR:
  case tiledb_datatype_t::TILEDB_DATETIME_MIN:
  case tiledb_datatype_t::TILEDB_DATETIME_SEC:
  case tiledb_datatype_t::TILEDB_DATETIME_MS:
  case tiledb_datatype_t::TILEDB_DATETIME_US:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS:
    return compare_typed_buffers<int64_t>(lhs, rhs, size);

  case tiledb_datatype_t::TILEDB_UINT64:
    return compare_typed_buffers<uint64_t>(lhs, rhs, size);

    // Uncomment to support BLOB dimensions
    // case tiledb_datatype_t::TILEDB_BLOB:
    //  return compare_typed_buffers<std::byte>(lhs, rhs, size);

  case tiledb_datatype_t::TILEDB_BOOL:
    return compare_typed_buffers<bool>(lhs, rhs, size);

  default: {
    const char *datatype_str;
    tiledb_datatype_to_str(datatype, &datatype_str);
    my_printf_error(ER_UNKNOWN_ERROR,
                    "Unknown or unsupported tiledb data type in "
                    "compare_typed_buffers: %s",
                    ME_ERROR_LOG | ME_FATAL, datatype_str);
  }
  }

  return 0;
}

tiledb::QueryCondition
tile::range::QueryCondition(const tiledb::Context &ctx,
                            const std::string &field_name) const {
  tiledb::QueryCondition query_condition(ctx);
  switch (this->operation_type) {
  case Item_func::EQ_FUNC:
  case Item_func::EQUAL_FUNC:
    query_condition.init(field_name, lower_value.get(), lower_value_size,
                         TILEDB_EQ);
    break;
  case Item_func::NE_FUNC:
    query_condition.init(field_name, lower_value.get(), lower_value_size,
                         TILEDB_NE);
    break;
  case Item_func::LT_FUNC:
    query_condition.init(field_name, upper_value.get(), upper_value_size,
                         TILEDB_LT);
    break;
  case Item_func::LE_FUNC:
    query_condition.init(field_name, upper_value.get(), upper_value_size,
                         TILEDB_LE);
    break;
  case Item_func::GE_FUNC:
    query_condition.init(field_name, lower_value.get(), lower_value_size,
                         TILEDB_GE);
    break;
  case Item_func::GT_FUNC:
    query_condition.init(field_name, lower_value.get(), lower_value_size,
                         TILEDB_GT);
    break;
  case Item_func::ISNULL_FUNC:
    query_condition.init(field_name, nullptr, 0, TILEDB_EQ);
    break;
  case Item_func::ISNOTNULL_FUNC:
    query_condition.init(field_name, nullptr, 0, TILEDB_NE);
    break;
  case Item_func::BETWEEN: {
    tiledb::QueryCondition lhs(ctx);
    tiledb::QueryCondition rhs(ctx);

    lhs.init(field_name, lower_value.get(), lower_value_size, TILEDB_GE);
    rhs.init(field_name, upper_value.get(), upper_value_size, TILEDB_LE);

    query_condition = lhs.combine(rhs, TILEDB_AND);
    break;
  }
  default:
    // TODO: Hanld other funcs
    break;
  }

  return query_condition;
}
