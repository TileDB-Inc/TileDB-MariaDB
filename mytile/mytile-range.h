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
 * This declares the range struct for handling pushdown ranges
 */

#pragma once

#define MYSQL_SERVER 1 // required for THD class

#include <my_global.h>    /* ulonglong */
#include <my_decimal.h>   // string2my_decimal
#include <mysqld_error.h> /* ER_UNKNOWN_ERROR */
#include <field.h>
#include <item.h>
#include <item_func.h>
#include <log.h>
#include <tiledb/tiledb>
#include <unordered_set>
#include "utils.h"

namespace tile {
/**
 * struct to store a tiledb query range and the operation type if coming from a
 * mysql predicate
 */
typedef struct range_struct {
  std::unique_ptr<void, decltype(&std::free)> lower_value;
  std::unique_ptr<void, decltype(&std::free)> upper_value;
  Item_func::Functype operation_type;
  tiledb_datatype_t datatype;
} range;

int set_range_from_item_consts(Item_basic_constant *lower_const,
                               Item_basic_constant *upper_const,
                               Item_result cmp_type,
                               std::shared_ptr<range> &range);
/**
 * Take a range, and will set the lower/upper bound as appropriate if it is
 * missing and takes into account the mariadb operations.
 *
 * Example:
 * if a users sets `WHERE dim1 > 10` the range that is passed to this function
 * will have: {lower_value = 10, upper_value=nullptr,
 * operation_type=Item_func::GT_FUNC, datatype=TILEDB_INT32}
 *
 * This function will see that upper_value is null and we want greater than so
 * it will set upper_value to the non_empty_domain max value, and because we
 * want greater than but not equal to it will increment the lower_value by
 * epsilon, which since it is an int is 1.
 *
 * The final range is:
 *
 * {lower_value = 11, upper_value=non_empty_domain[1],
 * operation_type=Item_func::GT_FUNC, datatype=TILEDB_INT32}
 *
 * @param thd
 * @param range
 * @param non_empty_domain
 * @param dimension
 */
void setup_range(THD *thd, const std::shared_ptr<range> &range,
                 void *non_empty_domain, tiledb::Dimension dimension);

template <typename T>
void setup_range(THD *thd, const std::shared_ptr<range> &range,
                 T *non_empty_domain) {
  T final_lower_value;
  T final_upper_value;
  switch (range->operation_type) {
  case Item_func::IN_FUNC: /* IN is treated like equal */
  case Item_func::BETWEEN: /* BETWEEN Is treated like equal */
  case Item_func::EQUAL_FUNC:
  case Item_func::EQ_FUNC:

    // When we are dealing with equality, all we need to do is to convert the
    // value from the mysql types (double/longlong) to the actual datatypes
    // TileDB is expecting
    if (std::is_floating_point<T>()) {
      double lower_value = *(static_cast<double *>(range->lower_value.get()));
      // cast to proper tiledb datatype
      final_lower_value = static_cast<T>(lower_value);
    } else { // assume its a long
      longlong lower_value =
          *(static_cast<longlong *>(range->lower_value.get()));
      // cast to proper tiledb datatype
      final_lower_value = static_cast<T>(lower_value);
    }
    range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(T)), &std::free);
    memcpy(range->lower_value.get(), &final_lower_value, sizeof(T));

    if (std::is_floating_point<T>()) {
      double upper_value = *(static_cast<double *>(range->upper_value.get()));
      // cast to proper tiledb datatype
      final_upper_value = static_cast<T>(upper_value);
    } else { // assume its a long
      longlong upper_value =
          *(static_cast<longlong *>(range->upper_value.get()));
      // cast to proper tiledb datatype
      final_upper_value = static_cast<T>(upper_value);
    }
    range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(T)), &std::free);
    memcpy(range->upper_value.get(), &final_upper_value, sizeof(T));

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
        std::malloc(sizeof(T)), &std::free);
    memcpy(range->lower_value.get(), non_empty_domain, sizeof(T));

    if (std::is_floating_point<T>()) {
      double upper_value = *(static_cast<double *>(range->upper_value.get()));
      // cast to proper tiledb datatype
      final_upper_value = static_cast<T>(upper_value);
    } else { // assume its a long
      longlong upper_value =
          *(static_cast<longlong *>(range->upper_value.get()));
      // cast to proper tiledb datatype
      final_upper_value = static_cast<T>(upper_value);
    }
    range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(T)), &std::free);
    memcpy(range->upper_value.get(), &final_upper_value, sizeof(T));

    break;
  }
  case Item_func::GE_FUNC: {
    range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(T)), &std::free);
    memcpy(range->upper_value.get(), &non_empty_domain[1], sizeof(T));

    if (std::is_floating_point<T>()) {
      double lower_value = *(static_cast<double *>(range->lower_value.get()));
      // cast to proper tiledb datatype
      final_lower_value = static_cast<T>(lower_value);
    } else { // assume its a long
      longlong lower_value =
          *(static_cast<longlong *>(range->lower_value.get()));
      // cast to proper tiledb datatype
      final_lower_value = static_cast<T>(lower_value);
    }
    range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(T)), &std::free);
    memcpy(range->lower_value.get(), &final_lower_value, sizeof(T));

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
  log_debug(
      thd, "pushed conditions: [%s, %s]",
      std::to_string(*static_cast<T *>(range->lower_value.get())).c_str(),
      std::to_string(*static_cast<T *>(range->upper_value.get())).c_str());
}

std::shared_ptr<range> merge_ranges(std::vector<std::shared_ptr<range>> &ranges,
                                    tiledb_datatype_t datatype);

template <typename T>
std::shared_ptr<range>
merge_ranges(std::vector<std::shared_ptr<range>> &ranges) {
  std::shared_ptr<range> merged_range;

  if (ranges.empty())
    return nullptr;

  // Set the first element as the default for the merged range, this gives us
  // some initial values to compare against
  merged_range = std::move(ranges[0]);
  // Remove first element since its now set as initial range
  ranges.erase(ranges.begin());

  // loop through ranges and set upper/lower maxima/minima
  for (auto &range : ranges) {
    if (range->lower_value != nullptr) {
      if (merged_range->lower_value == nullptr) {
        merged_range->lower_value = std::move(range->lower_value);
        // See if the current range has a higher low value than the "merged"
        // range, if so set the new low value, since the current range has a
        // more restrictive condition
      } else if (*(static_cast<T *>(merged_range->lower_value.get())) <
                 *(static_cast<T *>(range->lower_value.get()))) {
        merged_range->lower_value = std::move(range->lower_value);
      }
    }

    if (range->upper_value != nullptr) {
      if (merged_range->upper_value == nullptr) {
        merged_range->upper_value = std::move(range->upper_value);
        // See if the current range has a lower upper value than the "merged"
        // range, if so set the new upper value since the current range has a
        // more restrictive condition
      } else if (*(static_cast<T *>(merged_range->upper_value.get())) >
                 *(static_cast<T *>(range->upper_value.get()))) {
        merged_range->upper_value = std::move(range->upper_value);
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

/**
 * Takes a vector of ranges build from IN predicates and returns a unique vector
 * of ranges which are not contained by the existing main super range (if non
 * null) and are unique
 * @param in_ranges
 * @param main_range
 * @return
 */
std::vector<std::shared_ptr<range>> get_unique_non_contained_in_ranges(
    const std::vector<std::shared_ptr<range>> &in_ranges,
    const std::shared_ptr<range> &main_range);

template <typename T>
std::vector<std::shared_ptr<range>> get_unique_non_contained_in_ranges(
    const std::vector<std::shared_ptr<range>> &in_ranges,
    const std::shared_ptr<range> &main_range) {

  // Return unique non contained ranges
  std::vector<std::shared_ptr<range>> ret;

  std::unordered_set<T> unique_values_set;
  std::vector<T> unique_values_vec;

  // get datatype
  tiledb_datatype_t datatype;
  if (main_range != nullptr) {
    datatype = main_range->datatype;
  } else if (!in_ranges.empty()) {
    datatype = in_ranges[0]->datatype;
  }

  // Only set main range value if not null
  T main_lower_value;
  T main_upper_value;
  if (main_range != nullptr) {
    main_lower_value = *static_cast<T *>(main_range->lower_value.get());
    main_upper_value = *static_cast<T *>(main_range->upper_value.get());
  }

  for (auto &range : in_ranges) {
    // lower and upper values are equal, so just grab the lower
    // for in clauses, every values is set as a equality range
    T range_lower_value = *static_cast<T *>(range->lower_value.get());

    // Check for contained range if main range is non null
    if (main_range != nullptr) {
      // If the range is contained, skip it
      if ((main_lower_value <= range_lower_value) &&
          (range_lower_value <= main_upper_value)) {
        continue;
      }
    }

    // Add value to set
    if (unique_values_set.count(range_lower_value) == 0) {
      unique_values_set.insert(range_lower_value);
      unique_values_vec.push_back(range_lower_value);
    }
  }

  // from unique values build final ranges
  for (T val : unique_values_vec) {
    // Build range pointer
    std::shared_ptr<range> range = std::make_shared<tile::range>(tile::range{
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        Item_func::EQ_FUNC, datatype});

    // Allocate memory for lower value
    range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(T)), &std::free);
    // Copy lower value
    memcpy(range->lower_value.get(), &val, sizeof(T));

    // Allocate memory for upper value
    range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(T)), &std::free);
    // Copy upper value
    memcpy(range->upper_value.get(), &val, sizeof(T));

    ret.push_back(std::move(range));
  }

  return ret;
}

/**
 * Converts from key find flag enum to functype used by ranges
 * @param find_flag
 * @return
 */
Item_func::Functype find_flag_to_func(enum ha_rkey_function find_flag);

/**
 *
 * Takes a key and find_flag and converts to the a vector of ranges.
 * @param key
 * @param length
 * @param find_flag
 * @param datatype
 * @return
 */
std::vector<std::shared_ptr<tile::range>>
build_ranges_from_key(const uchar *key, uint length,
                      enum ha_rkey_function find_flag,
                      tiledb_datatype_t datatype);

/**
 * Takes a key and find_flag and converts to the a vector of ranges.
 * @tparam T
 * @param key
 * @param length
 * @param find_flag
 * @param datatype
 * @return
 */
template <typename T>
std::vector<std::shared_ptr<tile::range>>
build_ranges_from_key(const uchar *key, uint length,
                      enum ha_rkey_function find_flag,
                      tiledb_datatype_t datatype) {
  // Length shouldn't be zero here but better safe then segfault!
  if (length == 0)
    return {};

  uint dims_with_keys = length / sizeof(T);

  std::vector<std::shared_ptr<tile::range>> ranges;

  // Cast key to array of type T
  const T *key_typed = reinterpret_cast<const T *>(key);
  // Handle all keys but the last one
  for (size_t i = 0; i < dims_with_keys - 1; i++) {
    // Initialize range
    std::shared_ptr<tile::range> range = std::make_shared<tile::range>(
        tile::range{std::unique_ptr<void, decltype(&std::free)>(
                        std::malloc(sizeof(T)), &std::free),
                    std::unique_ptr<void, decltype(&std::free)>(
                        std::malloc(sizeof(T)), &std::free),
                    Item_func::EQ_FUNC, datatype});

    // Copy value into range settings
    // We can set lower/upper to key value because enforces equality for all key
    // parts except the last that is set which is handled below
    memcpy(range->lower_value.get(), &key_typed[i], sizeof(T));
    memcpy(range->upper_value.get(), &key_typed[i], sizeof(T));

    ranges.push_back(std::move(range));
  }

  // Handle the last dimension
  std::shared_ptr<tile::range> last_dimension_range =
      std::make_shared<tile::range>(tile::range{
          std::unique_ptr<void, decltype(&std::free)>(std::malloc(sizeof(T)),
                                                      &std::free),
          std::unique_ptr<void, decltype(&std::free)>(std::malloc(sizeof(T)),
                                                      &std::free),
          find_flag_to_func(find_flag), datatype});

  T lower = key_typed[dims_with_keys - 1];
  T upper = key_typed[dims_with_keys - 1];

  // The last key part, is where a range operation might be specified
  // So for this last dimension we must check to see if we need to set the lower
  // or upper bound and if we need to convert from greater/less than to the
  // greater/less than or equal to format.
  switch (last_dimension_range->operation_type) {
  // If we have greater than, lets make it greater than or equal
  // TileDB ranges are inclusive
  case Item_func::GT_FUNC: {
    last_dimension_range->operation_type = Item_func::GE_FUNC;
    if (std::is_floating_point<T>()) {
      lower = std::nextafter(lower, static_cast<T>(1.0f));
    } else {
      lower += 1;
    }
    memcpy(last_dimension_range->lower_value.get(), &lower, sizeof(T));
    last_dimension_range->upper_value = nullptr;
    break;
  }
  case Item_func::GE_FUNC: {
    memcpy(last_dimension_range->lower_value.get(), &lower, sizeof(T));
    last_dimension_range->upper_value = nullptr;
    break;
  }
  case Item_func::LT_FUNC: {
    // If we have less than, lets make it less than or equal
    // TileDB ranges are inclusive
    last_dimension_range->operation_type = Item_func::LE_FUNC;
    if (std::is_floating_point<T>()) {
      upper = std::nextafter(lower, static_cast<T>(-1.0f));
    } else {
      upper -= 1;
    }
    memcpy(last_dimension_range->upper_value.get(), &upper, sizeof(T));
    last_dimension_range->lower_value = nullptr;
    break;
  }
  case Item_func::LE_FUNC: {
    memcpy(last_dimension_range->upper_value.get(), &upper, sizeof(T));
    last_dimension_range->lower_value = nullptr;
    break;
  }
  case Item_func::EQ_FUNC: {
    memcpy(last_dimension_range->lower_value.get(), &lower, sizeof(T));
    memcpy(last_dimension_range->upper_value.get(), &upper, sizeof(T));
    break;
  }
  default:
    my_printf_error(ER_UNKNOWN_ERROR,
                    "Unsupported Item_func::functype in build_ranges_from_key",
                    ME_ERROR_LOG | ME_FATAL);
    break;
  }

  ranges.push_back(std::move(last_dimension_range));

  return ranges;
}
} // namespace tile
