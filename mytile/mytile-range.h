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
#include <tiledb/tiledb>

namespace tile {
typedef struct range_struct {
  std::unique_ptr<void, decltype(&std::free)> lower_value;
  std::unique_ptr<void, decltype(&std::free)> upper_value;
  Item_func::Functype operation_type;
  tiledb_datatype_t datatype;
} range;

void setup_range(const std::shared_ptr<range> &range, void *non_empty_domain,
                 tiledb::Dimension dimension);

template <typename T>
void setup_range(const std::shared_ptr<range> &range, T *non_empty_domain) {
  switch (range->operation_type) {
  case Item_func::EQUAL_FUNC:
  case Item_func::EQ_FUNC:
    break;
  case Item_func::LT_FUNC: {
    range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(T)), &std::free);
    memcpy(range->lower_value.get(), non_empty_domain, sizeof(T));

    T final_upper_value;
    if (std::is_floating_point<T>()) {
      double upper_value = *(static_cast<double *>(range->upper_value.get()));

      // cast to proper tiledb datatype
      final_upper_value = static_cast<T>(upper_value);

      // Subtract epsilon value for less than
      final_upper_value -= std::numeric_limits<T>::epsilon();
    } else { // assume its a long
      longlong upper_value =
          *(static_cast<longlong *>(range->upper_value.get()));

      // cast to proper tiledb datatype
      final_upper_value = static_cast<T>(upper_value);

      // Subtract epsilon value for less than
      final_upper_value -= 1;
    }
    range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(T)), &std::free);
    memcpy(range->upper_value.get(), &final_upper_value, sizeof(T));

    break;
  }
  case Item_func::LE_FUNC: {
    range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(T)), &std::free);
    memcpy(range->lower_value.get(), non_empty_domain, sizeof(T));

    T final_upper_value;
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

    T final_lower_value;
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
    range->upper_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(T)), &std::free);
    memcpy(range->upper_value.get(), &non_empty_domain[1], sizeof(T));

    T final_lower_value;
    if (std::is_floating_point<T>()) {
      double lower_value = *(static_cast<double *>(range->lower_value.get()));

      // cast to proper tiledb datatype
      final_lower_value = static_cast<T>(lower_value);

      // Add epsilon value for greater than
      final_lower_value += std::numeric_limits<T>::epsilon();
    } else { // assume its a long
      longlong lower_value =
          *(static_cast<longlong *>(range->lower_value.get()));

      // cast to proper tiledb datatype
      final_lower_value = static_cast<T>(lower_value);

      // Add epsilon value for greater than
      final_lower_value += 1;
    }
    range->lower_value = std::unique_ptr<void, decltype(&std::free)>(
        std::malloc(sizeof(T)), &std::free);
    memcpy(range->lower_value.get(), &final_lower_value, sizeof(T));

    break;

    break;
  } break;
  case Item_func::IN_FUNC: /* IN is not supported */
  case Item_func::BETWEEN:
  case Item_func::NE_FUNC: /* Not equal is not supported */
  default:
    break; // DBUG_RETURN(NULL);
  }        // endswitch functype
}
} // namespace tile