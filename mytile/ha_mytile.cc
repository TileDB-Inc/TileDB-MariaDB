/**
 * @file   ha_mytile.cc
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
 * rightsG to use, copy, modify, merge, publish, distribute, sublicense, and/or
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
 * This is the main handler implementation
 */

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation // gcc: Class implementation
#endif

#include <my_global.h>

#define MYSQL_SERVER 1

#include "ha_mytile.h"
#include "mytile-errors.h"
#include "mytile-discovery.h"
#include "mytile-statusvars.h"
#include "mytile-sysvars.h"
#include "mytile-metadata.h"
#include "mytile.h"
#include "utils.h"
#include "item.h"
#include "sql_type_geom.h"
#include "spatial.h"
#include <cstring>
#include <log.h>
#include <my_config.h>
#include <mysql/plugin.h>
#include <mysqld_error.h>
#include <sql_class.h>
#include <sql_select.h>
#include <vector>
#include <unordered_map>
#include <key.h> // key_copy, key_unpack, key_cmp_if_same, key_cmp

// Handler for mytile engine
handlerton *mytile_hton;

// Structure for table options
ha_create_table_option mytile_table_option_list[] = {
    HA_TOPTION_STRING("uri", array_uri),
    HA_TOPTION_NUMBER("capacity", capacity, 10000, 0, UINT64_MAX, 1),
    HA_TOPTION_ENUM("array_type", array_type, "DENSE,SPARSE", 1),
    HA_TOPTION_ENUM("cell_order", cell_order, "ROW_MAJOR,COLUMN_MAJOR,HILBERT",
                    0),
    HA_TOPTION_ENUM("tile_order", tile_order, "ROW_MAJOR,COLUMN_MAJOR", 0),
    HA_TOPTION_NUMBER("open_at", open_at, UINT64_MAX, 0, UINT64_MAX, 1),
    HA_TOPTION_STRING("encryption_key", encryption_key),
    HA_TOPTION_STRING("coordinate_filters", coordinate_filters),
    HA_TOPTION_STRING("offset_filters", offset_filters),
    HA_TOPTION_STRING("validity_filters", validity_filters),
    HA_TOPTION_END};

// Structure for specific field options
ha_create_table_option mytile_field_option_list[] = {
    HA_FOPTION_BOOL("dimension", dimension, false),
    HA_FOPTION_STRING("lower_bound", lower_bound),
    HA_FOPTION_STRING("upper_bound", upper_bound),
    HA_FOPTION_STRING("tile_extent", tile_extent),
    HA_FOPTION_STRING("filters", filters),
    HA_FOPTION_END};

tiledb::Config tile::build_config(THD *thd) {
  tiledb::Config cfg = tiledb::Config();

  std::string tiledb_config = tile::sysvars::tiledb_config(thd);

  // If the config is not an empty string, split it from csv to key=value
  // strings
  if (!tiledb_config.empty()) {
    std::vector<std::string> parameters = split(tiledb_config, ',');
    // Loop through each key=value
    for (std::string &param : parameters) {
      // Split key=value into a vector of size two
      std::vector<std::string> kv = split(param, '=');
      if (kv.size() == 2) {
        std::string key = kv[0];
        std::string value = kv[1];
        trim(key);
        trim(value);
        cfg[key] = value;
      }
    }
  }

  return cfg;
}

tiledb::Context tile::build_context(tiledb::Config &cfg) {
  tiledb::Context ctx(cfg);
  std::string prefix = "context.tag.";

  // Loop through config and see if there are any context tags which need to be
  // set
  for (auto &it : cfg) {
    auto res = std::mismatch(prefix.begin(), prefix.end(), it.first.begin());
    if (res.first == prefix.end()) {
      std::string tag_key = it.first.substr(prefix.length(), it.first.length());
      std::string value = it.second;
      trim(tag_key);
      trim(value);
      ctx.set_tag(tag_key, value);
    }
  }
  return ctx;
}

int tile::mytile_group_by_handler::end_scan() {
  DBUG_ENTER("tile::mytile_group_by_handler::end_scan");
  // reset qc and ranges
  if (this->aggr_query != nullptr) {
    this->aggr_query = nullptr;
  }

  if (this->tiledb_qc != nullptr) {
    this->tiledb_qc = nullptr;
  }

  if (this->aggr_array != nullptr && this->aggr_array->is_open()) {
    this->aggr_array->close();
  }

  this->pushdown_ranges.clear();
  this->pushdown_in_ranges.clear();
  delete this->aggr_array;
  DBUG_RETURN(0);
}

int tile::mytile_group_by_handler::init_scan() {
  DBUG_ENTER("tile::mytile_group_by_handler::init_scan");
  first_row = 1;
  int rc = 0;

  try {
    // Get domain, schema and dimensions
    auto schema = aggr_array->schema();
    auto domain = schema.domain();
    int empty_read;

    this->tiledb_sub = std::unique_ptr<tiledb::Subarray>(
        new tiledb::Subarray(*this->ctx, *aggr_array));

    tile::build_subarray(thd, this->valid_ranges, this->valid_in_ranges,
                         empty_read, domain, this->pushdown_ranges,
                         this->pushdown_in_ranges, this->tiledb_sub,
                         this->ctx.get(), this->aggr_array);
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[init_scan] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->aggr_array->uri().c_str(),
                    e.what());
    rc = ERR_INIT_SCAN_TILEDB;
    // clear out failed query details
    end_scan();
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[init_scan] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->aggr_array->uri().c_str(),
                    e.what());
    rc = ERR_INIT_SCAN_TILEDB;
    end_scan();
  }

  // The query condition is already built and will be applied in the next_row()
  // method.
  DBUG_RETURN(rc);
}

int tile::mytile_group_by_handler::next_row() {
  DBUG_ENTER("tile::mytile_group_by_handler::next_row");
  int rc = 0;
  // Get the current SELECT statement
  SELECT_LEX *select_lex = thd->lex->current_select;
  List_iterator_fast<Item> it(select_lex->item_list);
  Field **field_ptr = table->field;
  std::vector<uint8_t> validity(1);
  Item_sum *item_sum;

  /*
    Check if this is the first call to the function. If not, we have already
    returned all data.
  */
  if (!first_row) {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  first_row = 0;
  try {
    while ((item_sum = (Item_sum *)it++)) {
      Field *field = *(field_ptr++);
      Item_sum *item_sum_ptr = dynamic_cast<Item_sum *>(item_sum);

      if (!item_sum_ptr)
        continue;

      if (item_sum_ptr->get_arg_count() == 0)
        continue;

      std::string column_with_aggregate;
      const char *str_ptr = item_sum_ptr->get_arg(0)->name.str;
      if (str_ptr != nullptr) {
        std::string column_with_aggregate = str_ptr;
      } else {
        continue;
      }

      this->aggr_query =
          std::make_unique<tiledb::Query>(*this->ctx, *aggr_array, TILEDB_READ);

      auto schema = aggr_array->schema();
      auto domain = schema.domain();

      // set layout todo check correctness
      if (schema.array_type() == TILEDB_SPARSE) {
        aggr_query->set_layout(TILEDB_UNORDERED);
      } else {
        aggr_query->set_layout(TILEDB_GLOBAL_ORDER);
      }

      tiledb::QueryChannel default_channel =
          tiledb::QueryExperimental::get_default_channel(*aggr_query);

      // add query condition
      if (this->tiledb_qc != nullptr) {
        aggr_query->set_condition(*this->tiledb_qc);
      }

      aggr_query->set_subarray(*this->tiledb_sub);

      bool nullable = false;
      tiledb_datatype_t type;
      if (schema.has_attribute(column_with_aggregate)) {
        auto attr = schema.attribute(column_with_aggregate);
        if (attr.nullable())
          nullable = true;
        type = attr.type();
      } else {
        auto dim = domain.dimension(column_with_aggregate);
        type = dim.type();
      }
      switch (item_sum->sum_func()) {
      case Item_sum::SUM_FUNC: {
        std::string sum_string = "Sum";
        tiledb::ChannelOperation operation =
            tiledb::QueryExperimental::create_unary_aggregate<
                tiledb::SumOperator>(*aggr_query, column_with_aggregate);
        default_channel.apply_aggregate(sum_string, operation);

        if (nullable) {
          aggr_query->set_validity_buffer(sum_string, validity);
        }

        submit_and_set_sum_aggregate(aggr_query, type, field, sum_string);

        break;
      }
      case Item_sum::COUNT_FUNC: {
        std::string count_string = "Count";
        default_channel.apply_aggregate(count_string, tiledb::CountOperation());

        if (nullable) {
          aggr_query->set_validity_buffer(count_string, validity);
        }

        std::vector<uint64_t> count(1);
        aggr_query->set_data_buffer(count_string, count);

        aggr_query->submit();

        field->store(count[0], 0);
        break;
      }
      case Item_sum::AVG_FUNC: {
        std::string avg_string = "Avg";
        tiledb::ChannelOperation operation =
            tiledb::QueryExperimental::create_unary_aggregate<
                tiledb::MeanOperator>(*aggr_query, column_with_aggregate);
        default_channel.apply_aggregate(avg_string, operation);
        if (nullable)
          aggr_query->set_validity_buffer(avg_string, validity);
        std::vector<double> avg(1);
        aggr_query->set_data_buffer(avg_string, avg);

        aggr_query->submit();

        field->store(avg[0]);
        break;
      }
      case Item_sum::MAX_FUNC:
      case Item_sum::MIN_FUNC: {
        std::unique_ptr<tiledb::ChannelOperation> operation;

        if (item_sum->sum_func() == Item_sum::MAX_FUNC) {
          operation = std::make_unique<tiledb::ChannelOperation>(
              tiledb::QueryExperimental::create_unary_aggregate<
                  tiledb::MaxOperator>(*aggr_query, column_with_aggregate));
        } else {
          operation = std::make_unique<tiledb::ChannelOperation>(
              tiledb::QueryExperimental::create_unary_aggregate<
                  tiledb::MinOperator>(*aggr_query, column_with_aggregate));
        }

        std::string minmax_string = "minmax";
        default_channel.apply_aggregate(minmax_string, *operation);
        if (nullable) {
          aggr_query->set_validity_buffer(minmax_string, validity);
        }

        submit_and_set_minmax_aggregate(aggr_query, type, field, minmax_string);

        break;
      }
      default:
        throw tiledb::TileDBError(
            std::string("Unknown or Unsupported aggregate"));
      }
      field->set_notnull();
    }
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[next_row] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->aggr_array->uri().c_str(),
                    e.what());
    rc = ERR_INIT_SCAN_TILEDB;
    // clear out failed query details
    end_scan();
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[next_row] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->aggr_array->uri().c_str(),
                    e.what());
    rc = ERR_INIT_SCAN_TILEDB;
    end_scan();
  }

  DBUG_RETURN(rc);
}

int tile::mytile_group_by_handler::submit_and_set_minmax_aggregate(
    std::shared_ptr<tiledb::Query> &aggr_query, const tiledb_datatype_t type,
    Field *field, std::string minmax_string) {
  DBUG_ENTER("tile::mytile_group_by_handler::submit_and_set_minmax_aggregate");

  try {
    switch (type) {
    case TILEDB_FLOAT32: {
      std::vector<float> minmax(1);
      aggr_query->set_data_buffer(minmax_string, minmax);
      aggr_query->submit();
      field->store(minmax[0]);
      break;
    }
    case TILEDB_FLOAT64: {
      std::vector<double> minmax(1);
      aggr_query->set_data_buffer(minmax_string, minmax);
      aggr_query->submit();
      field->store(minmax[0]);
      break;
    }
    case TILEDB_INT8: {
      std::vector<int8_t> minmax(1);
      aggr_query->set_data_buffer(minmax_string, minmax);
      aggr_query->submit();
      field->store(minmax[0], 1);
      break;
    }
    case TILEDB_UINT8: {
      std::vector<uint8_t> minmax(1);
      aggr_query->set_data_buffer(minmax_string, minmax);
      aggr_query->submit();
      field->store(minmax[0], 0);
      break;
    }
    case TILEDB_INT16: {
      std::vector<int16_t> minmax(1);
      aggr_query->set_data_buffer(minmax_string, minmax);
      aggr_query->submit();
      field->store(minmax[0], 1);
      break;
    }
    case TILEDB_UINT16: {
      std::vector<uint16_t> minmax(1);
      aggr_query->set_data_buffer(minmax_string, minmax);
      aggr_query->submit();
      field->store(minmax[0], 0);
      break;
    }
    case TILEDB_INT32: {
      std::vector<int32_t> minmax(1);
      aggr_query->set_data_buffer(minmax_string, minmax);
      aggr_query->submit();
      field->store(minmax[0], 1);
      break;
    }
    case TILEDB_UINT32: {
      std::vector<uint32_t> minmax(1);
      aggr_query->set_data_buffer(minmax_string, minmax);
      aggr_query->submit();
      field->store(minmax[0], 0);
      break;
    }
    case TILEDB_UINT64: {
      std::vector<uint64_t> minmax(1);
      aggr_query->set_data_buffer(minmax_string, minmax);
      aggr_query->submit();
      field->store(minmax[0], 0);
      break;
    }
    case TILEDB_INT64: {
      std::vector<int64_t> minmax(1);
      aggr_query->set_data_buffer(minmax_string, minmax);
      aggr_query->submit();
      field->store(minmax[0], 1);
      break;
    }
    case TILEDB_DATETIME_YEAR:
    case TILEDB_DATETIME_MONTH:
    case TILEDB_DATETIME_WEEK:
    case TILEDB_DATETIME_DAY:
    case TILEDB_DATETIME_HR:
    case TILEDB_DATETIME_MIN:
    case TILEDB_DATETIME_SEC:
    case TILEDB_DATETIME_MS:
    case TILEDB_DATETIME_US:
    case TILEDB_DATETIME_NS:
    case TILEDB_DATETIME_PS:
    case TILEDB_DATETIME_FS:
    case TILEDB_DATETIME_AS:
    case TILEDB_TIME_HR:
    case TILEDB_TIME_MIN:
    case TILEDB_TIME_SEC:
    case TILEDB_TIME_MS:
    case TILEDB_TIME_US:
    case TILEDB_TIME_NS:
    case TILEDB_TIME_PS:
    case TILEDB_TIME_FS:
    case TILEDB_TIME_AS: {
      std::vector<int64_t> minmax(1);
      aggr_query->set_data_buffer(minmax_string, minmax);
      aggr_query->submit();
      field->store(minmax[0], 1);
      break;
    }
    case TILEDB_STRING_UTF8:
    case TILEDB_STRING_ASCII:
    case TILEDB_STRING_UCS2:
    case TILEDB_STRING_UCS4:
    case TILEDB_STRING_UTF16:
    case TILEDB_STRING_UTF32:
    case TILEDB_CHAR: {
      std::vector<uint64_t> offsets(1);
      aggr_query->set_offsets_buffer(minmax_string, offsets);
      std::string minmax;
      minmax.resize(32); // todo what size should we put here? maybe add option
                         // for users to increase this?
      aggr_query->set_data_buffer(minmax_string, minmax);
      aggr_query->submit();
      minmax.erase(std::find(minmax.begin(), minmax.end(), '\0'), minmax.end());
      field->store(minmax.c_str(), minmax.length(), &my_charset_latin1);
      break;
    }
    default: {
      throw tiledb::TileDBError(
          std::string("Unknown or Unsupported type for aggregate"));
    }
    }
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[submit_and_set_sum_aggregate] error",
                    ME_ERROR_LOG | ME_FATAL);
    DBUG_RETURN(ERR_AGGREGATES);
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[submit_and_set_sum_aggregate] error",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    DBUG_RETURN(ERR_AGGREGATES);
  }
  DBUG_RETURN(0);
}

int tile::mytile_group_by_handler::submit_and_set_sum_aggregate(
    std::shared_ptr<tiledb::Query> &aggr_query, const tiledb_datatype_t type,
    Field *field, std::string sum_string) {
  DBUG_ENTER("tile::mytile_group_by_handler::submit_and_set_sum_aggregate");
  try {
    switch (type) {
    case TILEDB_FLOAT32:
    case TILEDB_FLOAT64: {
      std::vector<double> sum(1);
      aggr_query->set_data_buffer(sum_string, sum);
      aggr_query->submit();
      field->store(sum[0]);
      break;
    }
    case TILEDB_UINT8:
    case TILEDB_UINT16:
    case TILEDB_UINT32:
    case TILEDB_UINT64: {
      std::vector<uint64_t> sum(1);
      aggr_query->set_data_buffer(sum_string, sum);
      aggr_query->submit();
      field->store(sum[0], 0);
      break;
    }
    case TILEDB_INT8:
    case TILEDB_INT16:
    case TILEDB_INT32:
    case TILEDB_INT64: {
      std::vector<int64_t> sum(1);
      aggr_query->set_data_buffer(sum_string, sum);
      aggr_query->submit();
      field->store(sum[0], 1);
      break;
    }
    default:
      throw tiledb::TileDBError(
          std::string("Unknown or Unsupported type for aggregate"));
    }
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[submit_and_set_sum_aggregate] error",
                    ME_ERROR_LOG | ME_FATAL);
    DBUG_RETURN(ERR_AGGREGATES);
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[submit_and_set_sum_aggregate] error",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    DBUG_RETURN(ERR_AGGREGATES);
  }
  DBUG_RETURN(0);
}

/**
 * Checks if the given field in the given array is aggregation compatible
 * @param field The input field
 * @param aggregate The aggregation
 * @param array_for_comp  The array
 * @return
 */
static bool aggregate_is_supported(const std::string field,
                                   const Item_sum::Sumfunctype aggregate,
                                   tiledb::Array *array_for_comp) {
  // if it not an attribute, no TileDB aggregation can be applied

  tiledb_datatype_t type;
  tiledb::ArraySchema schema = array_for_comp->schema();
  tiledb::Domain domain = schema.domain();
  if (schema.has_attribute(field)) {
    auto attr = schema.attribute(field);
    if (attr.cell_val_num() > 1 && !attr.variable_sized())
      return false; // multi valued not supported
    type = attr.type();
  } else if (domain.has_dimension(field)) {
    if (schema.array_type() == TILEDB_SPARSE)
      return false; // disable on sparse array dims
    auto dim = schema.domain().dimension(field);
    type = dim.type();
  } else {
    return false;
  }

  // The following switch is based on
  // https://docs.tiledb.com/main/background/internal-mechanics/aggregates
  switch (aggregate) {
  case Item_sum::SUM_FUNC:
  case Item_sum::AVG_FUNC:
    return tile::is_numeric_type(type);
  case Item_sum::MIN_FUNC:
  case Item_sum::MAX_FUNC:
    return tile::is_numeric_type(type) || tile::is_string_type(type);
  case Item_sum::COUNT_FUNC:
    // disable count for dense as it also counts the fill values,// thus
    // producing wrong results
    return schema.array_type() != TILEDB_DENSE;
  default:
    return false;
  }
}

static group_by_handler *mytile_create_group_by_handler(THD *thd,
                                                        Query *query) {
  tile::mytile_group_by_handler *handler;
  if (!tile::sysvars::enable_aggregate_pushdown(thd)) {
    return 0;
  }

  /* check that there is no group_by. Not currently supported by TileDB*/
  if (query->group_by != 0)
    return 0;

  /* check that there is no order_by. Not currently supported by TileDB*/
  if (query->order_by != 0) {
    return 0;
  }

  tile::mytile *mytile_ptr =
      dynamic_cast<tile::mytile *>(query->from->table->file);

  // take everything we need from the mytile handler.
  std::string encryption_key;
  if (mytile_ptr->get_table()->s->option_struct->encryption_key != nullptr) {
    encryption_key =
        std::string(mytile_ptr->get_table()->s->option_struct->encryption_key);
  }
  uint64_t open_at = mytile_ptr->get_table()->s->option_struct->open_at;
  std::string uri = mytile_ptr->get_uri();
  std::shared_ptr<tiledb::QueryCondition> &qc = mytile_ptr->get_qc();
  std::vector<std::vector<std::shared_ptr<tile::range>>> &ranges =
      mytile_ptr->get_pushdown_ranges();
  std::vector<std::vector<std::shared_ptr<tile::range>>> &in_ranges =
      mytile_ptr->get_pushdown_in_ranges();
  bool valid_ranges = mytile_ptr->valid_pushed_ranges();
  bool valid_in_ranges = mytile_ptr->valid_pushed_in_ranges();

  // open array here before init scan because we need to check if the aggregate
  // requested can be processed by TileDB.
  tiledb::Array *aggr_array;
  std::shared_ptr<tiledb::Config> cfg;
  std::shared_ptr<tiledb::Context> ctx;

  cfg = std::make_shared<tiledb::Config>(tile::build_config(thd));
  ctx = std::make_shared<tiledb::Context>(tile::build_context(*cfg));

  if (open_at != UINT64_MAX) {
#if TILEDB_VERSION_MAJOR >= 2 && TILEDB_VERSION_MINOR >= 15
    aggr_array = new tiledb::Array(
        *ctx, uri, TILEDB_READ,
        tiledb::TemporalPolicy(tiledb::TimeTravel, open_at),
        tiledb::EncryptionAlgorithm(
            encryption_key.empty() ? TILEDB_NO_ENCRYPTION : TILEDB_AES_256_GCM,
            encryption_key.c_str()));
#else
    aggr_array = new tiledb::Array(*ctx, uri, TILEDB_READ,
                                   encryption_key.empty() ? TILEDB_NO_ENCRYPTION
                                                          : TILEDB_AES_256_GCM,
                                   encryption_key, open_at);
#endif

  } else {

#if TILEDB_VERSION_MAJOR >= 2 && TILEDB_VERSION_MINOR >= 15
    aggr_array = new tiledb::Array(
        *ctx, uri, TILEDB_READ, tiledb::TemporalPolicy(),
        tiledb::EncryptionAlgorithm(
            encryption_key.empty() ? TILEDB_NO_ENCRYPTION : TILEDB_AES_256_GCM,
            encryption_key.c_str()));
#else
    aggr_array = new tiledb::Array(*ctx, uri, TILEDB_READ,
                                   encryption_key.empty() ? TILEDB_NO_ENCRYPTION
                                                          : TILEDB_AES_256_GCM,
                                   encryption_key);
#endif
  }

  // Get the current SELECT statement
  SELECT_LEX *select_lex = thd->lex->current_select;

  if (select_lex->agg_func_used()) {
    // Iterate through the item list to find TileDB compatible aggregates
    Item *item;
    List_iterator_fast<Item> it(*query->select);
    while ((item = it++)) {
      Item_sum *isp = dynamic_cast<Item_sum *>(item);

      std::string column_with_aggregate;
      const char *str_ptr = isp->get_arg(0)->name.str;
      if (str_ptr != nullptr) {
        std::string column_with_aggregate = str_ptr;
      }

      if (isp && !aggregate_is_supported(column_with_aggregate, isp->sum_func(),
                                         aggr_array)) {
        if (aggr_array != nullptr && aggr_array->is_open()) {
          aggr_array->close();
          delete aggr_array;
        }
        return 0;
      }
      // if you find at least one not compatible aggregate abort entire pushdown
    }

    /* Create handler and return it */
    handler = new tile::mytile_group_by_handler(
        thd, aggr_array, ctx, qc, valid_ranges, valid_in_ranges, ranges,
        in_ranges, encryption_key, open_at);
    return handler;
  }
  return 0;
}

// Create mytile object
static handler *mytile_create_handler(handlerton *hton, TABLE_SHARE *table,
                                      MEM_ROOT *mem_root) {
  return new (mem_root) tile::mytile(hton, table);
}

// mytile file extensions
static const char *mytile_exts[] = {NullS};

// Initialization function
static int mytile_init_func(void *p) {
  DBUG_ENTER("mytile_init_func");

  mytile_hton = static_cast<handlerton *>(p);
  mytile_hton->create = mytile_create_handler;
#if MYSQL_VERSION_ID < 100500
  mytile_hton->state = SHOW_OPTION_YES;
#endif
  mytile_hton->tablefile_extensions = mytile_exts;
  mytile_hton->table_options = mytile_table_option_list;
  mytile_hton->field_options = mytile_field_option_list;
  // Set table discovery functions
  mytile_hton->discover_table_structure = tile::mytile_discover_table_structure;
  mytile_hton->discover_table = tile::mytile_discover_table;
  mytile_hton->create_group_by = mytile_create_group_by_handler;

  DBUG_RETURN(0);
}

// Storage engine interface
struct st_mysql_storage_engine mytile_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

THR_LOCK_DATA **tile::mytile::store_lock(THD *thd, THR_LOCK_DATA **to,
                                         enum thr_lock_type lock_type) {
  DBUG_ENTER("tile::mytile::store_lock");
  DBUG_RETURN(to);
};

int tile::mytile::external_lock(THD *thd, int lock_type) {
  DBUG_ENTER("tile::mytile::external_lock");
  DBUG_RETURN(0);
}

tile::mytile::mytile(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg){};

tile::mytile_group_by_handler::mytile_group_by_handler(
    THD *thd_arg, tiledb::Array *array,
    std::shared_ptr<tiledb::Context> &context,
    std::shared_ptr<tiledb::QueryCondition> &qc, bool val_ranges,
    bool val_in_ranges,
    std::vector<std::vector<std::shared_ptr<tile::range>>> &ranges,
    std::vector<std::vector<std::shared_ptr<tile::range>>> &in_ranges,
    const std::string encryption_key, const uint64_t open_at)
    : group_by_handler(thd_arg, mytile_hton), aggr_array(array), ctx(context),
      tiledb_qc(qc), valid_ranges(val_ranges), valid_in_ranges(val_in_ranges),
      pushdown_ranges(ranges), pushdown_in_ranges(in_ranges),
      encryption_key(encryption_key), open_at(open_at){};

int tile::mytile::create(const char *name, TABLE *table_arg,
                         HA_CREATE_INFO *create_info) {
  DBUG_ENTER("tile::mytile::create");
  // First rebuild context with new config if needed
  tiledb::Config cfg = build_config(ha_thd());

  std::string encryption_key;
  if (table_arg->s->option_struct->encryption_key != nullptr) {
    encryption_key = std::string(table_arg->s->option_struct->encryption_key);
  } else if (this->table_share->option_struct->encryption_key != nullptr) {
    encryption_key =
        std::string(this->table_share->option_struct->encryption_key);
  }

  if (!encryption_key.empty()) {
    cfg["sm.encryption_type"] = "AES_256_GCM";
    cfg["sm.encryption_key"] = encryption_key.c_str();
  }

  if (cfg != this->config) {
    this->config = cfg;
    this->ctx = build_context(this->config);
  }
  DBUG_RETURN(create_array(name, table_arg, create_info, this->ctx));
}

int tile::mytile::open(const char *name, int mode, uint test_if_locked) {
  DBUG_ENTER("tile::mytile::open");
  // First rebuild context with new config if needed
  tiledb::Config cfg = build_config(ha_thd());

  std::string encryption_key;
  if (this->table->s->option_struct->encryption_key != nullptr) {
    encryption_key = std::string(this->table->s->option_struct->encryption_key);
  } else if (this->table_share->option_struct->encryption_key != nullptr) {
    encryption_key =
        std::string(this->table_share->option_struct->encryption_key);
  }

  if (!encryption_key.empty()) {
    cfg["sm.encryption_type"] = "AES_256_GCM";
    cfg["sm.encryption_key"] = encryption_key.c_str();
  }

  if (cfg != this->config) {
    this->config = cfg;
    this->ctx = build_context(this->config);
  }

  // Open TileDB Array
  try {
    uri = name;
    if (this->table->s->option_struct->array_uri != nullptr)
      uri = this->table->s->option_struct->array_uri;

    // Check if @metadata is ending of uri, if so the user is trying to query
    // the metadata we need to remove the keyword for checking if the array
    // exists
    this->metadata_query = false;
    if (tile::has_ending(uri, METADATA_ENDING)) {
      uri = uri.substr(0, uri.length() - METADATA_ENDING.length());
      metadata_query = true;
    }

    this->array_schema = std::unique_ptr<tiledb::ArraySchema>(
        new tiledb::ArraySchema(this->ctx, this->uri));
    this->domain =
        std::make_unique<tiledb::Domain>(this->array_schema->domain());
    this->ndim = domain->ndim();

    // Set ref length used for storing reference in position(), this is the size
    // of a subarray for querying
    this->ref_length = 0;
    bool any_var_length = false;
    for (const auto &dim : domain->dimensions()) {
      this->dimensionNames.push_back(dim.name());
      if (dim.cell_val_num() == TILEDB_VAR_NUM) {
        any_var_length = true;
        break;
      }
      this->ref_length += sizeof(uint64_t);
      this->ref_length += tiledb_datatype_size(dim.type());
    }

    if (any_var_length) {
      this->ref_length = MAX_FIELD_VARCHARLENGTH;
    }

    // If the user requests we will compute the table records on opening the
    // array
    if (tile::sysvars::compute_table_records(ha_thd())) {

      this->query = nullptr;

      open_array_for_reads(ha_thd());

      auto dims = domain->dimensions();

      this->subarray = std::unique_ptr<tiledb::Subarray>(
          new tiledb::Subarray(this->ctx, *this->array));
      // For each dimension we calculate the non empty domain (equivalent to
      // `select * from ...`, and the result is used to add a range to the
      // subarray)
      for (uint64_t dim_idx = 0; dim_idx < this->ndim; dim_idx++) {
        tiledb::Dimension dimension = domain->dimension(dim_idx);

        if (dimension.cell_val_num() == TILEDB_VAR_NUM) {
          const auto pair = this->array->non_empty_domain_var(dim_idx);
          this->subarray->add_range(dim_idx, pair.first, pair.second);
        } else {
          uint64_t size = (tiledb_datatype_size(dimension.type()) * 2);
          auto nonEmptyDomain = std::unique_ptr<void, decltype(&std::free)>(
              std::malloc(size), &std::free);

          this->ctx.handle_error(tiledb_array_get_non_empty_domain_from_index(
              this->ctx.ptr().get(), this->array->ptr().get(), dim_idx,
              nonEmptyDomain.get(), &this->empty_read));

          void *lower = static_cast<char *>(nonEmptyDomain.get());
          void *upper = static_cast<char *>(nonEmptyDomain.get()) +
                        tiledb_datatype_size(dimension.type());

          // set range
          this->ctx.handle_error(tiledb_subarray_add_range(
              this->ctx.ptr().get(), this->subarray->ptr().get(), dim_idx,
              lower, upper, nullptr));
        }
      }

      // Since we added ranges, we calculate the total number of records the
      // array contains
      this->records_upper_bound = this->computeRecordsUB();
      this->query->set_subarray(*this->subarray);
    }

  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "open error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, uri.c_str(), e.what());
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "open error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, uri.c_str(), e.what());
    DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
  }
  DBUG_RETURN(0);
}

int tile::mytile::close(void) {
  DBUG_ENTER("tile::mytile::close");
  try {
    // remove query if exists
    if (this->query != nullptr) {
      this->query = nullptr;
    }

    if (this->query_condition != nullptr)
      this->query_condition = nullptr;

    // close array
    if (this->array != nullptr && this->array->is_open())
      this->array->close();

    // Clear all allocated buffers
    dealloc_buffers();
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "close error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, uri.c_str(), e.what());
    DBUG_RETURN(ERR_CLOSE_TILEDB);
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "close error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, uri.c_str(), e.what());
    DBUG_RETURN(ERR_CLOSE_OTHER);
  }
  DBUG_RETURN(0);
}

bool tile::mytile::fields_have_same_name(Field *a, Field *b) {
  DBUG_ENTER("tile::mytile::fields_have_same_name");
  DBUG_RETURN(strcmp(a->field_name.str, b->field_name.str) == 0); // success
}

void tile::mytile::find_columns_to_drop(
    TABLE *new_table, TABLE *orig_table,
    std::vector<std::string> &columns_to_be_dropped) {
  DBUG_ENTER("tile::mytile::find_columns_to_drop");

  uint new_index = 0;
  // iterate the schema with the greater number of columns, in this case the
  // original table
  for (uint orig_index = 0; orig_index < orig_table->s->fields; orig_index++) {
    Field *curr_field_in_orig = orig_table->field[orig_index];

    // if we have reached the end of the smaller new table, add the remaining
    // columns to the columns_to_be_dropped vector
    if (new_index == new_table->s->fields) {
      columns_to_be_dropped.push_back(curr_field_in_orig->field_name.str);
      continue;
    }

    // increase the index of the new table only when you find the column in the
    // original array
    Field *curr_field_in_new = new_table->field[new_index];
    if (fields_have_same_name(curr_field_in_new, curr_field_in_orig)) {
      new_index++;
    } else {
      columns_to_be_dropped.push_back(curr_field_in_orig->field_name.str);
    }
  }
  DBUG_VOID_RETURN;
}

void tile::mytile::find_columns_to_add(
    TABLE *new_table, TABLE *orig_table, tiledb::Context context,
    std::vector<tiledb::Attribute> &columns_to_be_added) {
  DBUG_ENTER("tile::mytile::find_columns_to_add");

  // create a set of the existing atts
  std::unordered_set<std::string> orig_atts;
  for (uint orig_index = 0; orig_index < orig_table->s->fields; orig_index++) {
    Field *curr_field_in_orig = orig_table->field[orig_index];
    orig_atts.insert(curr_field_in_orig->field_name.str);
  }

  // loop over the atts of the new schema and add those who are not present in
  // the set
  for (uint new_index = 0; new_index < new_table->s->fields; new_index++) {
    Field *curr_field_in_new = new_table->field[new_index];
    auto it = orig_atts.find(curr_field_in_new->field_name.str);
    if (it == orig_atts.end()) {
      bool is_nullable = field_is_nullable(curr_field_in_new);

      // add filters
      tiledb::FilterList filter_list(context);
      if (curr_field_in_new->option_struct->filters != nullptr) {
        filter_list = tile::parse_filter_list(
            context, curr_field_in_new->option_struct->filters);
      }
      tiledb::Attribute attr =
          tile::create_field_attribute(context, curr_field_in_new, filter_list);

      // set if nullable
      attr.set_nullable(is_nullable);
      columns_to_be_added.push_back(attr);
    }
  }
  DBUG_VOID_RETURN;
}

bool tile::mytile::inplace_alter_table(TABLE *altered_table,
                                       Alter_inplace_info *ha_alter_info) {
  DBUG_ENTER("tile::mytile::inplace_alter_table");

  // create evolution object
  auto evolution = tiledb::ArraySchemaEvolution(this->ctx);
  // if the evolution is a DROP column
  if (ha_alter_info->handler_flags & ALTER_DROP_COLUMN) {
    // figuring out what columns have been dropped
    std::vector<std::string> columns_to_be_dropped;
    find_columns_to_drop(altered_table, this->table, columns_to_be_dropped);

    // drop columns
    for (const std::string &col : columns_to_be_dropped) {
      evolution.drop_attribute(col.c_str());
    }
    // evolve array
    evolution.array_evolve(this->uri);

    DBUG_RETURN(false); // success

    // if the evolution is an ADD column
  } else if (ha_alter_info->handler_flags & ALTER_ADD_COLUMN) {
    // figuring out what columns to add
    std::vector<tiledb::Attribute> atts;
    find_columns_to_add(altered_table, this->table, this->ctx, atts);

    // adding attributes
    for (const tiledb::Attribute &a : atts) {
      evolution.add_attribute(a);
    }

    // evolve array
    evolution.array_evolve(this->uri);
    DBUG_RETURN(false); // success
  }

  DBUG_RETURN(true); // error
}

enum_alter_inplace_result tile::mytile::check_if_supported_inplace_alter(
    TABLE *altered_table, Alter_inplace_info *const ha_alter_info) {
  DBUG_ENTER("tile::check_if_supported_inplace_alter");

  DBUG_ASSERT(ha_alter_info != nullptr);

  // checking if ALTER operation is supported
  if (ha_alter_info->handler_flags & (ALTER_ADD_COLUMN | ALTER_DROP_COLUMN)) {
    DBUG_RETURN(HA_ALTER_INPLACE_EXCLUSIVE_LOCK); // todo check lock type
  }

  my_printf_error(ER_ALTER_OPERATION_NOT_SUPPORTED,
                  "[SchemaEvolution] ALTER operation not supported. TileDB "
                  "supports only ADD and DROP.",
                  ME_ERROR_LOG | ME_FATAL);
  DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
}

bool tile::mytile::field_has_default_value(Field *field) const {
  DBUG_ENTER("tile::field_has_default_value");

  bool has_no_default_value = field->flags & NO_DEFAULT_VALUE_FLAG;

  DBUG_RETURN(!has_no_default_value);
}

bool tile::mytile::field_is_nullable(Field *field) const {
  DBUG_ENTER("tile::field_is_nullable");

  bool has_not_null = field->flags & NOT_NULL_FLAG;

  DBUG_RETURN(!has_not_null);
}

uint64_t tile::mytile::get_default_value_size(const void *value,
                                              tiledb_datatype_t type) const {
  DBUG_ENTER("tile::get_default_value_size");
  uint64_t size = 0;

  if (type == TILEDB_STRING_ASCII) {
    auto str = std::string(static_cast<const char *>(value));
    size = str.size();
  } else {
    size = tiledb_datatype_size(type);
  }

  DBUG_RETURN(size);
}

void tile::mytile::get_field_default_value(TABLE *table_arg, size_t field_idx,
                                           tiledb::Attribute *attr,
                                           std::shared_ptr<buffer> buff) {
  DBUG_ENTER("tile::get_field_default_value");

  this->record_index = 0;

  buff->name = table_arg->s->field[field_idx]->field_name.str;
  buff->dimension = false;
  buff->buffer_offset = 0;
  buff->fixed_size_elements = 1;

  auto size = tile::sysvars::write_buffer_size(this->ha_thd());
  buff->buffer_size = 0;
  buff->allocated_buffer_size = size;

  uint64_t *offset_buffer = nullptr;
  tiledb_datatype_t datatype = tile::mysqlTypeToTileDBType(
      table_arg->s->field[field_idx]->type(), false);
  auto data_buffer = alloc_buffer(datatype, size);
  buff->fixed_size_elements = attr->cell_val_num();
  if (attr->variable_sized()) {
    offset_buffer = static_cast<uint64_t *>(
        alloc_buffer(tiledb_datatype_t::TILEDB_UINT64, size));
    buff->offset_buffer_size = 0;
    buff->allocated_offset_buffer_size = size;
  }

  buff->validity_buffer = nullptr;

  if (attr->nullable()) {
    buff->validity_buffer = static_cast<uint8 *>(
        alloc_buffer(tiledb_datatype_t::TILEDB_UINT8, size));
    buff->validity_buffer_size = 0;
    buff->allocated_validity_buffer_size = size;
  }

  buff->offset_buffer = offset_buffer;
  buff->buffer = data_buffer;
  buff->type = attr->type();

  set_buffer_from_field(table_arg->s->field[field_idx], buff,
                        this->record_index, ha_thd(), attr->nullable());

  DBUG_VOID_RETURN;
}

int tile::mytile::create_array(const char *name, TABLE *table_arg,
                               HA_CREATE_INFO *create_info,
                               tiledb::Context context) {
  DBUG_ENTER("tile::create_array");
  int rc = 0;

  try {
    std::unique_ptr<tiledb::ArraySchema> schema;
    tiledb::VFS vfs(ctx);
    // Get array uri from name or table option
    std::string create_uri = name;

    if ((strncmp(table_arg->s->table_name.str, "s3://", 5) == 0) ||
        (strncmp(table_arg->s->table_name.str, "azure://", 8) == 0) ||
        (strncmp(table_arg->s->table_name.str, "gcs://", 6) == 0) ||
        (strncmp(table_arg->s->table_name.str, "tiledb://", 9) == 0))
      create_uri = table_arg->s->table_name.str;

    if (create_info->option_struct->array_uri != nullptr)
      create_uri = create_info->option_struct->array_uri;

    std::string encryption_key;
    if (create_info->option_struct->encryption_key != nullptr) {
      encryption_key = std::string(create_info->option_struct->encryption_key);
    }

    if (check_array_exists(vfs, ctx, create_uri, encryption_key, schema) &&
        sysvars::create_allow_subset_existing_array(ha_thd())) {
      // Next we write the frm file to persist the newly created table
      table_arg->s->write_frm_image();
      DBUG_RETURN(rc);
    }

    tiledb_array_type_t arrayType = TILEDB_SPARSE;
    if (create_info->option_struct->array_type == 1) {
      arrayType = TILEDB_SPARSE;
    } else {
      arrayType = TILEDB_DENSE;
    }

    // Create array schema
    schema = std::make_unique<tiledb::ArraySchema>(context, arrayType);

    // Create domain
    tiledb::Domain domain(context);

    // Only a single key is support, and that is the primary key. We can use the
    // primary key as an alternative to get which fields are suppose to be the
    // dimensions
    std::unordered_map<std::string, bool> primaryKeyParts;
    bool allows_dups = true;
    if (table_arg->key_info != nullptr) {
      uint key_index = 0;

      // Check for primary key
      if (table_arg->s->primary_key != MAX_KEY) {
        key_index = table_arg->s->primary_key;
        allows_dups = false;
      }
      KEY key_info = table_arg->key_info[key_index];
      for (uint i = 0; i < key_info.user_defined_key_parts; i++) {
        Field *field = key_info.key_part[i].field;
        primaryKeyParts[field->field_name.str] = true;

        try {
          tiledb::FilterList filter_list(context);
          if (field->option_struct->filters != nullptr) {
            filter_list =
              tile::parse_filter_list(context, field->option_struct->filters);
          } 

          auto dim = create_field_dimension(context, field, arrayType);
          if (filter_list.nfilters() > 0) {
            dim.set_filter_list(filter_list);
          }
          domain.add_dimension(dim);
        } catch (const std::exception &e) {
          // Log errors
          my_printf_error(
              ER_UNKNOWN_ERROR,
              "[create_array] error creating dimension for table %s : %s",
              ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
          DBUG_RETURN(ERR_CREATE_DIM_OTHER);
        }
      }
    }

    if (schema->array_type() == TILEDB_SPARSE && allows_dups)
      schema->set_allows_dups(allows_dups);

    // Create attributes or dimensions
    for (size_t field_idx = 0; table_arg->field[field_idx]; field_idx++) {
      Field *field = table_arg->field[field_idx];
      // Keys are created above
      if (primaryKeyParts.find(field->field_name.str) !=
          primaryKeyParts.end()) {
        continue;
      }

      bool has_default_value = field_has_default_value(field);
      bool is_nullable = field_is_nullable(field);

      // we currently ignore default values for dimensions
      // If the field has the dimension flag set or it is part of the primary
      // key we treat it is a dimension
      if (field->option_struct->dimension) {
        /* allow for backward compatability - we will catch nulls here on insert
        if (is_nullable) {
          my_printf_error(ER_UNKNOWN_ERROR,
                          "[create_array] error creating dimension for table %s
        : %s", ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), "nullable dimensions
        are not allowed"); DBUG_RETURN(ERR_CREATE_DIM_NULL);
        }
        */
        try {
          tiledb::FilterList filter_list(context);
          if (field->option_struct->filters != nullptr) {
            filter_list =
              tile::parse_filter_list(context, field->option_struct->filters);
          }

          auto dim = create_field_dimension(context, field, arrayType);
          if (filter_list.nfilters() > 0) {
            dim.set_filter_list(filter_list);
          }

          domain.add_dimension(dim);
        } catch (const std::exception &e) {
          // Log errors
          my_printf_error(
              ER_UNKNOWN_ERROR,
              "[create_array] error creating dimension for table %s : %s",
              ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
          DBUG_RETURN(ERR_CREATE_DIM_OTHER);
        }
      } else {
        tiledb::FilterList filter_list(context);
        if (field->option_struct->filters != nullptr) {
          filter_list =
              tile::parse_filter_list(context, field->option_struct->filters);
        }

        tiledb::Attribute attr =
            create_field_attribute(context, field, filter_list);

        attr.set_nullable(is_nullable);

        if (has_default_value) {
          std::shared_ptr<buffer> buff = std::make_shared<buffer>();
          get_field_default_value(table_arg, field_idx, &attr, buff);
          uint64_t default_value_size =
              get_default_value_size(buff->buffer, buff->type);
          if (default_value_size > 0) {
            if (is_nullable)
              attr.set_fill_value(buff->buffer, default_value_size,
                                  buff->validity_buffer[0]);
            else
              attr.set_fill_value(buff->buffer, default_value_size);
          }
          dealloc_buffer(buff);
        }

        // we create a vector with the enum labels and we add it to the
        // corresponding attribute
        if (field->real_type() == MYSQL_TYPE_ENUM) {
          std::vector<std::string> enum_values;
          // casting to field enum
          Field_enum *field_enum = (Field_enum *)field;
          const TYPELIB *enum_typelib = field_enum->get_typelib();
          std::string enum_name = std::string(field->field_name.str) + "_enum";

          // getting all the enum labels to create the TileDB enum
          if (enum_typelib != nullptr) {
            const tiledb::ArraySchema &const_schema = *schema;
            for (uint i = 0; i < enum_typelib->count; ++i) {
              enum_values.push_back(enum_typelib->type_names[i]);
            }
            // setting the enum to the attribute
            auto enmr =
                tiledb::Enumeration::create(ctx, enum_name, enum_values);
            tiledb::ArraySchemaExperimental::add_enumeration(ctx, const_schema,
                                                             enmr);
            tiledb::AttributeExperimental::set_enumeration_name(ctx, attr,
                                                                enum_name);
          }
        }
        schema->add_attribute(attr);
      };
    }

    if (create_info->option_struct->coordinate_filters != nullptr) {
      tiledb::FilterList filter_list = tile::parse_filter_list(
          context, create_info->option_struct->coordinate_filters);
      schema->set_coords_filter_list(filter_list);
    }

    if (create_info->option_struct->offset_filters != nullptr) {
      tiledb::FilterList filter_list = tile::parse_filter_list(
          context, create_info->option_struct->offset_filters);
      schema->set_offsets_filter_list(filter_list);
    }

    if (create_info->option_struct->validity_filters != nullptr) {
      tiledb::FilterList filter_list = tile::parse_filter_list(
          context, create_info->option_struct->validity_filters);
      schema->set_validity_filter_list(filter_list);
    }

    schema->set_domain(domain);

    // Set capacity
    schema->set_capacity(create_info->option_struct->capacity);

    // Set cell ordering if configured
    if (create_info->option_struct->cell_order == 0) {
      schema->set_cell_order(TILEDB_ROW_MAJOR);
    } else if (create_info->option_struct->cell_order == 1) {
      schema->set_cell_order(TILEDB_COL_MAJOR);
    } else if (create_info->option_struct->cell_order == 2) {
      schema->set_cell_order(TILEDB_HILBERT);
    }

    // Set tile ordering if configured and not HILBERT cell order
    if (create_info->option_struct->cell_order == 2) {
      if (create_info->option_struct->tile_order == 0) {
        schema->set_tile_order(TILEDB_ROW_MAJOR);
      } else if (create_info->option_struct->tile_order == 1) {
        schema->set_tile_order(TILEDB_COL_MAJOR);
      }
    }

    // Check array schema
    try {
      schema->check();
    } catch (tiledb::TileDBError &e) {
      my_printf_error(ER_UNKNOWN_ERROR, "Error in building schema %s",
                      ME_ERROR_LOG | ME_FATAL, e.what());
      DBUG_RETURN(ERR_BUILD_SCHEMA);
    }

    try {
      // Create the array on storage
      tiledb::Array::create(create_uri, *schema);
      // Next we write the frm file to persist the newly created table
      table_arg->s->write_frm_image();
    } catch (tiledb::TileDBError &e) {
      my_printf_error(ER_UNKNOWN_ERROR, "Error in creating array %s",
                      ME_ERROR_LOG | ME_FATAL, e.what());
      DBUG_RETURN(ERR_CREATE_ARRAY);
    }
  } catch (tiledb::TileDBError &e) {
    my_printf_error(ER_UNKNOWN_ERROR, "Error in creating table %s",
                    ME_ERROR_LOG | ME_FATAL, e.what());
    DBUG_RETURN(ERR_CREATE_TABLE);
  }
  DBUG_RETURN(rc);
}

uint64_t tile::mytile::computeRecordsUB() {
  DBUG_ENTER("tile::mytile::computeRecordsUB");

  std::string dimension_or_attribute_name = "";
  uint64_t size_of_record = 0;
  uint64_t max_size = std::numeric_limits<uint64_t>::min();

  try {
    tiledb::Domain domain = this->array_schema->domain();
    for (size_t dim_index = 0; dim_index < domain.ndim(); dim_index++) {
      auto dim = domain.dimension(dim_index);
      tiledb_datatype_t datatype = dim.type();
      size_of_record += tiledb_datatype_size(datatype);
      dimension_or_attribute_name = dim.name();
      uint64_t size = 0;
      if (dim.cell_val_num() == TILEDB_VAR_NUM) {
        auto sizes =
            this->query->est_result_size_var(dimension_or_attribute_name);
#if TILEDB_VERSION_MAJOR >= 2 and TILEDB_VERSION_MINOR >= 2
        size = sizes[0];
#else
        size = sizes.first;
#endif
      } else {
        this->ctx.handle_error(tiledb_query_get_est_result_size(
            this->ctx.ptr().get(), this->query->ptr().get(),
            dimension_or_attribute_name.c_str(), &size));
      }

      if (size > max_size) {
        max_size = size;
        break;
      }
    }
  } catch (tiledb::TileDBError &e) {
    my_printf_error(ER_UNKNOWN_ERROR,
                    "Error in calculating upper bound for records %s",
                    ME_ERROR_LOG | ME_FATAL, e.what());
    DBUG_RETURN(ERR_CALC_UPPER_BOUND);
  }

  uint64_t num_of_records = max_size / size_of_record;

  DBUG_RETURN(num_of_records);
}

int tile::mytile::init_scan(THD *thd) {
  DBUG_ENTER("tile::mytile::init_scan");
  int rc = 0;
  // Reset indicators
  this->record_index = 0;
  this->records = 0;
  this->records_read = 0;
  this->status = tiledb::Query::Status::UNINITIALIZED;

  // Get the read buffer size, either from user session or system setting
  this->read_buffer_size = tile::sysvars::read_buffer_size(thd);

  try {
    // Always reset query object so we make sure no ranges are left set
    this->query = nullptr;

    // Validate the array is open for reads
    open_array_for_reads(thd);

    // Allocate user buffers
    alloc_read_buffers(this->read_buffer_size);

    // Get domain and dimensions
    auto domain = this->array_schema->domain();
    auto dims = domain.dimensions();

    this->subarray = std::unique_ptr<tiledb::Subarray>(
        new tiledb::Subarray(this->ctx, *this->array));

    tile::build_subarray(thd, this->valid_pushed_ranges(),
                         this->valid_pushed_in_ranges(), this->empty_read,
                         domain, this->pushdown_ranges,
                         this->pushdown_in_ranges, this->subarray, &this->ctx,
                         this->array.get());

    // If a query condition on an attribute was set, apply it
    if (this->query_condition != nullptr) {
      this->query->set_condition(*this->query_condition);
    }

    // set subarray
    this->query->set_subarray(*this->subarray);

  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[init_scan] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = ERR_INIT_SCAN_TILEDB;
    // clear out failed query details
    rnd_end();
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[init_scan] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = ERR_INIT_SCAN_OTHER;
    // clear out failed query details
    rnd_end();
  }
  DBUG_RETURN(rc);
}

std::optional<Item_sum::Sumfunctype>
tile::mytile::has_aggregate(THD *thd, const std::string &field) {
  DBUG_ENTER("tile::mytile::has_aggregate");
  if (!tile::sysvars::enable_aggregate_pushdown(ha_thd())) {
    DBUG_RETURN(std::nullopt);
  }

  // Get the current SELECT statement
  SELECT_LEX *select_lex = thd->lex->current_select;

  if (select_lex->agg_func_used()) {
    // Iterate through the item list to find TileDB compatible aggregates
    Item *item;
    List_iterator_fast<Item> it(select_lex->item_list);
    while ((item = it++)) {
      Item_sum *isp = dynamic_cast<Item_sum *>(item);
      if (isp) {
        std::string column_with_aggregate;
        const char *str_ptr = isp->get_arg(0)->name.str;
        if (str_ptr != nullptr) {
          std::string column_with_aggregate = str_ptr;
        } else {
          continue;
        }

        if (field == column_with_aggregate) {
          switch (isp->sum_func()) {
          case Item_sum::SUM_FUNC:
            DBUG_RETURN(Item_sum::SUM_FUNC);
            break;
          case Item_sum::AVG_FUNC:
            DBUG_RETURN(Item_sum::AVG_FUNC);
            break;
          case Item_sum::MIN_FUNC:
            DBUG_RETURN(Item_sum::MIN_FUNC);
            break;
          case Item_sum::COUNT_FUNC:
            DBUG_RETURN(Item_sum::COUNT_FUNC);
            break;
          case Item_sum::MAX_FUNC:
            DBUG_RETURN(Item_sum::MAX_FUNC);
            break;
          default:
            DBUG_RETURN(std::nullopt);
          }
        }
      }
    }
  }
  DBUG_RETURN(std::nullopt);
}

int tile::mytile::load_metadata() {
  DBUG_ENTER("tile::mytile::load_metadata");
  int rc = 0;

  open_array_for_reads(ha_thd());
  uint64_t longest_key = 0;
  this->metadata_map =
      tile::build_metadata_map(ha_thd(), this->array, &longest_key);

  if (longest_key > this->ref_length) {
    this->ref_length = longest_key + 1;
  }

  DBUG_RETURN(rc);
}

int tile::mytile::rnd_init(bool scan) {
  DBUG_ENTER("tile::mytile::rnd_init");
  // Handle metadata queries
  if (metadata_query) {
    int rc = this->load_metadata();
    this->metadata_map_iterator = this->metadata_map.begin();
    DBUG_RETURN(rc);
  }
  DBUG_RETURN(init_scan(this->ha_thd()));
};

bool tile::mytile::query_complete() {
  DBUG_ENTER("tile::mytile::query_complete");
  // If we are complete and there is no more records we report EOF
  if (this->status == tiledb::Query::Status::COMPLETE &&
      this->record_index >= this->records) {
    DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}

int tile::mytile::scan_rnd_row(TABLE *table) {
  DBUG_ENTER("tile::mytile::scan_rnd_row");
  int rc = 0;
  const char *query_status;

  // If the query is empty, we should abort early
  // We have to check this here and not in rnd_init because
  // only rnd_next can return empty
  if (this->empty_read) {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  // We must set the bitmap for debug purpose, it is "write_set" because we use
  // Field->store
  MY_BITMAP *original_bitmap =
      dbug_tmp_use_all_columns(table, &table->write_set);
  tiledb_query_status_to_str(static_cast<tiledb_query_status_t>(status),
                             &query_status);

  if (this->query_complete()) {
    // Reset bitmap to original
    dbug_tmp_restore_column_map(&table->write_set, original_bitmap);
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  try {
    // If the cursor has passed the number of records from the previous query
    // (or if this is the first time), (re)submit the query->
    if (this->record_index >= this->records) {
      do {
        this->status = query->submit();
        // Compute the number of cells (records) that were returned by the query
        auto buff = this->buffers[0];
        if (buff->offset_buffer != nullptr) {
          this->records = buff->offset_buffer_size / sizeof(uint64_t);
        } else {
          this->records = buff->buffer_size / tiledb_datatype_size(buff->type);
        }

        // Increase the buffer allocation and resubmit if necessary.
        if (this->status == tiledb::Query::Status::INCOMPLETE &&
            this->records == 0) { // VERY IMPORTANT!!
          this->read_buffer_size = this->read_buffer_size * 2;
          dealloc_buffers();
          alloc_read_buffers(read_buffer_size);
        } else if (records > 0) {
          this->record_index = 0;
          // Break out of resubmit loop as we have some results.
          break;
        } else if (this->records == 0 &&
                   this->status == tiledb::Query::Status::COMPLETE) {
          // Reset bitmap to original
          dbug_tmp_restore_column_map(&table->write_set, original_bitmap);
          DBUG_RETURN(HA_ERR_END_OF_FILE);
        }
      } while (status == tiledb::Query::Status::INCOMPLETE);
    }

    tileToFields(record_index, false, table);

    this->record_index++;
    this->records_read++;

  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[scan_rnd_row] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = ERR_SCAN_RND_ROW_TILEDB;
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[scan_rnd_row] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = ERR_SCAN_RND_ROW_OTHER;
  }

  // Reset bitmap to original
  dbug_tmp_restore_column_map(&table->write_set, original_bitmap);
  DBUG_RETURN(rc);
}

int tile::mytile::rnd_next(uchar *buf) {
  DBUG_ENTER("tile::mytile::rnd_next");
  if (this->metadata_query) {
    DBUG_RETURN(metadata_next());
  }
  DBUG_RETURN(scan_rnd_row(table));
}

int tile::mytile::metadata_to_fields(
    const std::pair<std::string, std::string> &metadata) {
  DBUG_ENTER("tile::mytile::metadata_to_fields");
  int rc = 0;
  // We must set the bitmap for debug purpose, it is "write_set" because we use
  // Field->store
  MY_BITMAP *original_bitmap =
      dbug_tmp_use_all_columns(table, &table->write_set);

  for (Field **ffield = this->table->field; *ffield; ffield++) {
    Field *field = (*ffield);
    field->set_notnull();
    if (std::strcmp(field->field_name.str, "key") == 0) {
      rc = field->store(metadata.first.c_str(), metadata.first.length(),
                        &my_charset_latin1);
    } else if (std::strcmp(field->field_name.str, "value") == 0) {
      rc = field->store(metadata.second.c_str(), metadata.second.length(),
                        &my_charset_latin1);
    }
  }

  // Reset bitmap to original
  dbug_tmp_restore_column_map(&table->write_set, original_bitmap);
  DBUG_RETURN(rc);
}

int tile::mytile::metadata_next() {
  DBUG_ENTER("'tile::mytile::metadata_next");
  int rc = 0;
  // Check if we are out of metadata
  if (this->metadata_map_iterator == this->metadata_map.end()) {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  this->metadata_last_value = *this->metadata_map_iterator;

  rc = metadata_to_fields(this->metadata_last_value);

  // Move to next iterator
  ++this->metadata_map_iterator;

  DBUG_RETURN(rc);
}

int tile::mytile::rnd_end() {
  DBUG_ENTER("tile::mytile::rnd_end");
  dealloc_buffers();
  this->pushdown_ranges.clear();
  this->pushdown_in_ranges.clear();
  this->query_condition = nullptr;
  // Reset indicators
  this->record_index = 0;
  this->records = 0;
  this->records_read = 0;
  this->status = tiledb::Query::Status::UNINITIALIZED;
  this->query = nullptr;
  ds_mrr.dsmrr_close();
  DBUG_RETURN(close());
};

/**
 * Returns the coordinates as a single byte vector in the form
 * <uint64_t>-<data>-<uint64_t>-<data>
 *
 * Where the prefix of <uint64_t> is the length of the data to follow
 *
 * @param index
 * @return
 */
std::vector<uint8_t> tile::mytile::get_coords_as_byte_vector(uint64_t index) {
  auto domain = this->array_schema->domain();
  std::vector<uint8_t> data;
  for (uint64_t dim_idx = 0; dim_idx < this->ndim; dim_idx++) {
    tiledb::Dimension dimension = domain.dimension(dim_idx);

    bool var_sized = dimension.cell_val_num() == TILEDB_VAR_NUM;
    for (auto &buff : this->buffers) {
      if (buff == nullptr || buff->name != dimension.name()) {
        continue;
      } else { // the buffer is found
        uint64_t datatype_size = tiledb_datatype_size(dimension.type());
        uint64_t size = datatype_size;
        char *buffer_casted =
            static_cast<char *>(buff->buffer) + (index * datatype_size);
        if (var_sized) {
          uint64_t end_position = index + 1;
          uint64_t start_position = 0;
          // If its not the first value, we need to see where the previous
          // position ended to know where to start.
          if (index > 0) {
            start_position = buff->offset_buffer[index];
          }
          // If the current position is equal to the number of results - 1 then
          // we are at the last varchar value
          if (index >= (buff->offset_buffer_size / sizeof(uint64_t)) - 1) {
            end_position = buff->buffer_size / sizeof(char);
          } else { // Else read the end from the next offset.
            end_position = buff->offset_buffer[index + 1];
          }
          // Set the size
          size = end_position - start_position;
          // Set the casted buffer position
          buffer_casted = static_cast<char *>(buff->buffer) +
                          (start_position * sizeof(char));
        }

        // Copy the size details
        char *size_char = reinterpret_cast<char *>(&size);
        for (uint64_t i = 0; i < sizeof(uint64_t); ++i) {
          data.emplace_back(size_char[i]);
        }
        // The index offset for coordinates is computed based on the number of
        // dimensions, the datatype size and the record index We must subtract
        // one from the record index because position is called after the end
        // of rnd_next which means we have already incremented the index
        // position Current dimension's value is the (record_index - 1) Order of
        // dimensions is important
        for (uint64_t i = 0; i < size; ++i) {
          data.emplace_back(buffer_casted[i]);
        }
      }
    }
  }
  return data;
}

int tile::mytile::rnd_pos(uchar *buf, uchar *pos) {
  DBUG_ENTER("tile::mytile::rnd_pos");

  // Handle metadata queries
  if (this->metadata_query) {
    std::string key(reinterpret_cast<char *>(pos));
    auto it = this->metadata_map.find(key);
    if (it == this->metadata_map.end()) {
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    }

    DBUG_RETURN(metadata_to_fields(*it));
  }

  try {
    // Always reset query object so we make sure no ranges are left set
    this->query = nullptr;

    // Validate the array is open for reads
    open_array_for_reads(ha_thd());

    // Allocate user buffers
    alloc_read_buffers(this->read_buffer_size);

    auto domain = this->array_schema->domain();
    int current_offset = 0;
    this->subarray = std::unique_ptr<tiledb::Subarray>(
        new tiledb::Subarray(this->ctx, *this->array));
    // Reads dimensions in the same order as we wrote them
    // to ref in tile::mytile::position
    for (uint64_t dim_idx = 0; dim_idx < this->ndim; dim_idx++) {
      tiledb::Dimension dimension = domain.dimension(dim_idx);

      // Get datatype size
      uint64_t size = *reinterpret_cast<uint64_t *>(pos + current_offset);
      current_offset += sizeof(uint64_t);
      // Fetch the point
      void *point = pos + current_offset;
      // Move current offset for the next dimension's cordinate
      current_offset += size;

      // set range
      if (dimension.cell_val_num() == TILEDB_VAR_NUM) {
        this->ctx.handle_error(tiledb_subarray_add_range_var(
            this->ctx.ptr().get(), this->subarray->ptr().get(), dim_idx, point,
            size, point, size));
      } else {
        this->ctx.handle_error(tiledb_subarray_add_range(
            this->ctx.ptr().get(), this->subarray->ptr().get(), dim_idx, point,
            point, nullptr));
      }
    }
    this->query->set_subarray(*this->subarray);

    // Reset indicators
    this->record_index = 0;
    this->records = 0;
    this->records_read = 0;
    this->status = tiledb::Query::Status::UNINITIALIZED;
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[rnd_pos] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    DBUG_RETURN(ERR_RND_POS_TILEDB);
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[rnd_pos] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    DBUG_RETURN(ERR_RND_POS_OTHER);
  }
  DBUG_RETURN(rnd_next(buf));
}

// Fetches each buffer coordinate and store coordinate.
void tile::mytile::position(const uchar *record) {
  DBUG_ENTER("tile::mytile::position");

  // Handle metadata queries
  if (this->metadata_query) {
    memcpy(this->ref, this->metadata_last_value.first.c_str(),
           this->metadata_last_value.first.length() + 1);
    DBUG_VOID_RETURN;
  }

  std::vector<uint8_t> coords =
      get_coords_as_byte_vector(this->record_index - 1);

  if (coords.size() > this->ref_length) {
    my_printf_error(
        ER_UNKNOWN_ERROR,
        "[position] error dimensions longer than ref_length for %s :",
        ME_ERROR_LOG | ME_FATAL, this->uri.c_str());
    DBUG_VOID_RETURN;
  }

  memcpy(this->ref, coords.data(),
         std::min<uint64_t>(this->ref_length, coords.size()));

  DBUG_VOID_RETURN;
}

void tile::mytile::dealloc_buffer(std::shared_ptr<buffer> buff) const {
  DBUG_ENTER("tile::mytile::dealloc_buffer");

  if (buff->validity_buffer != nullptr) {
    free(buff->validity_buffer);
    buff->validity_buffer = nullptr;
  }

  if (buff->offset_buffer != nullptr) {
    free(buff->offset_buffer);
    buff->offset_buffer = nullptr;
  }

  if (buff->buffer != nullptr) {
    free(buff->buffer);
    buff->buffer = nullptr;
  }

  DBUG_VOID_RETURN;
}

void tile::mytile::dealloc_buffers() {
  DBUG_ENTER("tile::mytile::dealloc_buffers");
  // Free allocated buffers
  for (auto &buff : this->buffers) {
    // Ignore empty buffers
    if (buff == nullptr)
      continue;

    dealloc_buffer(buff);
  }

  this->buffers.clear();
  DBUG_VOID_RETURN;
}

const COND *tile::mytile::cond_push_cond(Item_cond *cond_item) {
  DBUG_ENTER("tile::mytile::cond_push_cond");
  tiledb_query_condition_combination_op_t op;

  switch (cond_item->functype()) {
  case Item_func::COND_AND_FUNC:
    op = TILEDB_AND;
    break;
  case Item_func::COND_OR_FUNC:
    op = TILEDB_OR;
    break;
  default:
    DBUG_RETURN(cond_item);
  }

  List<Item> *arglist = cond_item->argument_list();
  List_iterator<Item> li(*arglist);
  const Item *subitem;
  std::shared_ptr<tiledb::QueryCondition> queryCondition;
  std::shared_ptr<tiledb::QueryCondition> operatorCondition;

  // Depending on the condition type (OR, AND), we create a combination of
  // conditions. Then we combine this combined Query Condition with the
  // "primary" Query Condition with an AND. In the end, the "primary" Query
  // Condition contains all the sub conditions.
  for (uint32_t i = 0; i < arglist->elements; i++) {
    if ((subitem = li++)) {
      // COND_ITEMs
      cond_push_local(dynamic_cast<const COND *>(subitem), queryCondition);
      // Dimensions do not support QCs, hence the queryCondition ptr
      // returned from cond_push_local() will be null, so skip
      if (queryCondition != nullptr) {
        if (operatorCondition == nullptr) {
          // if we are dealing with the first qc of this multi-predicate
          // operator, we need to initialize
          operatorCondition =
              std::make_shared<tiledb::QueryCondition>(*queryCondition);
        } else {
          tiledb::QueryCondition tempCondition =
              queryCondition->combine(*operatorCondition, op);
          operatorCondition =
              std::make_shared<tiledb::QueryCondition>(tempCondition);
        }
      }
    }
  }
  if (operatorCondition != nullptr) {
    if (this->query_condition == nullptr) {
      this->query_condition =
          std::make_shared<tiledb::QueryCondition>(*operatorCondition);
    } else {
      // Combine all previous QCs with the current
      tiledb::QueryCondition qc =
          this->query_condition->combine(*operatorCondition, TILEDB_AND);
      this->query_condition = std::make_shared<tiledb::QueryCondition>(qc);
    }
  }
  DBUG_RETURN(nullptr);
}

const COND *tile::mytile::cond_push_func_datetime(
    const Item_func *func_item,
    std::shared_ptr<tiledb::QueryCondition> &qcPtr) {
  DBUG_ENTER("tile::mytile::cond_push_func_datetime");
  Item **args = func_item->arguments();
  bool neg = FALSE;

  Item_field *column_field = dynamic_cast<Item_field *>(args[0]);
  // If we can't convert the condition to a column let's bail
  // We should add support at some point for handling functions (i.e.
  // date_dimension = current_date())
  if (column_field == nullptr) {
    DBUG_RETURN(func_item);
  }

  // If the arguments are not constants, we must bail
  // We should add support at some point for handling functions (i.e.
  // date_dimension = current_date())
  for (uint i = 1; i < func_item->argument_count(); i++) {
    if (args[i]->type() != Item::CONST_ITEM &&
        func_item->functype() != Item_func::BETWEEN) {
      // datetime args in 'BETWEEN' are not constants
      DBUG_RETURN(func_item);
    }
  }

  // If the condition is not a dimension we must build a query_condition
  bool use_query_condition = false;

  uint64_t dim_idx = 0;
  tiledb_datatype_t datatype = tiledb_datatype_t::TILEDB_ANY;

  // Check attributes
  bool nullable = false;
  if (this->array_schema->has_attribute(column_field->field_name.str)) {

    // Dense arrays do not support query conditions.
    auto has_aggr = has_aggregate(ha_thd(), column_field->field_name.str);
    if (has_aggr == Item_sum::COUNT_FUNC) {
      DBUG_RETURN(func_item);
    }

    if (this->array_schema->array_type() == TILEDB_DENSE && !has_aggr) {
      DBUG_RETURN(func_item);
    }

    auto attr = this->array_schema->attribute(column_field->field_name.str);
    datatype = attr.type();
    nullable = attr.nullable();

    if (!attr.variable_sized() ||
        (attr.variable_sized() &&
         (datatype == TILEDB_STRING_ASCII || datatype == TILEDB_STRING_UTF8))) {
      use_query_condition = true;
    } else {
      // If we can't use query condition let MariaDB filter the attribute
      DBUG_RETURN(func_item);
    }
  } else {
    // Check dimensions
    auto dims = this->array_schema->domain().dimensions();
    for (uint64_t j = 0; j < this->ndim; j++) {
      if (dims[j].name() == column_field->field_name.str) {
        dim_idx = j;
        datatype = dims[j].type();
      }
    }
  }

  switch (func_item->functype()) {
  case Item_func::ISNULL_FUNC: {
    DBUG_RETURN(func_item);
    if (!use_query_condition || !nullable)
      DBUG_RETURN(func_item); /* Is null is not supported for ranges*/

    qcPtr = std::make_shared<tiledb::QueryCondition>(ctx);
    qcPtr->init(column_field->field_name.str, nullptr, 0, TILEDB_EQ);
    break;
  }
  case Item_func::ISNOTNULL_FUNC: {
    DBUG_RETURN(func_item);
    if (!use_query_condition || !nullable)
      DBUG_RETURN(func_item); /* Is not null is not supported for ranges*/

    qcPtr = std::make_shared<tiledb::QueryCondition>(ctx);
    qcPtr->init(column_field->field_name.str, nullptr, 0, TILEDB_NE);
    break;
  }
  case Item_func::NE_FUNC: {
    if (!use_query_condition)
      DBUG_RETURN(func_item); /* Not equal is not supported for ranges */

    // Create unique ptrs
    std::shared_ptr<range> range = std::make_shared<tile::range>(tile::range{
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        func_item->functype(), tiledb_datatype_t::TILEDB_ANY, 0, 0});

    // Get field type for comparison
    Item_result cmp_type = args[1]->cmp_type();

    if (TileDBDateTimeType(datatype)) {
      cmp_type = TIME_RESULT;
    }

    int ret = set_range_from_item_datetime(
        ha_thd(), dynamic_cast<Item_basic_constant *>(args[1]),
        dynamic_cast<Item_basic_constant *>(args[1]), cmp_type, range,
        datatype);

    if (ret)
      DBUG_RETURN(func_item);

    // If this is an attribute add it to the query condition
    qcPtr = std::make_shared<tiledb::QueryCondition>(
        range->QueryCondition(ctx, column_field->field_name.str));
    break;
  }
    // In is special because we need to do a tiledb range per argument and treat
    // it as OR not AND
  case Item_func::IN_FUNC:
    // Start at 1 because 0 is the field
    for (uint i = 1; i < func_item->argument_count(); i++) {
      Item_basic_constant *lower_const =
          dynamic_cast<Item_basic_constant *>(args[i]);
      // Init upper to be same becase for in clauses this is required
      Item_basic_constant *upper_const =
          dynamic_cast<Item_basic_constant *>(args[i]);

      // Create unique ptrs
      std::shared_ptr<range> range = std::make_shared<tile::range>(tile::range{
          std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
          std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
          Item_func::EQ_FUNC, tiledb_datatype_t::TILEDB_ANY, 0, 0});

      // Get field type for comparison
      Item_result cmp_type = args[i]->cmp_type();

      if (TileDBDateTimeType(datatype)) {
        cmp_type = TIME_RESULT;
      }

      int ret = set_range_from_item_datetime(ha_thd(), lower_const, upper_const,
                                             cmp_type, range, datatype);

      if (ret)
        DBUG_RETURN(func_item);

      // If this is an attribute add it to the query condition
      if (use_query_condition) {
        // IN clauses are not supported
        // When they are uncomment below
        DBUG_RETURN(func_item);
        /*if (this->query_condition == nullptr) {
          this->query_condition = std::make_shared<tiledb::QueryCondition>(
              range->QueryCondition(ctx, column_field->field_name.str));
        } else {
          tiledb::QueryCondition qc = this->query_condition->combine(
              range->QueryCondition(ctx, column_field->field_name.str),
              TILEDB_AND);
          this->query_condition = std::make_shared<tiledb::QueryCondition>(qc);
        }*/
      } else {
        // Add the range to the pushdown in ranges
        auto &range_vec = this->pushdown_in_ranges[dim_idx];
        range_vec.push_back(std::move(range));
      }
    }

    break;
    // Handle equal case by setting upper and lower ranges to same value
  case Item_func::EQ_FUNC: {
    // Create unique ptrs
    std::shared_ptr<range> range = std::make_shared<tile::range>(tile::range{
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        func_item->functype(), tiledb_datatype_t::TILEDB_ANY, 0, 0});

    // Get field type for comparison
    Item_result cmp_type = args[1]->cmp_type();

    if (TileDBDateTimeType(datatype)) {
      cmp_type = TIME_RESULT;
    }

    int ret = set_range_from_item_datetime(ha_thd(), args[1], args[1], cmp_type,
                                           range, datatype);

    if (ret)
      DBUG_RETURN(func_item);

    // If this is an attribute add it to the query condition
    if (use_query_condition) {
      qcPtr = std::make_shared<tiledb::QueryCondition>(
          range->QueryCondition(ctx, column_field->field_name.str));
    } else {
      // Add the range to the pushdown in ranges
      auto &range_vec = this->pushdown_ranges[dim_idx];
      range_vec.push_back(std::move(range));
    }

    break;
  }
  case Item_func::BETWEEN:
    neg = (dynamic_cast<const Item_func_opt_neg *>(func_item))->negated;
    if (neg) // don't support negations!
      DBUG_RETURN(func_item);
  // fall through
  case Item_func::LE_FUNC: // Handle all cases where there is 1 or 2 arguments
                           // we must set on
  case Item_func::LT_FUNC:
  case Item_func::GE_FUNC:
  case Item_func::GT_FUNC: {
    bool between = false;
    // the range
    Item_basic_constant *lower_const = nullptr;
    Item_basic_constant *upper_const = nullptr;

    Item *lower_item_between = nullptr;
    Item *upper_item_between = nullptr;

    // Get field type for comparison
    Item_result cmp_type = args[1]->cmp_type();

    if (TileDBDateTimeType(datatype)) {
      cmp_type = TIME_RESULT;
    }

    // If we have 3 items then we can set lower and upper
    if (func_item->argument_count() == 3) {
      between = true;
      lower_item_between = args[1];
      upper_item_between = args[2];
      // If the condition is less than we know its the upper limit we have
    } else if (func_item->functype() == Item_func::LT_FUNC ||
               func_item->functype() == Item_func::LE_FUNC) {
      upper_const = dynamic_cast<Item_basic_constant *>(args[1]);
      // If the condition is greater than we know its the lower limit we have
    } else if (func_item->functype() == Item_func::GT_FUNC ||
               func_item->functype() == Item_func::GE_FUNC) {
      lower_const = dynamic_cast<Item_basic_constant *>(args[1]);
    }

    // Create unique ptrs
    std::shared_ptr<range> range = std::make_shared<tile::range>(tile::range{
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        func_item->functype(), tiledb_datatype_t::TILEDB_ANY, 0, 0});

    int ret;
    if (between) {
      ret = set_range_from_item_datetime(ha_thd(), lower_item_between,
                                         upper_item_between, cmp_type, range,
                                         datatype);
    } else {

      ret = set_range_from_item_datetime(ha_thd(), lower_const, upper_const,
                                         cmp_type, range, datatype);
    }

    if (ret)
      DBUG_RETURN(func_item);

    // If this is an attribute add it to the query condition
    if (use_query_condition) {
      qcPtr = std::make_shared<tiledb::QueryCondition>(
          range->QueryCondition(ctx, column_field->field_name.str));
    } else {
      // Add the range to the pushdown in ranges
      auto &range_vec = this->pushdown_ranges[dim_idx];
      range_vec.push_back(std::move(range));
    }
    break;
  }
  default:
    DBUG_RETURN(func_item);
  } // endswitch functype
  DBUG_RETURN(nullptr);
}

const COND *tile::mytile::cond_push_func_spatial(
    const Item_func *func_item,
    std::shared_ptr<tiledb::QueryCondition> &qcPtr) {
  DBUG_ENTER("tile::mytile::cond_push_func_spatial");
  Item **args = func_item->arguments();

  // Find the geometry column name in order to identify valid operands
  std::string geometry_column = "wkb_geometry";
  this->load_metadata();
  auto sr = this->metadata_map.find("GEOMETRY_ATTRIBUTE_NAME");
  if (sr != this->metadata_map.end()) {
    geometry_column = sr->second;
  }
  std::string expected_cast = "GeometryFromWkb(" + geometry_column + ")";

  // X and Y dimension names, these may be adjustable via metadata in the future
  std::string x_name = "_X";
  std::string y_name = "_Y";

  // Loop through args, determine which one is
  // a) the cast wkb_geometry attribute/column
  // b) a function that evaluates to a constant geometry
  int wkb_arg = -1;
  int aoi_arg = -1;
  for (uint i = 0; i < func_item->argument_count(); i++) {
    switch (args[i]->type()) {
    case Item::FIELD_ITEM: {
      Item_field *column_field = dynamic_cast<Item_field *>(args[i]);
      if (column_field != nullptr) {
        if (column_field->field_name.str == geometry_column) {
          // If the user passes the column name of the blob-typed attribute,
          // bail. The spatial functions use it despite the type mismatch and
          // return garbage.
          my_printf_error(
              ER_UNKNOWN_ERROR,
              "[cond_push_func_spatial] wkb must be cast, GeometryFromWkb",
              ME_ERROR_LOG | ME_FATAL);
          DBUG_RETURN(nullptr);
        }
      }
      break;
    }
    case Item::CACHE_ITEM: {
      Item_cache *c = dynamic_cast<Item_cache *>(args[i]);
      if (c != nullptr) {
        if (c->const_item() && c->const_during_execution()) {
          aoi_arg = i;
        }
      }
      break;
    }
    case Item::FUNC_ITEM: {
      Item_func *f = dynamic_cast<Item_func *>(args[i]);
      // Special case: Sentinel value used to signify the wkb attribute
      // Has to match the literal cast string e.g.
      // "GeometryFromWkb(wkb_geometry)"
      if (f->full_name() == expected_cast) {
        wkb_arg = i;
      }
      break;
    }
    default: {
      break;
    }
    }
  }

  if (aoi_arg >= 0 && wkb_arg >= 0) {
    // Determine padding
    auto find_x = this->metadata_map.find("PAD_X");
    auto find_y = this->metadata_map.find("PAD_Y");
    double pad_x;
    double pad_y;
    if (find_x != this->metadata_map.end() &&
        find_y != this->metadata_map.end()) {
      pad_x = std::stod(find_x->second);
      pad_y = std::stod(find_y->second);
    } else {
      pad_x = 0.0;
      pad_y = 0.0;
    }

    double x1 = 0;
    double y1 = 0;
    double x2 = 0;
    double y2 = 0;

    // Evaluate to native geometry type
    auto *aoi = dynamic_cast<Item_cache *>(args[aoi_arg]);
    aoi->eval_const_cond();
    if (aoi->has_value()) {
      String arg_val;
      String *swkb = aoi->val_str(&arg_val);
      Geometry_buffer buffer;
      Geometry *geom = NULL;

      geom = Geometry::construct(&buffer, swkb->ptr(), swkb->length());

      // Calculate minimum bounding rectangle of geometry
      if (geom != nullptr) {
        MBR mbr;
        const char *c_end;
        geom->get_mbr(&mbr, &c_end);
        x1 = mbr.xmin;
        y1 = mbr.ymin;
        x2 = mbr.xmax;
        y2 = mbr.ymax;
      } else {
        my_printf_error(ER_UNKNOWN_ERROR,
                        "[cond_push_func_spatial] Invalid constant geometry",
                        ME_ERROR_LOG | ME_FATAL);
      }
    }

    if (x1 == 0 && y1 == 0 && x2 == 0 && y2 == 0) {
      DBUG_RETURN(func_item);
    }

    // Expand the bbox by padding
    x1 = x1 - (pad_x / 2.0);
    y1 = y1 - (pad_y / 2.0);
    x2 = x2 + (pad_x / 2.0);
    y2 = y2 + (pad_y / 2.0);

    // Find X and Y dimensions
    auto dims = this->array_schema->domain().dimensions();
    uint64_t x_idx = std::numeric_limits<uint64_t>::max();
    uint64_t y_idx = std::numeric_limits<uint64_t>::max();
    tiledb_datatype_t x_datatype;
    tiledb_datatype_t y_datatype;

    for (uint64_t d = 0; d < this->ndim; d++) {
      if (dims[d].name() == x_name) {
        x_idx = d;
        x_datatype = dims[d].type();
      } else if (dims[d].name() == y_name) {
        y_idx = d;
        y_datatype = dims[d].type();
      }
    }

    // If we've identified _X and _Y dims, we can implement the pushdown
    if (x_idx <= this->ndim && y_idx <= this->ndim) {
      Item_result cmp_type = Item_result::REAL_RESULT;
      THD *thd = ha_thd();

      // X dimension
      {
        std::shared_ptr<range> range =
            std::make_shared<tile::range>(tile::range{
                std::unique_ptr<void, decltype(&std::free)>(nullptr,
                                                            &std::free),
                std::unique_ptr<void, decltype(&std::free)>(nullptr,
                                                            &std::free),
                Item_func::BETWEEN, tiledb_datatype_t::TILEDB_ANY, 0, 0});

        Item_float *lower = new (thd->mem_root) Item_float(thd, x1, 0);
        Item_float *upper = new (thd->mem_root) Item_float(thd, x2, 0);
        int ret = set_range_from_item_consts(
            thd, dynamic_cast<Item_basic_constant *>(lower),
            dynamic_cast<Item_basic_constant *>(upper), cmp_type, range,
            x_datatype);
        if (ret) {
          DBUG_RETURN(func_item);
        }
        auto &range_vec = this->pushdown_ranges[x_idx];
        range_vec.push_back(std::move(range));
      }

      // Y dimension
      {
        std::shared_ptr<range> range =
            std::make_shared<tile::range>(tile::range{
                std::unique_ptr<void, decltype(&std::free)>(nullptr,
                                                            &std::free),
                std::unique_ptr<void, decltype(&std::free)>(nullptr,
                                                            &std::free),
                Item_func::BETWEEN, tiledb_datatype_t::TILEDB_ANY, 0, 0});

        Item_float *lower = new (thd->mem_root) Item_float(thd, y1, 0);
        Item_float *upper = new (thd->mem_root) Item_float(thd, y2, 0);
        int ret = set_range_from_item_consts(
            thd, dynamic_cast<Item_basic_constant *>(lower),
            dynamic_cast<Item_basic_constant *>(upper), cmp_type, range,
            y_datatype);
        if (ret) {
          DBUG_RETURN(func_item);
        }
        auto &range_vec = this->pushdown_ranges[y_idx];
        range_vec.push_back(std::move(range));
      }
    }
  } // else: not eligible for pushdown

  DBUG_RETURN(nullptr);
}

const COND *
tile::mytile::cond_push_func(const Item_func *func_item,
                             std::shared_ptr<tiledb::QueryCondition> &qcPtr) {
  DBUG_ENTER("tile::mytile::cond_push_func");
  Item **args = func_item->arguments();
  bool neg = FALSE;
  bool is_enum = false;

  Item_field *column_field = dynamic_cast<Item_field *>(args[0]);
  // If we can't convert the condition to a column let's bail
  // We should add support at some point for handling functions (i.e.
  // date_dimension = current_date())
  if (column_field == nullptr) {
    DBUG_RETURN(func_item);
  }

  // If the arguments are not constants, we must bail
  // We should add support at some point for handling functions (i.e.
  // date_dimension = current_date())
  for (uint i = 1; i < func_item->argument_count(); i++) {
    if (args[i]->type() != Item::CONST_ITEM) {
      DBUG_RETURN(func_item);
    }
  }

  // If the condition is not a dimension we must build a query_condition
  bool use_query_condition = false;

  uint64_t dim_idx = 0;
  tiledb_datatype_t datatype = tiledb_datatype_t::TILEDB_ANY;

  // Check attributes
  bool nullable = false;
  if (this->array_schema->has_attribute(column_field->field_name.str)) {

    // Dense arrays do not support query conditions
    auto has_aggr = has_aggregate(ha_thd(), column_field->field_name.str);
    if (has_aggr == Item_sum::COUNT_FUNC) {
      DBUG_RETURN(func_item);
    }

    if (this->array_schema->array_type() == TILEDB_DENSE && !has_aggr) {
      DBUG_RETURN(func_item);
    }

    auto attr = this->array_schema->attribute(column_field->field_name.str);
    datatype = attr.type();
    nullable = attr.nullable();
    auto enmr_name =
        tiledb::AttributeExperimental::get_enumeration_name(ctx, attr);
    is_enum = enmr_name.has_value();

    if (is_enum)
      DBUG_RETURN(func_item); // disable enum push down for now TODO
    if (!attr.variable_sized() ||
        (attr.variable_sized() &&
         (datatype == TILEDB_STRING_ASCII || datatype == TILEDB_STRING_UTF8))) {
      use_query_condition = true;
    } else {
      // If we can't use query condition let MariaDB filter the attribute
      DBUG_RETURN(func_item);
    }

  } else {
    auto dims = this->array_schema->domain().dimensions();
    for (uint64_t j = 0; j < this->ndim; j++) {
      if (dims[j].name() == column_field->field_name.str) {
        dim_idx = j;
        datatype = dims[j].type();
      }
    }
  }

  switch (func_item->functype()) {
  case Item_func::ISNULL_FUNC: {
    DBUG_RETURN(func_item);
    if (!use_query_condition || !nullable)
      DBUG_RETURN(func_item); /* Is null is not supported for ranges*/

    qcPtr = std::make_shared<tiledb::QueryCondition>(ctx);
    qcPtr->init(column_field->field_name.str, nullptr, 0, TILEDB_EQ);
    break;
  }
  case Item_func::ISNOTNULL_FUNC: {
    DBUG_RETURN(func_item);
    if (!use_query_condition || !nullable)
      DBUG_RETURN(func_item); /* Is not null is not supported for ranges*/

    qcPtr = std::make_shared<tiledb::QueryCondition>(ctx);
    qcPtr->init(column_field->field_name.str, nullptr, 0, TILEDB_NE);
    break;
  }
  case Item_func::NE_FUNC: {
    if (!use_query_condition)
      DBUG_RETURN(func_item); /* Not equal is not supported for ranges*/

    // Create unique ptrs
    std::shared_ptr<range> range = std::make_shared<tile::range>(tile::range{
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        func_item->functype(), tiledb_datatype_t::TILEDB_ANY, 0, 0});

    // Get field type for comparison
    Item_result cmp_type = args[1]->cmp_type();

    if (TileDBDateTimeType(datatype)) {
      cmp_type = TIME_RESULT;
    }

    int ret = set_range_from_item_consts(
        ha_thd(), dynamic_cast<Item_basic_constant *>(args[1]),
        dynamic_cast<Item_basic_constant *>(args[1]), cmp_type, range,
        datatype);

    if (ret)
      DBUG_RETURN(func_item);

    // If this is an attribute add it to the query condition
    qcPtr = std::make_shared<tiledb::QueryCondition>(
        range->QueryCondition(ctx, column_field->field_name.str));
    break;
  }
    // In is special because we need to do a tiledb range per argument and treat
    // it as OR not AND
  case Item_func::IN_FUNC:
    // Start at 1 because 0 is the field
    for (uint i = 1; i < func_item->argument_count(); i++) {
      Item_basic_constant *lower_const =
          dynamic_cast<Item_basic_constant *>(args[i]);
      // Init upper to be same because for in clauses this is required
      Item_basic_constant *upper_const =
          dynamic_cast<Item_basic_constant *>(args[i]);

      // Create unique ptrs
      std::shared_ptr<range> range = std::make_shared<tile::range>(tile::range{
          std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
          std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
          Item_func::EQ_FUNC, tiledb_datatype_t::TILEDB_ANY, 0, 0});

      // Get field type for comparison
      Item_result cmp_type = args[i]->cmp_type();

      if (TileDBDateTimeType(datatype)) {
        cmp_type = TIME_RESULT;
      }

      int ret = set_range_from_item_consts(ha_thd(), lower_const, upper_const,
                                           cmp_type, range, datatype);

      if (ret)
        DBUG_RETURN(func_item);

      // If this is an attribute add it to the query condition
      if (use_query_condition) {
        // IN clauses are not supported
        // When they are uncomment below
        DBUG_RETURN(func_item);
        /*if (this->query_condition == nullptr) {
          this->query_condition = std::make_shared<tiledb::QueryCondition>(
              range->QueryCondition(ctx, column_field->field_name.str));
        } else {
          tiledb::QueryCondition qc = this->query_condition->combine(
              range->QueryCondition(ctx, column_field->field_name.str),
              TILEDB_AND);
          this->query_condition = std::make_shared<tiledb::QueryCondition>(qc);
        }*/
      } else {
        // Add the range to the pushdown in ranges
        auto &range_vec = this->pushdown_in_ranges[dim_idx];
        range_vec.push_back(std::move(range));
      }
    }

    break;
    // Handle equal case by setting upper and lower ranges to same value
  case Item_func::EQ_FUNC: {
    // Create unique ptrs
    std::shared_ptr<range> range = std::make_shared<tile::range>(tile::range{
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        func_item->functype(), tiledb_datatype_t::TILEDB_ANY, 0, 0});

    // Get field type for comparison
    Item_result cmp_type = args[1]->cmp_type();

    if (TileDBDateTimeType(datatype)) {
      cmp_type = TIME_RESULT;
    }

    int ret = set_range_from_item_consts(
        ha_thd(), dynamic_cast<Item_basic_constant *>(args[1]),
        dynamic_cast<Item_basic_constant *>(args[1]), cmp_type, range,
        datatype);

    if (ret)
      DBUG_RETURN(func_item);

    // If this is an attribute add it to the query condition
    if (use_query_condition) {
      qcPtr = std::make_shared<tiledb::QueryCondition>(
          range->QueryCondition(ctx, column_field->field_name.str));
    } else {
      // Add the range to the pushdown in ranges
      auto &range_vec = this->pushdown_ranges[dim_idx];
      range_vec.push_back(std::move(range));
    }

    break;
  }
  case Item_func::BETWEEN:
    neg = (dynamic_cast<const Item_func_opt_neg *>(func_item))->negated;
    if (neg) // don't support negations!
      DBUG_RETURN(func_item);
    // fall through
  case Item_func::LE_FUNC: // Handle all cases where there is 1 or 2 arguments
                           // we must set on
  case Item_func::LT_FUNC:
  case Item_func::GE_FUNC:
  case Item_func::GT_FUNC: {
    // the range
    Item_basic_constant *lower_const = nullptr;
    Item_basic_constant *upper_const = nullptr;

    // Get field type for comparison
    Item_result cmp_type = args[1]->cmp_type();

    if (TileDBDateTimeType(datatype)) {
      cmp_type = TIME_RESULT;
    }

    // If we have 3 items then we can set lower and upper
    if (func_item->argument_count() == 3) {
      lower_const = dynamic_cast<Item_basic_constant *>(args[1]);
      upper_const = dynamic_cast<Item_basic_constant *>(args[2]);
      // If the condition is less than we know its the upper limit we have
    } else if (func_item->functype() == Item_func::LT_FUNC ||
               func_item->functype() == Item_func::LE_FUNC) {
      upper_const = dynamic_cast<Item_basic_constant *>(args[1]);
      // If the condition is greater than we know its the lower limit we have
    } else if (func_item->functype() == Item_func::GT_FUNC ||
               func_item->functype() == Item_func::GE_FUNC) {
      lower_const = dynamic_cast<Item_basic_constant *>(args[1]);
    }

    // Create unique ptrs
    std::shared_ptr<range> range = std::make_shared<tile::range>(tile::range{
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        func_item->functype(), tiledb_datatype_t::TILEDB_ANY, 0, 0});

    int ret = set_range_from_item_consts(ha_thd(), lower_const, upper_const,
                                         cmp_type, range, datatype);

    if (ret)
      DBUG_RETURN(func_item);

    // If this is an attribute add it to the query condition
    if (use_query_condition) {
      qcPtr = std::make_shared<tiledb::QueryCondition>(
          range->QueryCondition(ctx, column_field->field_name.str));
    } else {
      // Add the range to the pushdown in ranges
      auto &range_vec = this->pushdown_ranges[dim_idx];
      range_vec.push_back(std::move(range));
    }

    break;
  }
  default:
    DBUG_RETURN(func_item);
  } // endswitch functype
  DBUG_RETURN(nullptr);
}

const COND *tile::mytile::cond_push(const COND *cond) {
  DBUG_ENTER("tile::mytile::cond_push");
  // NOTE: This is called one or more times by handle interface. Once for each
  // condition

  if (!tile::sysvars::enable_pushdown(ha_thd())) {
    DBUG_RETURN(cond);
  }

  DBUG_RETURN(cond_push_local(cond, this->query_condition));
}

const COND *
tile::mytile::cond_push_local(const COND *cond,
                              std::shared_ptr<tiledb::QueryCondition> &qcPtr) {
  DBUG_ENTER("tile::mytile::cond_push_local");
  // Make sure pushdown ranges is not empty
  if (this->pushdown_ranges.empty()) {
    this->pushdown_ranges.resize(this->ndim);
  }

  // Make sure pushdown in ranges is not empty
  if (this->pushdown_in_ranges.empty()) {
    this->pushdown_in_ranges.resize(this->ndim);
  }

  const COND *ret;
  switch (cond->type()) {
  case Item::COND_ITEM: {
    Item_cond *cond_item = dynamic_cast<Item_cond *>(const_cast<COND *>(cond));
    ret = cond_push_cond(cond_item);
    DBUG_RETURN(ret);
    break;
  }
  case Item::FUNC_ITEM: {
    const Item_func *func_item = dynamic_cast<const Item_func *>(cond);

    // We don't support these predicates yet:
    // SP_DISJOINT_FUNC b/c it's the inverse
    // SP_WITHIN_FUNC, SP_CONTAINS_FUNC, SP_CROSSES_FUNC b/c not symetrical
    // SP_TOUCHES b/c there's a small chance that we'll miss it with 0 padding
    // The rest should automatically benefit from the spatial index
    if (func_item->functype() == Item_func::SP_INTERSECTS_FUNC ||
        func_item->functype() == Item_func::SP_EQUALS_FUNC ||
        func_item->functype() == Item_func::SP_OVERLAPS_FUNC) {
      ret = cond_push_func_spatial(func_item, qcPtr);
      DBUG_RETURN(ret);
    }

    if (func_item->argument_count() > 1) {
      Item **args = func_item->arguments();

      Item_field *column_field = dynamic_cast<Item_field *>(args[0]);

      if (column_field == nullptr) {
        DBUG_RETURN(func_item);
      }

      tiledb_datatype_t datatype = tiledb_datatype_t::TILEDB_ANY;

      if (this->array_schema->has_attribute(column_field->name.str)) {
        auto attr = this->array_schema->attribute(column_field->name.str);
        datatype = attr.type();
      } else {
        auto dim =
            this->array_schema->domain().dimension(column_field->name.str);
        datatype = dim.type();
      }

      if (datatype == TILEDB_DATETIME_AS || datatype == TILEDB_DATETIME_FS ||
          datatype == TILEDB_DATETIME_PS || datatype == TILEDB_DATETIME_NS ||
          datatype == TILEDB_DATETIME_US || datatype == TILEDB_DATETIME_MS ||
          datatype == TILEDB_DATETIME_SEC || datatype == TILEDB_DATETIME_MIN ||
          datatype == TILEDB_DATETIME_HR || datatype == TILEDB_DATETIME_DAY ||
          datatype == TILEDB_DATETIME_WEEK ||
          datatype == TILEDB_DATETIME_MONTH ||
          datatype == TILEDB_DATETIME_YEAR || datatype == TILEDB_TIME_AS ||
          datatype == TILEDB_TIME_FS || datatype == TILEDB_TIME_PS ||
          datatype == TILEDB_TIME_NS || datatype == TILEDB_TIME_US ||
          datatype == TILEDB_TIME_MS || datatype == TILEDB_TIME_SEC ||
          datatype == TILEDB_TIME_MIN || datatype == TILEDB_TIME_HR) {
        ret = cond_push_func_datetime(func_item, qcPtr);
        DBUG_RETURN(ret);
      }
    }

    ret = cond_push_func(func_item, qcPtr);
    DBUG_RETURN(ret);
    break;
  }
  // not supported currently
  // Field items are when two have two fields of a table, i.e. a join.
  case Item::FIELD_ITEM:
    // fall through
  default: {
    DBUG_RETURN(cond);
  }
  }
  DBUG_RETURN(cond);
}

void tile::mytile::cond_pop() {
  DBUG_ENTER("tile::mytile::cond_pop");

  DBUG_VOID_RETURN;
}

Item *tile::mytile::idx_cond_push(uint keyno, Item *idx_cond) {
  DBUG_ENTER("tile::mytile::idx_cond_push");
  std::shared_ptr<tiledb::QueryCondition> null;
  auto ret = cond_push_local(static_cast<Item_cond *>(idx_cond), null);
  DBUG_RETURN(const_cast<Item *>(ret));
}

void tile::mytile::drop_table(const char *name) {
  DBUG_ENTER("tile::mytile::drop_table");
  delete_table(name);
  DBUG_VOID_RETURN;
}

int tile::mytile::delete_table(const char *name) {
  DBUG_ENTER("tile::mytile::delete_table");
  if (!tile::sysvars::delete_arrays(ha_thd())) {
    DBUG_RETURN(0);
  }

  try {
    tiledb::VFS vfs(this->ctx);
    TABLE_SHARE *s;
    if (this->table != nullptr)
      s = this->table->s;
    else
      s = this->table_share;
    if (s != nullptr && s->option_struct != nullptr &&
        s->option_struct->array_uri != nullptr) {
      vfs.remove_dir(s->option_struct->array_uri);
    } else {
      vfs.remove_dir(name);
    }
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    sql_print_error("delete_table error for table %s : %s", name, e.what());
    DBUG_RETURN(ERR_DELETE_TABLE_TILEDB);
  } catch (const std::exception &e) {
    // Log errors
    sql_print_error("delete_table error for table %s : %s", name, e.what());
    DBUG_RETURN(ERR_DELETE_TABLE_OTHER);
  }
  DBUG_RETURN(0);
}

int tile::mytile::info(uint) {
  DBUG_ENTER("tile::mytile::info");
  // Need records to be greater than 1 to avoid 0/1 row optimizations by query
  // optimizer
  stats.records = this->records_upper_bound;
  DBUG_RETURN(0);
};

ulonglong tile::mytile::table_flags(void) const {
  DBUG_ENTER("tile::mytile::table_flags");
  DBUG_RETURN(
      HA_PARTIAL_COLUMN_READ | HA_REC_NOT_IN_SEQ | HA_CAN_SQL_HANDLER |
      //               HA_REQUIRE_PRIMARY_KEY | HA_PRIMARY_KEY_IN_READ_INDEX |
      HA_FAST_KEY_READ | HA_SLOW_RND_POS | HA_CAN_TABLE_CONDITION_PUSHDOWN |
      HA_CAN_EXPORT | HA_CONCURRENT_OPTIMIZE | HA_CAN_ONLINE_BACKUPS |
      HA_CAN_BIT_FIELD | HA_FILE_BASED);
}

std::vector<std::tuple<tiledb_datatype_t, bool, bool, bool>>
tile::mytile::build_field_details_for_buffers(const tiledb::ArraySchema &schema,
                                              Field **fields,
                                              const uint64_t &field_num,
                                              MY_BITMAP *read_set) {
  DBUG_ENTER("tile::mytile::build_field_details_for_buffers");
  std::vector<std::tuple<tiledb_datatype_t, bool, bool, bool>> ret;
  for (size_t fieldIndex = 0; fieldIndex < field_num; fieldIndex++) {
    Field *field = fields[fieldIndex];
    std::string field_name = field->field_name.str;
    // Only set buffers for fields that are asked for except always set
    // dimension We check the read_set because the read_set is set to ALL column
    // for writes and set to the subset of columns for reads
    if (!bitmap_is_set(read_set, fieldIndex) &&
        !schema.domain().has_dimension(field_name)) {
      continue;
    }

    tiledb_datatype_t datatype;
    bool var_len = false;
    bool nullable = false;
    bool list = false;

    if (schema.domain().has_dimension(field_name)) {
      auto dim = schema.domain().dimension(field_name);
      datatype = dim.type();
      var_len = dim.cell_val_num() == TILEDB_VAR_NUM;
      // Dimension can't be nullable
      nullable = false;
    } else { // attribute
      tiledb::Attribute attr = schema.attribute(field_name);
      datatype = attr.type();
      var_len = attr.cell_val_num();
      nullable = attr.nullable();
      list = attr.cell_val_num() > 1 && attr.cell_val_num() != TILEDB_VAR_NUM;
    }

    ret.emplace_back(std::make_tuple(datatype, var_len, nullable, list));
  }

  DBUG_RETURN(ret);
}

void tile::mytile::alloc_buffers(uint64_t memory_budget) {
  DBUG_ENTER("tile::mytile::alloc_buffers");
  // Set Attribute Buffers
  auto domain = this->array_schema->domain();
  auto dims = domain.dimensions();

  if (this->buffers.empty()) {
    for (size_t i = 0; i < table->s->fields; i++)
      this->buffers.emplace_back();
  }

  std::vector<std::tuple<tiledb_datatype_t, bool, bool, bool>> field_details =
      build_field_details_for_buffers(*this->array_schema, table->field,
                                      table->s->fields, table->read_set);

  tile::BufferSizeByType bufferSizesByType =
      tile::compute_buffer_sizes(field_details, memory_budget);

  for (size_t fieldIndex = 0; fieldIndex < table->s->fields; fieldIndex++) {
    Field *field = table->field[fieldIndex];
    std::string field_name = field->field_name.str;
    // Only set buffers for fields that are asked for except always set
    // dimensions. We check the read_set because the read_set is set to ALL
    // column for writes and set to the subset of columns for reads
    if (!bitmap_is_set(this->table->read_set, fieldIndex) &&
        !this->array_schema->domain().has_dimension(field_name)) {
      continue;
    }

    // Create buffer
    std::shared_ptr<buffer> buff = std::make_shared<buffer>();
    tiledb_datatype_t datatype;
    uint64_t data_size = 0;
    buff->name = field_name;
    buff->dimension = false;
    buff->buffer_offset = 0;
    buff->fixed_size_elements = 1;

    if (this->array_schema->domain().has_dimension(field_name)) {
      auto dim = this->array_schema->domain().dimension(field_name);
      datatype = dim.type();
      buff->dimension = true;
      data_size = bufferSizesByType.SizeByType(datatype);

      if (dim.cell_val_num() == TILEDB_VAR_NUM) {
        uint64_t *offset_buffer = static_cast<uint64_t *>(
            alloc_buffer(tiledb_datatype_t::TILEDB_UINT64,
                         bufferSizesByType.uint64_buffer_size));
        buff->offset_buffer_size = bufferSizesByType.uint64_buffer_size;
        buff->allocated_offset_buffer_size =
            bufferSizesByType.uint64_buffer_size;
        buff->offset_buffer = offset_buffer;

        // Override buffer size as this is var length
        data_size = bufferSizesByType.var_length_uint8_buffer_size;
      }
    } else { // attribute
      tiledb::Attribute attr = this->array_schema->attribute(field_name);

      uint8_t *validity_buffer = nullptr;
      uint64_t *offset_buffer = nullptr;

      buff->fixed_size_elements = attr.cell_val_num();
      datatype = attr.type();
      data_size = bufferSizesByType.SizeByType(datatype);

      if (attr.nullable()) {
        validity_buffer = static_cast<uint8_t *>(
            alloc_buffer(tiledb_datatype_t::TILEDB_UINT8,
                         bufferSizesByType.var_length_uint8_buffer_size));
        buff->validity_buffer_size =
            bufferSizesByType.var_length_uint8_buffer_size;
        buff->allocated_validity_buffer_size =
            bufferSizesByType.var_length_uint8_buffer_size;
      }
      if (attr.variable_sized()) {
        offset_buffer = static_cast<uint64_t *>(
            alloc_buffer(tiledb_datatype_t::TILEDB_UINT64,
                         bufferSizesByType.uint64_buffer_size));
        buff->offset_buffer_size = bufferSizesByType.uint64_buffer_size;
        buff->allocated_offset_buffer_size =
            bufferSizesByType.uint64_buffer_size;

        // Override buffer size as this is var length
        data_size = bufferSizesByType.var_length_uint8_buffer_size;
      }

      buff->validity_buffer = validity_buffer;
      buff->offset_buffer = offset_buffer;
      buff->type = attr.type();
    }
    buff->buffer = alloc_buffer(datatype, data_size);
    buff->type = datatype;
    buff->buffer_size = data_size;
    buff->allocated_buffer_size = data_size;
    this->buffers[fieldIndex] = buff;
  }
  DBUG_VOID_RETURN;
}

void tile::mytile::alloc_read_buffers(uint64_t memory_budget) {
  alloc_buffers(memory_budget);
  auto domain = this->array_schema->domain();

  for (auto &buff : this->buffers) {
    // Only set buffers which are non-null
    if (buff == nullptr)
      continue;

    this->ctx.handle_error(tiledb_query_set_data_buffer(
        this->ctx.ptr().get(), this->query->ptr().get(), buff->name.c_str(),
        buff->buffer, &buff->buffer_size));

    if (buff->validity_buffer != nullptr) {
      this->ctx.handle_error(tiledb_query_set_validity_buffer(
          this->ctx.ptr().get(), this->query->ptr().get(), buff->name.c_str(),
          buff->validity_buffer, &buff->validity_buffer_size));
    }

    if (buff->offset_buffer != nullptr) {
      this->ctx.handle_error(tiledb_query_set_offsets_buffer(
          this->ctx.ptr().get(), this->query->ptr().get(), buff->name.c_str(),
          buff->offset_buffer, &buff->offset_buffer_size));
    }
  }
}

int tile::mytile::tileToFields(uint64_t orignal_index, bool dimensions_only,
                               TABLE *table) {
  DBUG_ENTER("tile::mytile::tileToFields");
  int rc = 0;
  try {
    for (size_t fieldIndex = 0; fieldIndex < table->s->fields; fieldIndex++) {
      Field *field = table->field[fieldIndex];
      std::shared_ptr<buffer> buff = this->buffers[fieldIndex];
      // Only read fields that are asked for and the buffer was originally set
      if (!bitmap_is_set(this->table->read_set, fieldIndex) ||
          buff == nullptr) {
        continue;
      }
      uint64_t index = orignal_index;

      if (dimensions_only) {
        continue;
      }

      set_field(ha_thd(), field, buff, index);
    }
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[tileToFields] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = ERR_TILE_TO_FIELDS_TILEDB;
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[tileToFields] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = ERR_TILE_TO_FIELDS_OTHER;
  }

  DBUG_RETURN(rc);
};

int tile::mytile::mysql_row_to_tiledb_buffers(const uchar *buf) {
  DBUG_ENTER("tile::mytile::mysql_row_to_tiledb_buffers");
  int error = 0;

  try {
    for (size_t fieldIndex = 0; fieldIndex < table->s->fields; fieldIndex++) {
      Field *field = table->field[fieldIndex];

      if (field->is_null() && field->option_struct->dimension) {
        sql_print_error(
            "[mysql_row_to_tiledb_buffers] write error for table %s : %s",
            this->uri.c_str(), "dimension null not supported");
        error = ERR_ROW_TO_TILEDB_DIM_NULL;
        DBUG_RETURN(error);
      }

      std::shared_ptr<buffer> buffer = this->buffers[fieldIndex];
      error = set_buffer_from_field(field, buffer, this->record_index, ha_thd(),
                                    true /* check null */);
    }
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    sql_print_error(
        "[mysql_row_to_tiledb_buffers] write error for table %s : %s",
        this->uri.c_str(), e.what());
    error = ERR_ROW_TO_TILEDB_TILEDB;
  } catch (const std::exception &e) {
    // Log errors
    sql_print_error(
        "[mysql_row_to_tiledb_buffers] write error for table %s : %s",
        this->uri.c_str(), e.what());
    error = ERR_ROW_TO_TILEDB_OTHER;
  }

  DBUG_RETURN(error);
}

void tile::mytile::setup_write() {
  DBUG_ENTER("tile::mytile::setup_write");

  // Make sure array is open for writes
  open_array_for_writes(ha_thd());

  // We must set the bitmap for debug purpose, it is "read_set" because we use
  // Field->val_*
  MY_BITMAP *original_bitmap = tmp_use_all_columns(table, &table->read_set);
  this->write_buffer_size = tile::sysvars::write_buffer_size(this->ha_thd());
  alloc_buffers(this->write_buffer_size);
  this->record_index = 0;
  // Reset buffer sizes to 0 for writes
  // We increase the size for every cell/row we are given to write
  for (auto &buff : this->buffers) {
    buff->buffer_size = 0;
    buff->offset_buffer_size = 0;
    buff->validity_buffer_size = 0;
  }
  // Reset bitmap to original
  tmp_restore_column_map(&table->read_set, original_bitmap);
  DBUG_VOID_RETURN;
}

/**
 * Helper to end and finalize writes
 */
int tile::mytile::finalize_write() {
  DBUG_ENTER("tile::mytile::finalize_write");
  int rc = 0;
  // Set all buffers with proper size
  try {
    // Submit query
    if (this->query != nullptr) {
      rc = flush_write();
      if (rc)
        DBUG_RETURN(rc);

      // If the layout is GLOBAL_ORDER we need to call finalize
      if (query->query_layout() == tiledb_layout_t::TILEDB_GLOBAL_ORDER) {
        this->query->finalize();
      }
      this->query = nullptr;

      // Clear all allocated buffers
      dealloc_buffers();
    }
    rc = close();

  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR,
                    "[finalize_write] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = ERR_FINALIZE_WRITE_TILEDB;
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR,
                    "[finalize_write] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = ERR_FINALIZE_WRITE_OTHER;
  }
  DBUG_RETURN(rc);
}

void tile::mytile::start_bulk_insert(ha_rows rows, uint flags) {
  DBUG_ENTER("tile::mytile::start_bulk_insert");
  this->bulk_write = true;
  setup_write();
  DBUG_VOID_RETURN;
}

int tile::mytile::end_bulk_insert() {
  DBUG_ENTER("tile::mytile::end_bulk_insert");
  this->bulk_write = false;
  DBUG_RETURN(finalize_write());
}

int tile::mytile::flush_write() {
  DBUG_ENTER("tile::mytile::flush_write");

  int rc = 0;
  if (this->query == nullptr)
    DBUG_RETURN(rc);
  // Set all buffers with proper size
  try {

    for (auto &buff : this->buffers) {

      if (buff == nullptr)
        continue;

      if (buff->dimension && this->array_schema->array_type() ==
                                 tiledb_array_type_t::TILEDB_DENSE) {
        // In case of a dense write we need to create a subarray
        // see
        // https://docs.tiledb.com/main/background/internal-mechanics/writing#dense-writes

        // Get the first and last elements of the dim buffer
        tiledb_datatype_t type = buff->type;
        uint64_t type_size = tiledb_datatype_size(type);
        int num_elements = buff->buffer_size / type_size;

        // The user is responsible for providing the dim values in the correct
        // order for each dimension. See docs link above.
        void *first_element = buff->buffer;
        void *last_element =
            (char *)buff->buffer + (num_elements - 1) * type_size;

        // Use the c-api because it uses void*
        this->ctx.handle_error(tiledb_subarray_add_range_by_name(
            this->ctx.ptr().get(), this->subarray->ptr().get(),
            buff->name.c_str(), first_element, last_element, nullptr));

        // set the subarray to the query
        this->query->set_subarray(*this->subarray);

        // continue without setting any buffers
        continue;
      }

      this->ctx.handle_error(tiledb_query_set_data_buffer(
          this->ctx.ptr().get(), this->query->ptr().get(), buff->name.c_str(),
          buff->buffer, &buff->buffer_size));

      if (buff->validity_buffer != nullptr) {
        this->ctx.handle_error(tiledb_query_set_validity_buffer(
            this->ctx.ptr().get(), this->query->ptr().get(), buff->name.c_str(),
            buff->validity_buffer, &buff->validity_buffer_size));
      }

      if (buff->offset_buffer != nullptr) {
        this->ctx.handle_error(tiledb_query_set_offsets_buffer(
            this->ctx.ptr().get(), this->query->ptr().get(), buff->name.c_str(),
            buff->offset_buffer, &buff->offset_buffer_size));
      }
    }

    // Only submit the query if there is actual data, else just carry on
    if (this->buffers[0]->buffer_size > 0) {
      query->submit();
    }

    // After query submit reset buffer sizes
    this->record_index = 0;
    // Reset buffer sizes to 0 for writes
    // We increase the size for every cell/row we are given to write
    for (auto &buff : this->buffers) {
      buff->buffer_size = 0;
      buff->offset_buffer_size = 0;
      buff->validity_buffer_size = 0;
    }
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[flush_write] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = ERR_FLUSH_WRITE_TILEDB;
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[flush_write] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = ERR_FLUSH_WRITE_OTHER;
  }

  DBUG_RETURN(rc);
}

int tile::mytile::write_row(const uchar *buf) {
  DBUG_ENTER("tile::mytile::write_row");
  int rc = 0;
  // We must set the bitmap for debug purpose, it is "read_set" because we use
  // Field->val_*
  MY_BITMAP *original_bitmap = tmp_use_all_columns(table, &table->read_set);

  if (!this->bulk_write) {
    setup_write();
  }

  try {
    rc = mysql_row_to_tiledb_buffers(buf);
    if (rc == ERR_WRITE_FLUSH_NEEDED) {
      rc = flush_write();
      // Reset bitmap to original
      tmp_restore_column_map(&table->read_set, original_bitmap);
      if (rc)
        DBUG_RETURN(rc);

      DBUG_RETURN(write_row(buf));
    }
    if (rc) {
      // Reset bitmap to original
      tmp_restore_column_map(&table->read_set, original_bitmap);
      DBUG_RETURN(rc);
    }
    this->record_index++;
    auto domain = this->array_schema->domain();

    if (!this->bulk_write) {
      rc = finalize_write();
    }
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[write_row] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = ERR_WRITE_ROW_TILEDB;
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[write_row] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = ERR_WRITE_ROW_OTHER;
  }

  // Reset bitmap to original
  tmp_restore_column_map(&table->read_set, original_bitmap);
  DBUG_RETURN(rc);
}

ulong tile::mytile::index_flags(uint idx, uint part, bool all_parts) const {
  DBUG_ENTER("tile::mytile::index_flags");
  DBUG_RETURN(HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER | HA_READ_RANGE |
              HA_KEYREAD_ONLY | HA_DO_RANGE_FILTER_PUSHDOWN |
#if MYSQL_VERSION_ID < 100500
              HA_DO_INDEX_COND_PUSHDOWN);
#else
              HA_DO_INDEX_COND_PUSHDOWN | HA_CLUSTERED_INDEX);
#endif
}

void tile::mytile::open_array_for_reads(THD *thd) {
  bool reopen_for_every_query = tile::sysvars::reopen_for_every_query(thd);
  std::string encryption_key;
  if (this->table->s->option_struct->encryption_key != nullptr) {
    encryption_key = std::string(this->table->s->option_struct->encryption_key);
  }

  // If we want to reopen for every query then we'll build a new context and do
  // it
  if (reopen_for_every_query || this->array == nullptr) {
    // First rebuild context with new config if needed
    tiledb::Config cfg = build_config(ha_thd());

    if (cfg != this->config) {
      this->config = cfg;
      this->ctx = build_context(this->config);
    }
    if (this->table->s->option_struct->open_at != UINT64_MAX) {
#if TILEDB_VERSION_MAJOR >= 2 && TILEDB_VERSION_MINOR >= 15
      this->array = std::make_shared<tiledb::Array>(
          this->ctx, this->uri, TILEDB_READ,
          tiledb::TemporalPolicy(tiledb::TimeTravel,
                                 this->table->s->option_struct->open_at),
          tiledb::EncryptionAlgorithm(
              encryption_key.empty() ? TILEDB_NO_ENCRYPTION
                                     : TILEDB_AES_256_GCM,
              this->table->s->option_struct->encryption_key));
#else
      this->array = std::make_shared<tiledb::Array>(
          this->ctx, this->uri, TILEDB_READ,
          encryption_key.empty() ? TILEDB_NO_ENCRYPTION : TILEDB_AES_256_GCM,
          encryption_key, this->table->s->option_struct->open_at);
#endif

    } else {

#if TILEDB_VERSION_MAJOR >= 2 && TILEDB_VERSION_MINOR >= 15
      this->array = std::make_shared<tiledb::Array>(
          this->ctx, this->uri, TILEDB_READ, tiledb::TemporalPolicy(),
          tiledb::EncryptionAlgorithm(
              encryption_key.empty() ? TILEDB_NO_ENCRYPTION
                                     : TILEDB_AES_256_GCM,
              this->table->s->option_struct->encryption_key));
#else
      this->array = std::make_shared<tiledb::Array>(
          this->ctx, this->uri, TILEDB_READ,
          encryption_key.empty() ? TILEDB_NO_ENCRYPTION : TILEDB_AES_256_GCM,
          encryption_key);
#endif
    }
    this->query =
        std::make_unique<tiledb::Query>(this->ctx, *this->array, TILEDB_READ);
    // Else lets try to open reopen and use existing contexts
  } else {
    if ((this->array->is_open() && this->array->query_type() != TILEDB_READ) ||
        !this->array->is_open()) {
      if (this->array->is_open())
        this->array->close();

      if (this->table->s->option_struct->open_at != UINT64_MAX) {
        this->array->open(
            TILEDB_READ,
            encryption_key.empty() ? TILEDB_NO_ENCRYPTION : TILEDB_AES_256_GCM,
            encryption_key, this->table->s->option_struct->open_at);
      } else {
        this->array->open(TILEDB_READ,
                          encryption_key.empty() ? TILEDB_NO_ENCRYPTION
                                                 : TILEDB_AES_256_GCM,
                          encryption_key);
      }
    }

    if (this->query == nullptr || this->query->query_type() != TILEDB_READ) {
      this->query =
          std::make_unique<tiledb::Query>(this->ctx, *this->array, TILEDB_READ);
    }
  }

  // Fetch user set read layout
  tiledb_layout_t query_layout = tile::sysvars::read_query_layout(thd);

  // Set layout
  this->query->set_layout(query_layout);
  // If the layout is unordered but the array type is dense we will set it to
  // the tile order instead. Currently, multi-range queries do not support dense
  // unordered reads
  if (this->array_schema->array_type() == tiledb_array_type_t::TILEDB_DENSE &&
      query_layout == tiledb_layout_t::TILEDB_UNORDERED) {
    this->query->set_layout(this->array_schema->tile_order());
  }

  // if this is a join then MariaDB expects the results to be sorted in
  // row-major
  if (thd->lex->current_select->join != nullptr &&
      thd->lex->current_select->join->table_count > 1) {
    this->query->set_layout(tiledb_layout_t::TILEDB_ROW_MAJOR);
  }
}

void tile::mytile::open_array_for_writes(THD *thd) {
  bool reopen_for_every_query = tile::sysvars::reopen_for_every_query(thd);
  std::string encryption_key;
  if (this->table->s->option_struct->encryption_key != nullptr) {
    encryption_key = std::string(this->table->s->option_struct->encryption_key);
  }

  // If we want to reopen for every query then we'll build a new context and do
  // it
  if (reopen_for_every_query || this->array == nullptr) {
    // First rebuild context with new config if needed
    tiledb::Config cfg = build_config(ha_thd());

    if (cfg != this->config) {
      this->config = cfg;
      this->ctx = build_context(this->config);
    }

#if TILEDB_VERSION_MAJOR >= 2 && TILEDB_VERSION_MINOR >= 15
    this->array = std::make_shared<tiledb::Array>(
        this->ctx, this->uri, TILEDB_WRITE, tiledb::TemporalPolicy(),
        tiledb::EncryptionAlgorithm(
            encryption_key.empty() ? TILEDB_NO_ENCRYPTION : TILEDB_AES_256_GCM,
            this->table->s->option_struct->encryption_key));
#else
    this->array = std::make_shared<tiledb::Array>(
        this->ctx, this->uri, TILEDB_WRITE,
        encryption_key.empty() ? TILEDB_NO_ENCRYPTION : TILEDB_AES_256_GCM,
        encryption_key);
#endif
    this->query =
        std::make_unique<tiledb::Query>(this->ctx, *this->array, TILEDB_WRITE);
    // Else lets try to open reopen and use existing contexts
  } else {

    if ((this->array->is_open() && this->array->query_type() != TILEDB_WRITE) ||
        !this->array->is_open()) {
      if (this->array->is_open())
        this->array->close();

      this->array->open(TILEDB_WRITE,
                        encryption_key.empty() ? TILEDB_NO_ENCRYPTION
                                               : TILEDB_AES_256_GCM,
                        encryption_key);
    }
    if (this->query == nullptr || this->query->query_type() != TILEDB_WRITE) {
      this->query = std::make_unique<tiledb::Query>(this->ctx, *this->array,
                                                    TILEDB_WRITE);
    }
  }

  if (this->array_schema->array_type() == tiledb_array_type_t::TILEDB_SPARSE) {
    this->query->set_layout(tiledb_layout_t::TILEDB_UNORDERED);
  } else {
    this->query->set_layout(this->array_schema->cell_order());
    // dense writes need a subarray
    this->subarray = std::unique_ptr<tiledb::Subarray>(
        new tiledb::Subarray(this->ctx, *this->array));
  };
}

bool tile::mytile::valid_pushed_ranges() {
  if (this->pushdown_ranges.empty())
    return false;

  // Check all dimensions and makes sure that atleast one is non empty and non
  // null
  bool one_valid_range = false;
  for (const auto &range : this->pushdown_ranges) {
    if (!range.empty()) {
      const auto &range_ptr = range[0];
      if (range_ptr != nullptr && (range_ptr->lower_value != nullptr ||
                                   range_ptr->upper_value != nullptr)) {
        return true;
      }
    }
  }

  return one_valid_range;
}

bool tile::mytile::valid_pushed_in_ranges() {
  if (this->pushdown_in_ranges.empty())
    return false;

  // Check all dimensions and makes sure that atleast one is non empty and non
  // null
  bool one_valid_range = false;
  for (const auto &range : this->pushdown_in_ranges) {
    if (!range.empty()) {
      const auto &range_ptr = range[0];
      if (range_ptr != nullptr && (range_ptr->lower_value != nullptr ||
                                   range_ptr->upper_value != nullptr)) {
        return true;
      }
    }
  }

  return one_valid_range;
}

int tile::mytile::index_init(uint idx, bool sorted) {
  DBUG_ENTER("tile::mytile::index_init");
  // If we are doing an index scan we need to use row-major order to get the
  // results in the expected order
  int rc = init_scan(this->ha_thd());

  if (rc)
    DBUG_RETURN(rc);

  this->query->set_layout(tiledb_layout_t::TILEDB_ROW_MAJOR);

  DBUG_RETURN(0);
}

int tile::mytile::index_end() {
  DBUG_ENTER("tile::mytile::index_end");
  DBUG_RETURN(rnd_end());
}

int tile::mytile::index_read(uchar *buf, const uchar *key, uint key_len,
                             enum ha_rkey_function find_flag) {
  DBUG_ENTER("tile::mytile::index_read");
  // reset or add pushdowns for this key if not MRR
  if (!this->mrr_query) {
    this->set_pushdowns_for_key(key, key_len, true /* start_key */, find_flag);
    int rc = init_scan(this->ha_thd());

    if (rc)
      DBUG_RETURN(rc);

    // Index scans are expected in row major order
    this->query->set_layout(tiledb_layout_t::TILEDB_ROW_MAJOR);
  }

  DBUG_RETURN(index_read_scan(key, key_len, find_flag, false /* reset */));
}

int tile::mytile::index_first(uchar *buf) {
  DBUG_ENTER("tile::mytile::index_first");
  // Treat just as normal row read
  DBUG_RETURN(scan_rnd_row(table));
}

int tile::mytile::index_next(uchar *buf) {
  DBUG_ENTER("tile::mytile::index_next");
  // Treat just as normal row read
  DBUG_RETURN(scan_rnd_row(table));
}

int8_t tile::mytile::compare_key_to_dims(const uchar *key, uint key_len,
                                         uint64_t index) {
  int key_position = 0;

  const KEY *key_info = table->key_info;

  for (uint64_t key_part_index = 0;
       key_part_index < key_info->user_defined_key_parts; key_part_index++) {

    const KEY_PART_INFO *key_part_info = &(key_info->key_part[key_part_index]);

    for (auto &dim_buffer : this->buffers) {
      if (dim_buffer == nullptr ||
          dim_buffer->name != this->dimensionNames[key_part_index]) {
        continue;
      } else { // buffer for dimension was found
        uint8_t dim_comparison =
            compare_key_to_dim(key_part_index, key + key_position,
                               key_part_info->length, index, dim_buffer);
        key_position += key_part_info->length;
        if (dim_comparison != 0) {
          return dim_comparison;
        }
      }
      break; // skipping rest of buffers, proceed with next dimension
    }

    if (key_position >= (int)key_len) {
      break;
    }
  }
  return 0;
}

int8_t tile::mytile::compare_key_to_dim(const uint64_t dim_idx,
                                        const uchar *key, uint64_t key_part_len,
                                        const uint64_t index,
                                        const std::shared_ptr<buffer> &buf) {

  uint64_t datatype_size = tiledb_datatype_size(buf->type);

  void *fixed_buff_pointer =
      (static_cast<char *>(buf->buffer) + index * datatype_size);

  switch (buf->type) {

  case TILEDB_FLOAT32:
    return compare_key_to_dim<float>(dim_idx, key, key_part_len,
                                     fixed_buff_pointer);
  case TILEDB_FLOAT64:
    return compare_key_to_dim<double>(dim_idx, key, key_part_len,
                                      fixed_buff_pointer);
    //  case TILEDB_CHAR:
  case TILEDB_INT8:
    return compare_key_to_dim<int8>(dim_idx, key, key_part_len,
                                    fixed_buff_pointer);
  case TILEDB_UINT8:
    return compare_key_to_dim<uint8>(dim_idx, key, key_part_len,
                                     fixed_buff_pointer);
  case TILEDB_INT16:
    return compare_key_to_dim<int16>(dim_idx, key, key_part_len,
                                     fixed_buff_pointer);
  case TILEDB_UINT16:
    return compare_key_to_dim<uint16>(dim_idx, key, key_part_len,
                                      fixed_buff_pointer);
  case TILEDB_INT32:
    return compare_key_to_dim<int32>(dim_idx, key, key_part_len,
                                     fixed_buff_pointer);
  case TILEDB_UINT32:
    return compare_key_to_dim<uint32>(dim_idx, key, key_part_len,
                                      fixed_buff_pointer);
  case TILEDB_UINT64:
    return compare_key_to_dim<uint64>(dim_idx, key, key_part_len,
                                      fixed_buff_pointer);
  case TILEDB_INT64:
    return compare_key_to_dim<int64>(dim_idx, key, key_part_len,
                                     fixed_buff_pointer);
  case TILEDB_DATETIME_YEAR: {
    // XXX: for some reason maria uses year offset from 1900 here
    MYSQL_TIME mysql_time = {1900U + *((uint8_t *)(key)), 0, 0, 0, 0, 0, 0, 0,
                             MYSQL_TIMESTAMP_TIME};
    int64_t xs = MysqlTimeToTileDBTimeVal(ha_thd(), mysql_time, buf->type);
    return compare_key_to_dim<int64>(dim_idx, (uchar *)&xs, key_part_len,
                                     fixed_buff_pointer);
  }
  case TILEDB_DATETIME_MONTH:
  case TILEDB_DATETIME_WEEK:
  case TILEDB_DATETIME_DAY:
  case TILEDB_DATETIME_HR:
  case TILEDB_DATETIME_MIN:
  case TILEDB_DATETIME_SEC:
  case TILEDB_DATETIME_MS:
  case TILEDB_DATETIME_US:
  case TILEDB_DATETIME_NS:
  case TILEDB_DATETIME_PS:
  case TILEDB_DATETIME_FS:
  case TILEDB_DATETIME_AS:
  case TILEDB_TIME_HR:
  case TILEDB_TIME_MIN:
  case TILEDB_TIME_SEC:
  case TILEDB_TIME_MS:
  case TILEDB_TIME_US:
  case TILEDB_TIME_NS:
  case TILEDB_TIME_PS:
  case TILEDB_TIME_FS:
  case TILEDB_TIME_AS: {
    MYSQL_TIME mysql_time;
    Field *field = table->field[dim_idx];

    uchar *tmp = field->ptr;
    field->ptr = (uchar *)key;
    field->get_date(&mysql_time, date_mode_t(0));
    field->ptr = tmp;

    int64_t xs = MysqlTimeToTileDBTimeVal(ha_thd(), mysql_time, buf->type);
    return compare_key_to_dim<int64>(dim_idx, (uchar *)&xs, key_part_len,
                                     fixed_buff_pointer);
  }
  case TILEDB_STRING_ASCII: {
    const uint16_t char_length = *reinterpret_cast<const uint16_t *>(key);
    uint64_t key_offset = sizeof(uint16_t);

    uint64_t end_position = index + 1;
    uint64_t start_position = 0;
    // If its not the first value, we need to see where the previous position
    // ended to know where to start.
    if (index > 0) {
      start_position = buf->offset_buffer[index];
    }
    // If the current position is equal to the number of results - 1 then we are
    // at the last varchar value
    if (index >= (buf->offset_buffer_size / sizeof(uint64_t)) - 1) {
      end_position = buf->buffer_size / sizeof(char);
    } else { // Else read the end from the next offset.
      end_position = buf->offset_buffer[index + 1];
    }
    size_t size = end_position - start_position;

    // If the key size is zero, this can happen when we are doing partial key
    // matches For strings a size of 0 means anything should be considered a
    // match so we return 0
    if (char_length == 0)
      return 0;

    void *buff = (static_cast<char *>(buf->buffer) + start_position);

    return compare_key_to_dim<char>(dim_idx, key + key_offset, char_length,
                                    buff, size);
  }
  case TILEDB_BLOB:
  case TILEDB_GEOM_WKB:
  case TILEDB_GEOM_WKT:
    return compare_key_to_dim<std::byte>(dim_idx, key, key_part_len,
                                         fixed_buff_pointer);
  case TILEDB_BOOL:
    return compare_key_to_dim<bool>(dim_idx, key, key_part_len,
                                    fixed_buff_pointer);
  default: {
    my_printf_error(ER_UNKNOWN_ERROR, "Unsupported datatype in key compare",
                    ME_ERROR_LOG | ME_FATAL);
  }
  }

  return 0;
}

int tile::mytile::index_read_scan(const uchar *key, uint key_len,
                                  enum ha_rkey_function find_flag, bool reset) {
  DBUG_ENTER("tile::mytile::index_read_scan");

  int rc = 0;
  const char *query_status;

  // If the query is empty, we should abort early
  // We have to check this here and not in rnd_init because
  // only index_read_scan can return empty
  if (this->empty_read) {
    if (reset)
      index_end();
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  // We must set the bitmap for debug purpose, it is "write_set" because we use
  // Field->store
  MY_BITMAP *original_bitmap =
      dbug_tmp_use_all_columns(table, &table->write_set);
  tiledb_query_status_to_str(static_cast<tiledb_query_status_t>(status),
                             &query_status);

  bool restarted_scan = false;
begin:
  if (!this->mrr_query && this->records == this->records_examined &&
      status == tiledb::Query::Status::COMPLETE) {
    // Reset bitmap to original
    dbug_tmp_restore_column_map(&table->write_set, original_bitmap);
    if (reset)
      index_end();
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  try {
    // If the cursor has examined all records
    // (or if this is the first time), (re)submit the query
    if (this->records_examined >= this->records) {

      do {
        this->status = query->submit();

        // Compute the number of cells (records) that were returned by the query
        auto buff = this->buffers[0];
        if (buff->offset_buffer != nullptr) {
          this->records = buff->offset_buffer_size / sizeof(uint64_t);
        } else {
          this->records = buff->buffer_size / tiledb_datatype_size(buff->type);
        }

        // Increase the buffer allocation and resubmit if necessary.
        if (this->status == tiledb::Query::Status::INCOMPLETE &&
            this->records == 0) { // VERY IMPORTANT!!
          this->read_buffer_size = this->read_buffer_size * 2;
          dealloc_buffers();
          alloc_read_buffers(read_buffer_size);
        } else if (records > 0) {
          this->record_index = 0;
          this->records_examined = 0;
          // Break out of resubmit loop as we have some results.
          break;
          // Handle when the query doesn't return results by exiting early
        } else if (this->records == 0 &&
                   status == tiledb::Query::Status::COMPLETE) {
          // Reset bitmap to original
          dbug_tmp_restore_column_map(&table->write_set, original_bitmap);
          if (reset)
            index_end();
          DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
        }
      } while (status == tiledb::Query::Status::INCOMPLETE);
    }

    bool found = false;
    do {
      if (this->record_index == this->records) {
        // if we reached the end, go back to the beginning.
        // This will ensure that we don't miss out on keys.
        // Worst case is that the key is at position n-1 and we are at
        // position n. If all keys are sorted as they are supposed to be
        // since we are passing the mrr_sort_keys=on parameter, this
        // algorithm should be optimal. Worst case is when the sort
        // order is descending.
        this->record_index = 0;
      }
      int key_cmp = compare_key_to_dims(key, key_len, this->record_index);
      // If the current index coordinates matches the key we are looking for we
      // must set the fields. Key is found!
      if ((key_cmp == 0 &&
           (find_flag == ha_rkey_function::HA_READ_KEY_EXACT ||
            find_flag == ha_rkey_function::HA_READ_KEY_OR_NEXT ||
            find_flag == ha_rkey_function::HA_READ_KEY_OR_PREV)) ||
          (key_cmp > 0 && (find_flag == ha_rkey_function::HA_READ_BEFORE_KEY ||
                           find_flag == ha_rkey_function::HA_READ_KEY_OR_PREV ||
                           find_flag == ha_rkey_function::HA_READ_AFTER_KEY)) ||
          (key_cmp < 0 &&
           (find_flag == ha_rkey_function::HA_READ_AFTER_KEY ||
            find_flag == ha_rkey_function::HA_READ_KEY_OR_NEXT))) {
        tileToFields(record_index, false, table);
        found = true;

        // set up next read in this batch
        this->record_index++;
        this->records_examined = 0;
        break;
      } else if (key_cmp < 0) {

        // First lets make sure the "start" record of this (potentially
        // incomplete) query buffers is before what we are looking for
        // If the key we are looking for is before the first record it
        // means we've missed the key and the request can not be resolved
        if (compare_key_to_dims(key, key_len, 0) < 0) {

          // If we have already tried rerunning the query from the
          // start then the request can not be satisfied we need to
          // return key not found to prevent an infinite loop
          if (restarted_scan) {
            // Reset bitmap to original
            dbug_tmp_restore_column_map(&table->write_set, original_bitmap);
            if (reset)
              index_end();
            DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
          }

          // restart scan
          this->query->set_layout(tiledb_layout_t::TILEDB_ROW_MAJOR);
          restarted_scan = true;
          goto begin;
        }
      }

      // always increase record index. The records_examined refers to the
      // records we have checked to see if our key matches.
      // When the records_examined value equals to all records in this batch
      // we can safely say that we have checked all keys
      this->record_index++;
      this->records_examined++;
    } while (this->records_examined < this->records);

    // if key not found
    if (!found) {
      // check for restart
      if (!restarted_scan) {
        restarted_scan = true;
        goto begin;
      }
      // Key not found even after restart, reset bitmap to original
      dbug_tmp_restore_column_map(&table->write_set, original_bitmap);
      if (reset)
        index_end();
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    }

  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR,
                    "[index_read_scan] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = ERR_INDEX_READ_SCAN_TILEDB;
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR,
                    "[index_read_scan] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = ERR_INDEX_READ_SCAN_OTHER;
  }

  // Reset bitmap to original
  dbug_tmp_restore_column_map(&table->write_set, original_bitmap);
  if (reset)
    index_end();
  DBUG_RETURN(rc);
}

int tile::mytile::index_read_idx_map(uchar *buf, uint idx, const uchar *key,
                                     key_part_map keypart_map,
                                     enum ha_rkey_function find_flag) {
  DBUG_ENTER("tile::mytile::index_read_idx_map");

  uint key_len = calculate_key_len(table, idx, key, keypart_map);

  // reset or add pushdowns for this key if not MRR
  if (!this->mrr_query) {
    this->set_pushdowns_for_key(key, key_len, true /* start_key */, find_flag);

    // If we are doing an index scan we need to use row-major order to get the
    // results in the expected order

    int rc = init_scan(this->ha_thd());
    if (rc)
      DBUG_RETURN(rc);

    this->query->set_layout(tiledb_layout_t::TILEDB_ROW_MAJOR);
  }

  DBUG_RETURN(index_read_scan(key, key_len, find_flag, true /* reset */));
}

#if MYSQL_VERSION_ID < 100500
ha_rows tile::mytile::records_in_range(uint inx, key_range *min_key,
                                       key_range *max_key) {
#else
ha_rows tile::mytile::records_in_range(uint inx, const key_range *min_key,
                                       const key_range *max_key,
                                       page_range *page) {
#endif
  return (ha_rows)10000;
}

int tile::mytile::set_pushdowns_for_key(const uchar *key, uint key_len,
                                        bool start_key,
                                        enum ha_rkey_function find_flag) {
  DBUG_ENTER("tile::mytile::set_pushdowns_for_key");
  std::map<uint64_t, std::shared_ptr<tile::range>> ranges_from_keys =
      tile::build_ranges_from_key(ha_thd(), table, key, key_len, find_flag,
                                  start_key, this->array_schema->domain());

  if (!ranges_from_keys.empty()) {
    if (this->query_complete() ||
        (!this->valid_pushed_ranges() && !this->valid_pushed_in_ranges())) {
      this->pushdown_ranges.clear();
      this->pushdown_in_ranges.clear();
      this->pushdown_ranges.resize(this->ndim);
      this->pushdown_in_ranges.resize(this->ndim);

      // Copy shared pointer to main range pushdown
      for (uint64_t i = 0; i < this->ndim; i++) {
        auto range = ranges_from_keys[i];
        if (range.get() != nullptr) {
          this->pushdown_ranges[i].push_back(range);
        }
      }
    }
  }
  DBUG_RETURN(0);
}

/****************************************************************************
 * MRR implementation: use DS-MRR
 ***************************************************************************/

int tile::mytile::build_mrr_ranges() {
  DBUG_ENTER("tile::mytile::build_mrr_ranges");
  this->pushdown_ranges.clear();
  this->pushdown_in_ranges.clear();

  std::vector<std::shared_ptr<tile::range>> tmp_ranges;
  this->pushdown_ranges.resize(this->ndim);
  this->pushdown_in_ranges.resize(this->ndim);
  for (uint64_t i = 0; i < this->ndim; i++) {
    tmp_ranges.emplace_back(std::make_shared<tile::range>(tile::range{
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        Item_func::EQ_FUNC, tiledb_datatype_t::TILEDB_ANY, 0, 0}));
  };

  // Get domain and dimensions
  auto domain = this->array_schema->domain();
  auto dims = domain.dimensions();

  // Loop over all keys
  while (!mrr_funcs.next(mrr_iter, &mrr_cur_range)) {

    if (mrr_cur_range.start_key.key != nullptr) {
      uint64_t key_offset = 0;
      for (uint64_t i = 0; i < this->ndim; i++) {

        // Exit when we've reached the end of the key
        if (key_offset >= mrr_cur_range.start_key.length)
          break;

        uint64_t key_len = 0;
        bool last_key_part = false;
        tiledb_datatype_t datatype = dims[i].type();
        Field *field;
        for (uint64_t fi = 0; fi < table->s->fields; fi++) {
          Field *f = table->s->field[fi];
          if (std::string(f->field_name.str, f->field_name.length) ==
              dims[i].name()) {
            field = f;
            break;
          }
        }

        auto &range = tmp_ranges[i];
        if (datatype == TILEDB_STRING_ASCII) {
          const uint16_t char_length = *reinterpret_cast<const uint16_t *>(
              mrr_cur_range.start_key.key + key_offset);
          key_len += sizeof(uint16_t);
          key_len += char_length;

          // If the key size is zero, this can happen when we are doing partial
          // key matches For strings a size of 0 means anything should be
          // considered a match so we return 0
          if (char_length == 0) {
            range->lower_value = nullptr;
            range->upper_value = nullptr;
            continue;
          }
        } else {
          key_len += table->s->key_info[active_index].key_part[i].length;
        }

        if (key_offset + key_len >= mrr_cur_range.start_key.length)
          last_key_part = true;

        range->datatype = datatype;
        update_range_from_key_for_super_range(
            range, mrr_cur_range.start_key, key_offset, true /* start_key */,
            last_key_part, datatype, ha_thd(), field);
        key_offset += key_len;
      }
    }

    // If the end key is not the same as the start key we need to also build the
    // range from it
    if (mrr_cur_range.end_key.key != nullptr) {
      uint64_t key_offset = 0;
      for (uint64_t i = 0; i < this->ndim; i++) {

        // Exit when we've reached the end of the key
        if (key_offset >= mrr_cur_range.end_key.length)
          break;

        uint64_t key_len = 0;
        bool last_key_part = false;
        tiledb_datatype_t datatype = dims[i].type();
        Field *field;
        for (uint64_t fi = 0; fi < table->s->fields; fi++) {
          Field *f = table->s->field[fi];
          if (std::string(f->field_name.str, f->field_name.length) ==
              dims[i].name()) {
            field = f;
            break;
          }
        }

        auto &range = tmp_ranges[i];

        if (datatype == TILEDB_STRING_ASCII) {
          const uint16_t char_length = *reinterpret_cast<const uint16_t *>(
              mrr_cur_range.end_key.key + key_offset);
          key_len += sizeof(uint16_t);
          key_len += char_length;

          if (char_length == 0) {
            range->lower_value = nullptr;
            range->upper_value = nullptr;
            continue;
          }
        } else {
          key_len += table->s->key_info[active_index].key_part[i].length;
        }

        if (key_offset + key_len >= mrr_cur_range.end_key.length)
          last_key_part = true;

        range->datatype = datatype;
        update_range_from_key_for_super_range(
            range, mrr_cur_range.end_key, key_offset, false /* start_key */,
            last_key_part, datatype, ha_thd(), field);
        key_offset += key_len;
      }
    }
  }

  // Now that we have all ranges, let's build them into super ranges
  for (size_t i = 0; i < tmp_ranges.size(); i++) {
    auto &range = tmp_ranges[i];
    if (range->operation_type != Item_func::BETWEEN &&
        range->lower_value != nullptr && range->upper_value != nullptr)
      range->operation_type = Item_func::BETWEEN;

    if (range->lower_value != nullptr || range->upper_value != nullptr)
      this->pushdown_ranges[i].push_back(range);
  }

  int rc = init_scan(this->ha_thd());
  if (rc)
    DBUG_RETURN(rc);

  this->query->set_layout(tiledb_layout_t::TILEDB_ROW_MAJOR);
  DBUG_RETURN(rc);
}

int tile::mytile::multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param,
                                        uint n_ranges, uint mode,
                                        HANDLER_BUFFER *buf) {

  DBUG_ENTER("tile::mytile::multi_range_read_init");
  // If MRR is disabled fall back to default implementation
  if (!tile::sysvars::mrr_support(ha_thd())) {
    DBUG_RETURN(handler::multi_range_read_init(seq, seq_init_param, n_ranges,
                                               mode, buf));
  }
  mrr_iter = seq->init(seq_init_param, n_ranges, mode);
  mrr_funcs = *seq;

  this->mrr_query = true;

  int rc = build_mrr_ranges();
  if (rc)
    DBUG_RETURN(rc);

  // Never use default implementation also use sort key access
  mode &= ~HA_MRR_USE_DEFAULT_IMPL;
  DBUG_RETURN(
      ds_mrr.dsmrr_init(this, seq, seq_init_param, n_ranges, mode, buf));
}

int tile::mytile::multi_range_read_next(range_id_t *range_info) {
  DBUG_ENTER("tile::mytile::multi_range_read_next");
  // If MRR is disabled fall back to default implementation
  if (!tile::sysvars::mrr_support(ha_thd())) {
    DBUG_RETURN(handler::multi_range_read_next(range_info));
  }
  int res = ds_mrr.dsmrr_next(range_info);
  DBUG_RETURN(res);
}

ha_rows tile::mytile::multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq,
                                                  void *seq_init_param,
                                                  uint n_ranges, uint *bufsz,
                                                  uint *mrr_mode, ha_rows limit,
                                                  Cost_estimate *cost) {
  DBUG_ENTER("tile::mytile::multi_range_read_info_const");
  // If MRR is disabled fall back to default implementation
  if (!tile::sysvars::mrr_support(ha_thd())) {
    DBUG_RETURN(handler::multi_range_read_info_const(
        keyno, seq, seq_init_param, n_ranges, bufsz, mrr_mode, limit, cost));
  }
  /*
    This call is here because there is no location where this->table would
    already be known.
    TODO: consider moving it into some per-query initialization call.
  */
  ds_mrr.init(this, table);
  bool rc = ds_mrr.dsmrr_info_const(keyno, seq, seq_init_param, n_ranges, bufsz,
                                    mrr_mode, limit, cost);

  DBUG_RETURN(rc);
}

ha_rows tile::mytile::multi_range_read_info(uint keyno, uint n_ranges,
                                            uint keys, uint key_parts,
                                            uint *bufsz, uint *flags,
                                            Cost_estimate *cost) {
  DBUG_ENTER("tile::mytile::multi_range_read_info");
  // If MRR is disabled fall back to default implementation
  if (!tile::sysvars::mrr_support(ha_thd())) {
    DBUG_RETURN(handler::multi_range_read_info(keyno, n_ranges, keys, key_parts,
                                               bufsz, flags, cost));
  }
  ds_mrr.init(this, table);
  *flags &= ~HA_MRR_USE_DEFAULT_IMPL;
  bool rc =
      ds_mrr.dsmrr_info(keyno, n_ranges, keys, key_parts, bufsz, flags, cost);
  *flags &= ~HA_MRR_USE_DEFAULT_IMPL;
  DBUG_RETURN(rc);
}

int tile::mytile::multi_range_read_explain_info(uint mrr_mode, char *str,
                                                size_t size) {
  DBUG_ENTER("tile::mytile::multi_range_read_explain_info");
  // If MRR is disabled fall back to default implementation
  if (!tile::sysvars::mrr_support(ha_thd())) {
    DBUG_RETURN(handler::multi_range_read_explain_info(mrr_mode, str, size));
  }
  mrr_mode &= ~HA_MRR_USE_DEFAULT_IMPL;
  DBUG_RETURN(ds_mrr.dsmrr_explain_info(mrr_mode, str, size));
}
/* MyTile MRR implementation ends */

int tile::mytile::index_next_same(uchar *buf, const uchar *key, uint keylen) {
  int error;
  DBUG_ENTER("mytile::index_next_same");
  // Store the current indexes so we can reset them after checking
  // if the next key is the same
  uint64_t record_index_before_check = this->record_index;
  uint64_t records_read_before_check = this->records_read;
  uint64_t records_examined_before_check = this->records_examined;
  // Index needs to be reset to 0 due to incomplete query resubmission
  // If we don't reset it to zero we'll start at the previous index which will
  // be wrong since index_next runs the next query
  const bool reset_to_zero_index = this->records_examined >= this->records;
  if (!(error = index_next(buf))) {
    if (key_cmp_if_same(table, key, active_index, keylen)) {
      table->status = STATUS_NOT_FOUND;
      error = HA_ERR_END_OF_FILE;
    };

    // Reset the record indexes back to before the first read
    if (this->mrr_query) {
      this->record_index = record_index_before_check;
      this->records_read = records_read_before_check;
      this->records_examined = records_examined_before_check;
      if (reset_to_zero_index) {
        this->record_index = 0;
      }
    }
  }

  DBUG_PRINT("return", ("%i", error));
  DBUG_RETURN(error);
}

std::shared_ptr<tiledb::Query> &tile::mytile::get_query() {
  return this->query;
}

std::shared_ptr<tiledb::Array> &tile::mytile::get_array() {
  return this->array;
}

std::shared_ptr<tiledb::QueryCondition> &tile::mytile::get_qc() {
  return this->query_condition;
}

std::vector<std::vector<std::shared_ptr<tile::range>>> &
tile::mytile::get_pushdown_ranges() {
  return this->pushdown_ranges;
}

std::vector<std::vector<std::shared_ptr<tile::range>>> &
tile::mytile::get_pushdown_in_ranges() {
  return this->pushdown_in_ranges;
}

std::string tile::mytile::get_uri() { return this->uri; }

TABLE *tile::mytile::get_table() { return this->table; }

mysql_declare_plugin(mytile){
    MYSQL_STORAGE_ENGINE_PLUGIN, /* the plugin type (a MYSQL_XXX_PLUGIN value)
                                  */
    &mytile_storage_engine, /* pointer to type-specific plugin descriptor   */
    "MyTile",               /* plugin name                                  */
    "TileDB, Inc.",         /* plugin author (for I_S.PLUGINS)              */
    "Storage engine for accessing TileDB Arrays", /* general descriptive text
                                                     (for I_S.PLUGINS)   */
    PLUGIN_LICENSE_PROPRIETARY, /* the plugin license (PLUGIN_LICENSE_XXX) */
    mytile_init_func,           /* Plugin Init */
    NULL,                       /* Plugin Deinit */
    0x0342,                     /* version number (0.34.2) */
    tile::statusvars::mytile_status_variables, /* status variables */
    tile::sysvars::mytile_system_variables,    /* system variables */
    NULL,                                      /* config options */
    0,                                         /* flags */
} mysql_declare_plugin_end;
maria_declare_plugin(mytile){
    MYSQL_STORAGE_ENGINE_PLUGIN, /* the plugin type (a MYSQL_XXX_PLUGIN value)
                                  */
    &mytile_storage_engine, /* pointer to type-specific plugin descriptor   */
    "MyTile",               /* plugin name                                  */
    "TileDB, Inc.",         /* plugin author (for I_S.PLUGINS)              */
    "Storage engine for accessing TileDB Arrays", /* general descriptive text
                                                     (for I_S.PLUGINS)   */
    PLUGIN_LICENSE_PROPRIETARY, /* the plugin license (PLUGIN_LICENSE_XXX) */
    mytile_init_func,           /* Plugin Init */
    NULL,                       /* Plugin Deinit */
    0x0342,                     /* version number (0.34.2) */
    tile::statusvars::mytile_status_variables, /* status variables */
    tile::sysvars::mytile_system_variables,    /* system variables */
    "0.34.2",                                  /* string version */
    MariaDB_PLUGIN_MATURITY_BETA               /* maturity */
} maria_declare_plugin_end;