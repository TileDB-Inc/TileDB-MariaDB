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
#include "mytile-discovery.h"
#include "mytile-sysvars.h"
#include "mytile.h"
#include "utils.h"
#include <cstring>
#include <log.h>
#include <my_config.h>
#include <mysql/plugin.h>
#include <mysqld_error.h>
#include <vector>
#include <unordered_map>

// Handler for mytile engine
handlerton *mytile_hton;

// Structure for table options
ha_create_table_option mytile_table_option_list[] = {
    HA_TOPTION_STRING("uri", array_uri),
    HA_TOPTION_ENUM("array_type", array_type, "DENSE,SPARSE", 1),
    HA_TOPTION_NUMBER("capacity", capacity, 10000, 0, UINT64_MAX, 1),
    HA_TOPTION_ENUM("cell_order", cell_order, "ROW_MAJOR,COLUMN_MAJOR",
                    TILEDB_ROW_MAJOR),
    HA_TOPTION_ENUM("tile_order", tile_order, "ROW_MAJOR,COLUMN_MAJOR",
                    TILEDB_ROW_MAJOR),
    HA_TOPTION_NUMBER("open_at", open_at, UINT64_MAX, 0, UINT64_MAX, 1),
    HA_TOPTION_STRING("encryption_key", encryption_key),
    HA_TOPTION_END};

// Structure for specific field options
ha_create_table_option mytile_field_option_list[] = {
    HA_FOPTION_BOOL("dimension", dimension, false),
    HA_FOPTION_STRING("lower_bound", lower_bound),
    HA_FOPTION_STRING("upper_bound", upper_bound),
    HA_FOPTION_STRING("tile_extent", tile_extent), HA_FOPTION_END};

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
  mytile_hton->state = SHOW_OPTION_YES;
  mytile_hton->create = mytile_create_handler;
  mytile_hton->tablefile_extensions = mytile_exts;
  mytile_hton->table_options = mytile_table_option_list;
  mytile_hton->field_options = mytile_field_option_list;
  // Set table discovery functions
  mytile_hton->discover_table_structure = tile::mytile_discover_table_structure;
  mytile_hton->discover_table = tile::mytile_discover_table;

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

int tile::mytile::create(const char *name, TABLE *table_arg,
                         HA_CREATE_INFO *create_info) {
  DBUG_ENTER("tile::mytile::create");
  // First rebuild context with new config if needed
  tiledb::Config cfg = build_config(ha_thd());

  if (!compare_configs(cfg, this->config)) {
    this->config = cfg;
    this->ctx = build_context(this->config);
  }
  DBUG_RETURN(create_array(name, table_arg, create_info, this->ctx));
}

int tile::mytile::open(const char *name, int mode, uint test_if_locked) {
  DBUG_ENTER("tile::mytile::open");

  // First rebuild context with new config if needed
  tiledb::Config cfg = build_config(ha_thd());

  if (!compare_configs(cfg, this->config)) {
    this->config = cfg;
    this->ctx = build_context(this->config);
  }

  // Open TileDB Array
  try {
    std::string encryption_key;
    if (this->table->s->option_struct->encryption_key != nullptr) {
      encryption_key =
          std::string(this->table->s->option_struct->encryption_key);
    } else if (this->table_share->option_struct->encryption_key != nullptr) {
      encryption_key =
          std::string(this->table_share->option_struct->encryption_key);
    }
    uri = name;
    if (this->table->s->option_struct->array_uri != nullptr)
      uri = this->table->s->option_struct->array_uri;

    this->array_schema =
        std::unique_ptr<tiledb::ArraySchema>(new tiledb::ArraySchema(
            this->ctx, this->uri,
            encryption_key.empty() ? TILEDB_NO_ENCRYPTION : TILEDB_AES_256_GCM,
            encryption_key));
    auto domain = this->array_schema->domain();
    this->ndim = domain.ndim();

    // Set ref length used for storing reference in position(), this is the size
    // of a subarray for querying
    this->ref_length = (this->ndim * tiledb_datatype_size(domain.type()) * 2);

    // If the user requests we will compute the table records on opening the
    // array
    if (tile::sysvars::compute_table_records(ha_thd())) {

      open_array_for_reads(ha_thd());

      uint64_t subarray_size =
          tiledb_datatype_size(domain.type()) * this->ndim * 2;
      auto subarray = std::unique_ptr<void, decltype(&std::free)>(
          std::malloc(subarray_size), &std::free);

      // Get the non empty domain
      this->ctx.handle_error(tiledb_array_get_non_empty_domain(
          this->ctx.ptr().get(), this->array->ptr().get(), subarray.get(),
          &this->empty_read));

      this->records_upper_bound = computeRecordsUB(this->array, subarray.get());
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
    if (this->query != nullptr)
      this->query = nullptr;

    // close array
    if (this->array != nullptr && this->array->is_open())
      this->array->close();

    // Clear all allocated buffers
    dealloc_buffers();
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "close error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, uri.c_str(), e.what());
    DBUG_RETURN(-20);
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "close error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, uri.c_str(), e.what());
    DBUG_RETURN(-21);
  }
  DBUG_RETURN(0);
}

int tile::mytile::create_array(const char *name, TABLE *table_arg,
                               HA_CREATE_INFO *create_info,
                               tiledb::Context context) {
  DBUG_ENTER("tile::create_array");
  int rc = 0;

  tiledb_array_type_t arrayType = TILEDB_SPARSE;
  if (create_info->option_struct->array_type == 1) {
    arrayType = TILEDB_SPARSE;
  } else {
    arrayType = TILEDB_DENSE;
  }

  // Create array schema
  std::unique_ptr<tiledb::ArraySchema> schema =
      std::make_unique<tiledb::ArraySchema>(context, arrayType);

  // Create domain
  tiledb::Domain domain(context);

  // Only a single key is support, and that is the primary key. We can use the
  // primary key as an alternative to get which fields are suppose to be the
  // dimensions
  std::unordered_map<std::string, bool> primaryKeyParts;
  if (table_arg->key_info != nullptr) {
    KEY key_info = table_arg->key_info[0];
    for (uint i = 0; i < key_info.user_defined_key_parts; i++) {
      Field *field = key_info.key_part[i].field;
      primaryKeyParts[field->field_name.str] = true;
    }
  }

  // Create attributes or dimensions
  for (Field **ffield = table_arg->field; *ffield; ffield++) {
    Field *field = (*ffield);
    // If the field has the dimension flag set or it is part of the primary key
    // we treat it is a dimension
    if (field->option_struct->dimension ||
        primaryKeyParts.find(field->field_name.str) != primaryKeyParts.end()) {

      // Validate the user has set the tile extent
      // Only tile extent is checked because for the dimension domain we use the
      // datatypes min/max values
      if (field->option_struct->tile_extent == nullptr ||
          strcmp(field->option_struct->tile_extent, "") == 0) {
        my_printf_error(
            ER_UNKNOWN_ERROR,
            "Dimension field %s tile_extent was not set, can not create table",
            ME_ERROR_LOG | ME_FATAL, field->field_name);
        DBUG_RETURN(-13);
      }
      domain.add_dimension(create_field_dimension(context, field));
    } else { // Else this is treated as a dimension
      // Currently hard code the filter list to zstd compression
      tiledb::FilterList filterList(context);
      tiledb::Filter filter(context, TILEDB_FILTER_ZSTD);
      filterList.add_filter(filter);
      tiledb::Attribute attr =
          create_field_attribute(context, field, filterList);
      schema->add_attribute(attr);
    };
  }

  schema->set_domain(domain);

  // Set capacity
  schema->set_capacity(create_info->option_struct->capacity);

  // Set cell ordering if configured
  if (create_info->option_struct->cell_order == 0) {
    schema->set_cell_order(TILEDB_ROW_MAJOR);
  } else if (create_info->option_struct->cell_order == 1) {
    schema->set_cell_order(TILEDB_COL_MAJOR);
  }

  // Set tile ordering if configured
  if (create_info->option_struct->tile_order == 0) {
    schema->set_tile_order(TILEDB_ROW_MAJOR);
  } else if (create_info->option_struct->tile_order == 1) {
    schema->set_tile_order(TILEDB_COL_MAJOR);
  }

  // Get array uri from name or table option
  std::string create_uri = name;
  if (create_info->option_struct->array_uri != nullptr)
    create_uri = create_info->option_struct->array_uri;

  // Check array schema
  try {
    schema->check();
  } catch (tiledb::TileDBError &e) {
    my_printf_error(ER_UNKNOWN_ERROR, "Error in building schema %s",
                    ME_ERROR_LOG | ME_FATAL, e.what());
    DBUG_RETURN(-10);
  }

  try {
    std::string encryption_key;
    if (create_info->option_struct->encryption_key != nullptr) {
      encryption_key = std::string(create_info->option_struct->encryption_key);
    }
    // Create the array on storage
    tiledb::Array::create(create_uri, *schema,
                          encryption_key.empty() ? TILEDB_NO_ENCRYPTION
                                                 : TILEDB_AES_256_GCM,
                          encryption_key);
    // Next we write the frm file to persist the newly created table
    table_arg->s->write_frm_image();
  } catch (tiledb::TileDBError &e) {
    my_printf_error(ER_UNKNOWN_ERROR, "Error in creating array %s",
                    ME_ERROR_LOG | ME_FATAL, e.what());
    DBUG_RETURN(-11);
  }
  DBUG_RETURN(rc);
}

int tile::mytile::init_scan(
    THD *thd, std::unique_ptr<void, decltype(&std::free)> subarray) {
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

    // Create the subarray
    uint64_t subarray_size =
        tiledb_datatype_size(domain.type()) * dims.size() * 2;
    if (subarray == nullptr) {
      subarray = std::unique_ptr<void, decltype(&std::free)>(
          std::malloc(subarray_size), &std::free);
    }

    // Get the non empty domain
    this->ctx.handle_error(tiledb_array_get_non_empty_domain(
        this->ctx.ptr().get(), this->array->ptr().get(), subarray.get(),
        &this->empty_read));

    this->total_num_records_UB = computeRecordsUB(this->array, subarray.get());
    if (!this->valid_pushed_ranges() &&
        !this->valid_pushed_in_ranges()) { // No pushdown
      if (this->empty_read)
        DBUG_RETURN(rc);

      log_debug(thd, "no pushdowns possible for query on table %s",
                this->table->s->table_name.str);

      // Set subarray using capi
      this->ctx.handle_error(tiledb_query_set_subarray(
          this->ctx.ptr().get(), this->query->ptr().get(), subarray.get()));

    } else {
      log_debug(thd, "pushdown of ranges for query on table %s",
                this->table->s->table_name.str);

      // Loop over dimensions and build rangers for that dimension
      for (uint64_t dim_idx = 0; dim_idx < this->ndim; dim_idx++) {
        // This ranges vector and the ranges it contains will be manipulated in
        // merge ranges
        const auto &ranges = this->pushdown_ranges[dim_idx];

        // If there is valid ranges from IN clause, first we must see if they
        // are contained inside the merged super range we have
        const auto &in_ranges = this->pushdown_in_ranges[dim_idx];

        // get start of non empty domain
        void *lower = static_cast<char *>(subarray.get()) +
                      (dim_idx * 2 * tiledb_datatype_size(domain.type()));

        // If the ranges for this dimension are not empty, we'll push it down
        // else non empty domain is used
        if (!ranges.empty() || !in_ranges.empty()) {
          log_debug(thd, "Pushdown for %s.%s", this->table->s->table_name.str,
                    dims[dim_idx].name().c_str());

          std::shared_ptr<tile::range> range = nullptr;
          if (!ranges.empty()) {
            // Merge multiple ranges into single super range
            range = merge_ranges(ranges, dims[dim_idx].type());

            if (range != nullptr) {
              // Setup the range by filling in missing values with non empty
              // domain
              setup_range(thd, range, lower, dims[dim_idx]);

              // set range
              this->ctx.handle_error(tiledb_query_add_range(
                  this->ctx.ptr().get(), this->query->ptr().get(), dim_idx,
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
              this->ctx.handle_error(tiledb_query_add_range(
                  this->ctx.ptr().get(), this->query->ptr().get(), dim_idx,
                  in_range->lower_value.get(), in_range->upper_value.get(),
                  nullptr));
            }
          }
        } else { // If the range is empty we need to use the non-empty-domain

          void *upper =
              static_cast<char *>(lower) + tiledb_datatype_size(domain.type());
          this->ctx.handle_error(tiledb_query_add_range(
              this->ctx.ptr().get(), this->query->ptr().get(), dim_idx, lower,
              upper, nullptr));
        }
      }
    }
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[init_scan] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = -111;
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[init_scan] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = -112;
  }
  DBUG_RETURN(rc);
}

int tile::mytile::rnd_init(bool scan) {
  DBUG_ENTER("tile::mytile::rnd_init");
  DBUG_RETURN(init_scan(
      this->ha_thd(),
      std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free)));
};

bool tile::mytile::query_complete() {
  DBUG_ENTER("tile::mytile::query_complete");
  // If we have run out of records report EOF
  // note the upper bound of records might be *more* than actual results, thus
  // this check is not guaranteed see the next check were we look for complete
  // query and row position
  if (this->records_read >= this->total_num_records_UB) {
    DBUG_RETURN(true);
  }

  // If we are complete and there is no more records we report EOF
  if (this->status == tiledb::Query::Status::COMPLETE &&
      static_cast<int64_t>(this->record_index) >= this->records) {
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
  my_bitmap_map *original_bitmap =
      dbug_tmp_use_all_columns(table, table->write_set);
  tiledb_query_status_to_str(static_cast<tiledb_query_status_t>(status),
                             &query_status);

  if (this->query_complete()) {
    // Reset bitmap to original
    dbug_tmp_restore_column_map(table->write_set, original_bitmap);
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  try {
    // If the cursor has passed the number of records from the previous query
    // (or if this is the first time), (re)submit the query->
    if (static_cast<int64_t>(this->record_index) >= this->records) {
      do {
        this->status = query->submit();

        // Compute the number of cells (records) that were returned by the query
        this->records = this->coord_buffer->buffer_size / this->ndim /
                        tiledb_datatype_size(this->coord_buffer->type);

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
          dbug_tmp_restore_column_map(table->write_set, original_bitmap);
          DBUG_RETURN(rc);
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
    rc = -121;
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[scan_rnd_row] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = -122;
  }

  // Reset bitmap to original
  dbug_tmp_restore_column_map(table->write_set, original_bitmap);
  DBUG_RETURN(rc);
}

int tile::mytile::rnd_next(uchar *buf) {
  DBUG_ENTER("tile::mytile::rnd_next");
  DBUG_RETURN(scan_rnd_row(table));
}

int tile::mytile::rnd_end() {
  DBUG_ENTER("tile::mytile::rnd_end");
  dealloc_buffers();
  this->pushdown_ranges.clear();
  this->pushdown_in_ranges.clear();
  // Reset indicators
  this->record_index = 0;
  this->records = 0;
  this->records_read = 0;
  this->status = tiledb::Query::Status::UNINITIALIZED;
  this->total_num_records_UB = 0;
  this->query = nullptr;
  ds_mrr.dsmrr_close();
  DBUG_RETURN(close());
};

int tile::mytile::rnd_pos(uchar *buf, uchar *pos) {
  DBUG_ENTER("tile::mytile::rnd_pos");
  auto domain = this->array_schema->domain();
  this->ctx.handle_error(tiledb_query_set_subarray(
      this->ctx.ptr().get(), this->query->ptr().get(), pos));
  this->total_num_records_UB = computeRecordsUB(this->array, pos);

  // Reset indicators
  this->record_index = 0;
  this->records = 0;
  this->records_read = 0;
  this->status = tiledb::Query::Status::UNINITIALIZED;
  DBUG_RETURN(rnd_next(buf));
}

void tile::mytile::position(const uchar *record) {
  DBUG_ENTER("tile::mytile::position");
  // copy the position
  uint64_t datatype_size = tiledb_datatype_size(this->coord_buffer->type);
  // The index offset for coordinates is computed based on the number of
  // dimensions, the datatype size and the record index We must subtract one
  // from the record index becuase position is called after the end of rnd_next
  // which means we have already incremented the index position
  uint64_t index_offset = (this->record_index - 1) * this->ndim *
                          tiledb_datatype_size(this->coord_buffer->type);
  // Helper variable to pointer of start for coordinates offsets
  char *coord_start =
      static_cast<char *>(this->coord_buffer->buffer) + index_offset;

  // Loop through each dimension and copy the current coordinates to setup a new
  // subarray
  for (uint64_t i = 0; i < this->ndim; i++) {
    memcpy(this->ref + (i * datatype_size * 2),
           coord_start + (i * datatype_size), datatype_size);
    memcpy(this->ref + ((i * datatype_size * 2) + datatype_size),
           coord_start + (i * datatype_size), datatype_size);
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

    if (!buff->dimension) {
      if (buff->offset_buffer != nullptr) {
        free(buff->offset_buffer);
        buff->offset_buffer = nullptr;
      }

      if (buff->buffer != nullptr) {
        free(buff->buffer);
        buff->buffer = nullptr;
      }
    }
  }

  if (this->coord_buffer != nullptr && this->coord_buffer->buffer != nullptr) {
    free(this->coord_buffer->buffer);
    this->coord_buffer->buffer = nullptr;
    this->coord_buffer = nullptr;
  }

  this->buffers.clear();
  DBUG_VOID_RETURN;
}

const COND *tile::mytile::cond_push_cond(Item_cond *cond_item) {
  DBUG_ENTER("tile::mytile::cond_push_cond");
  switch (cond_item->functype()) {
  case Item_func::COND_AND_FUNC:
    // vop = OP_AND;
    break;
  case Item_func::COND_OR_FUNC: // Currently don't support OR pushdown
    DBUG_RETURN(cond_item);
    //    break;
  default:
    DBUG_RETURN(nullptr);
  } // endswitch functype

  List<Item> *arglist = cond_item->argument_list();
  List_iterator<Item> li(*arglist);
  const Item *subitem;

  for (uint32_t i = 0; i < arglist->elements; i++) {
    if ((subitem = li++)) {
      // COND_ITEMs
      cond_push_local(dynamic_cast<const COND *>(subitem));
    }
  }
  DBUG_RETURN(nullptr);
}

const COND *tile::mytile::cond_push_func(const Item_func *func_item) {
  DBUG_ENTER("tile::mytile::cond_push_func");
  Item **args = func_item->arguments();
  bool neg = FALSE;

  Item_field *column_field = dynamic_cast<Item_field *>(args[0]);
  // If we can't convert the condition to a column let's bail
  // We should add support at some point for handling functions (i.e.
  // date_dimension = current_date())
  if (column_field == nullptr) {
    DBUG_RETURN(func_item);
  }
  // If the condition is not a dimension we can't handle it
  if (!this->array_schema->domain().has_dimension(
          column_field->field_name.str)) {
    DBUG_RETURN(func_item);
  }

  uint64_t dim_idx = 0;
  auto dims = this->array_schema->domain().dimensions();
  for (uint64_t j = 0; j < this->ndim; j++) {
    if (dims[j].name() == column_field->field_name.str)
      dim_idx = j;
  }

  switch (func_item->functype()) {
  case Item_func::NE_FUNC:
    DBUG_RETURN(func_item); /* Not equal is not supported */
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
          Item_func::EQ_FUNC, tiledb_datatype_t::TILEDB_ANY});

      // Get field type for comparison
      Item_result cmp_type = args[i]->cmp_type();

      int ret =
          set_range_from_item_consts(lower_const, upper_const, cmp_type, range);

      if (ret)
        DBUG_RETURN(func_item);

      // Add the range to the pushdown in ranges
      auto &range_vec = this->pushdown_in_ranges[dim_idx];
      range_vec.push_back(std::move(range));
    }

    break;
    // Handle equal case by setting upper and lower ranges to same value
  case Item_func::EQ_FUNC: {
    // Create unique ptrs
    std::shared_ptr<range> range = std::make_shared<tile::range>(tile::range{
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        func_item->functype(), tiledb_datatype_t::TILEDB_ANY});

    int ret =
        set_range_from_item_consts(dynamic_cast<Item_basic_constant *>(args[1]),
                                   dynamic_cast<Item_basic_constant *>(args[1]),
                                   args[1]->cmp_type(), range);

    if (ret)
      DBUG_RETURN(func_item);

    // Add the range to the pushdown ranges
    auto &range_vec = this->pushdown_ranges[dim_idx];
    range_vec.push_back(std::move(range));

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
        func_item->functype(), tiledb_datatype_t::TILEDB_ANY});

    int ret =
        set_range_from_item_consts(lower_const, upper_const, cmp_type, range);

    if (ret)
      DBUG_RETURN(func_item);

    // Add the range to the pushdown ranges
    auto &range_vec = this->pushdown_ranges[dim_idx];
    range_vec.push_back(std::move(range));

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

  DBUG_RETURN(cond_push_local(cond));
}

const COND *tile::mytile::cond_push_local(const COND *cond) {
  DBUG_ENTER("tile::mytile::cond_push_local");
  // Make sure pushdown ranges is not empty
  if (this->pushdown_ranges.empty()) {
    this->pushdown_ranges.resize(this->ndim);
  }

  // Make sure pushdown in ranges is not empty
  if (this->pushdown_in_ranges.empty()) {
    this->pushdown_in_ranges.resize(this->ndim);
  }

  switch (cond->type()) {
  case Item::COND_ITEM: {
    Item_cond *cond_item = dynamic_cast<Item_cond *>(const_cast<COND *>(cond));
    const COND *ret = cond_push_cond(cond_item);
    DBUG_RETURN(ret);
    break;
  }
  case Item::FUNC_ITEM: {
    const Item_func *func_item = dynamic_cast<const Item_func *>(cond);
    const COND *ret = cond_push_func(func_item);
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
  auto ret = cond_push_local(static_cast<Item_cond *>(idx_cond));
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
    DBUG_RETURN(-25);
  } catch (const std::exception &e) {
    // Log errors
    sql_print_error("delete_table error for table %s : %s", name, e.what());
    DBUG_RETURN(-26);
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
  DBUG_RETURN(HA_PARTIAL_COLUMN_READ | HA_REC_NOT_IN_SEQ | HA_CAN_SQL_HANDLER |
              // HA_REQUIRE_PRIMARY_KEY | HA_PRIMARY_KEY_IN_READ_INDEX |
              HA_CAN_TABLE_CONDITION_PUSHDOWN | HA_CAN_EXPORT |
              HA_CONCURRENT_OPTIMIZE | HA_CAN_ONLINE_BACKUPS |
              HA_CAN_BIT_FIELD | HA_FILE_BASED);
}

void tile::mytile::alloc_buffers(uint64_t size) {
  DBUG_ENTER("tile::mytile::alloc_buffers");
  // Set Attribute Buffers
  auto domain = this->array_schema->domain();
  auto dims = domain.dimensions();

  const char *array_type;
  tiledb_array_type_to_str(this->array_schema->array_type(), &array_type);

  // Set Coordinate Buffer
  // We don't use set_coords to avoid needing to switch on datatype
  auto coords_buffer = alloc_buffer(domain.type(), size);

  if (this->buffers.empty()) {
    for (size_t i = 0; i < table->s->fields; i++)
      this->buffers.emplace_back();
  }

  for (size_t fieldIndex = 0; fieldIndex < table->s->fields; fieldIndex++) {
    Field *field = table->field[fieldIndex];
    std::string field_name = field->field_name.str;
    // Only set buffers for fields that are asked for except always set
    // dimension We check the read_set because the read_set is set to ALL column
    // for writes and set to the subset of columns for reads
    if (!bitmap_is_set(this->table->read_set, fieldIndex) &&
        !this->array_schema->domain().has_dimension(field_name)) {
      continue;
    }

    // Create buffer
    std::shared_ptr<buffer> buff = std::make_shared<buffer>();
    buff->name = field_name;
    buff->dimension = false;
    buff->buffer_offset = 0;
    buff->fixed_size_elements = 1;
    buff->buffer_size = size;
    buff->allocated_buffer_size = size;

    if (this->array_schema->domain().has_dimension(field_name)) {
      buff->buffer = coords_buffer;
      buff->type = domain.type();
      buff->dimension = true;
      for (size_t i = 0; i < dims.size(); i++) {
        if (dims[i].name() == field_name) {
          buff->buffer_offset = i;
          break;
        }
      }
      buff->fixed_size_elements = domain.ndim();
      this->coord_buffer = buff;
    } else { // attribute
      tiledb::Attribute attr = this->array_schema->attribute(field_name);
      uint64_t *offset_buffer = nullptr;
      auto data_buffer = alloc_buffer(attr.type(), size);
      buff->fixed_size_elements = attr.cell_val_num();
      if (attr.variable_sized()) {
        offset_buffer = static_cast<uint64_t *>(
            alloc_buffer(tiledb_datatype_t::TILEDB_UINT64, size));
        buff->offset_buffer_size = size;
        buff->allocated_offset_buffer_size = size;
      }

      buff->offset_buffer = offset_buffer;
      buff->buffer = data_buffer;
      buff->type = attr.type();
    }
    this->buffers[fieldIndex] = buff;
  }
  DBUG_VOID_RETURN;
}

void tile::mytile::alloc_read_buffers(uint64_t size) {
  alloc_buffers(size);
  auto domain = this->array_schema->domain();

  for (auto &buff : this->buffers) {
    // Only set buffers which are non-null
    if (buff == nullptr)
      continue;

    if (domain.has_dimension(buff->name)) {
      this->ctx.handle_error(tiledb_query_set_buffer(
          this->ctx.ptr().get(), this->query->ptr().get(), tiledb_coords(),
          buff->buffer, &buff->buffer_size));
    } else {
      if (buff->offset_buffer != nullptr) {
        this->ctx.handle_error(tiledb_query_set_buffer_var(
            this->ctx.ptr().get(), this->query->ptr().get(), buff->name.c_str(),
            buff->offset_buffer, &buff->offset_buffer_size, buff->buffer,
            &buff->buffer_size));
      } else {
        this->ctx.handle_error(tiledb_query_set_buffer(
            this->ctx.ptr().get(), this->query->ptr().get(), buff->name.c_str(),
            buff->buffer, &buff->buffer_size));
      }
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
      field->set_notnull();

      if (buff->dimension) {
        index = (index * ndim) + buff->buffer_offset;
      } else if (dimensions_only) {
        continue;
      }

      set_field(ha_thd(), field, buff, index);
    }
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[tileToFields] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = -131;
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[tileToFields] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = -132;
  }

  DBUG_RETURN(rc);
};

int tile::mytile::mysql_row_to_tiledb_buffers(const uchar *buf) {
  DBUG_ENTER("tile::mytile::mysql_row_to_tiledb_buffers");
  int error = 0;

  try {
    for (size_t fieldIndex = 0; fieldIndex < table->s->fields; fieldIndex++) {
      Field *field = table->field[fieldIndex];
      // Error if there is a field missing from writing
      if (!bitmap_is_set(this->table->write_set, fieldIndex)) {
        my_printf_error(ER_UNKNOWN_ERROR,
                        "[mysql_row_to_tiledb_buffers] field %s is not set, "
                        "tiledb reqiures "
                        "all fields set for writting",
                        ME_ERROR_LOG | ME_FATAL, field->field_name.str);
      }

      if (field->is_null()) {
        error = HA_ERR_UNSUPPORTED;
      } else {
        std::shared_ptr<buffer> buffer = this->buffers[fieldIndex];
        error =
            set_buffer_from_field(field, buffer, this->record_index, ha_thd());
      }
    }
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    sql_print_error(
        "[mysql_row_to_tiledb_buffers] write error for table %s : %s",
        this->uri.c_str(), e.what());
    error = -101;
  } catch (const std::exception &e) {
    // Log errors
    sql_print_error(
        "[mysql_row_to_tiledb_buffers] write error for table %s : %s",
        this->uri.c_str(), e.what());
    error = -102;
  }

  DBUG_RETURN(error);
}

void tile::mytile::setup_write() {
  DBUG_ENTER("tile::mytile::setup_write");

  // Make sure array is open for writes
  open_array_for_writes(ha_thd());

  // We must set the bitmap for debug purpose, it is "read_set" because we use
  // Field->val_*
  my_bitmap_map *original_bitmap = tmp_use_all_columns(table, table->read_set);
  this->write_buffer_size = tile::sysvars::write_buffer_size(this->ha_thd());
  alloc_buffers(this->write_buffer_size);
  this->record_index = 0;
  // Reset buffer sizes to 0 for writes
  // We increase the size for every cell/row we are given to write
  for (auto &buff : this->buffers) {
    buff->buffer_size = 0;
    buff->offset_buffer_size = 0;
  }
  // Reset bitmap to original
  tmp_restore_column_map(table->read_set, original_bitmap);
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
      flush_write();
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
    rc = -301;
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR,
                    "[finalize_write] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = -302;
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

    // Handle case where flush was called but no data was written
    if (coord_buffer->buffer_size != 0) {

      uint64_t coord_size = 0;
      for (auto &buff : this->buffers) {
        if (this->array_schema->domain().has_dimension(buff->name)) {
          coord_size = buff->buffer_size * buff->fixed_size_elements;
          this->ctx.handle_error(tiledb_query_set_buffer(
              this->ctx.ptr().get(), this->query->ptr().get(), tiledb_coords(),
              buff->buffer, &coord_size));
        } else {
          if (buff->offset_buffer != nullptr) {
            this->ctx.handle_error(tiledb_query_set_buffer_var(
                this->ctx.ptr().get(), this->query->ptr().get(),
                buff->name.c_str(), buff->offset_buffer,
                &buff->offset_buffer_size, buff->buffer, &buff->buffer_size));
          } else {
            this->ctx.handle_error(tiledb_query_set_buffer(
                this->ctx.ptr().get(), this->query->ptr().get(),
                buff->name.c_str(), buff->buffer, &buff->buffer_size));
          }
        }
      }

      // Only submit the query if there is actual data, else just carry on
      if (coord_size > 0) {
        query->submit();
      }
    }

    // After query submit reset buffer sizes
    this->record_index = 0;
    // Reset buffer sizes to 0 for writes
    // We increase the size for every cell/row we are given to write
    for (auto &buff : this->buffers) {
      buff->buffer_size = 0;
      buff->offset_buffer_size = 0;
    }
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[flush_write] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = -311;
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[flush_write] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = -312;
  }

  DBUG_RETURN(rc);
}

int tile::mytile::write_row(const uchar *buf) {
  DBUG_ENTER("tile::mytile::write_row");
  int rc = 0;
  // We must set the bitmap for debug purpose, it is "read_set" because we use
  // Field->val_*
  my_bitmap_map *original_bitmap = tmp_use_all_columns(table, table->read_set);

  if (!this->bulk_write) {
    setup_write();
  }

  try {
    rc = mysql_row_to_tiledb_buffers(buf);
    if (rc == ERR_WRITE_FLUSH_NEEDED) {
      flush_write();

      // Reset bitmap to original
      tmp_restore_column_map(table->read_set, original_bitmap);
      DBUG_RETURN(write_row(buf));
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
    rc = -201;
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[write_row] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = -202;
  }

  // Reset bitmap to original
  tmp_restore_column_map(table->read_set, original_bitmap);
  DBUG_RETURN(rc);
}

ulong tile::mytile::index_flags(uint idx, uint part, bool all_parts) const {
  DBUG_ENTER("tile::mytile::index_flags");
  DBUG_RETURN(HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER | HA_READ_RANGE |
              HA_KEYREAD_ONLY | HA_DO_RANGE_FILTER_PUSHDOWN |
              HA_DO_INDEX_COND_PUSHDOWN);
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

    if (!compare_configs(cfg, this->config)) {
      this->config = cfg;
      this->ctx = build_context(this->config);
    }
    if (this->table->s->option_struct->open_at != UINT64_MAX) {
      this->array = std::make_shared<tiledb::Array>(
          this->ctx, this->uri, TILEDB_READ,
          encryption_key.empty() ? TILEDB_NO_ENCRYPTION : TILEDB_AES_256_GCM,
          encryption_key, this->table->s->option_struct->open_at);
    } else {
      this->array = std::make_shared<tiledb::Array>(
          this->ctx, this->uri, TILEDB_READ,
          encryption_key.empty() ? TILEDB_NO_ENCRYPTION : TILEDB_AES_256_GCM,
          encryption_key);
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
  // If the layout is unordered by the the array type is dense we will set it to
  // the tile order instead Currently multi-range queries do not support dense
  // unordered reads
  if (this->array_schema->array_type() == tiledb_array_type_t::TILEDB_DENSE &&
      query_layout == tiledb_layout_t::TILEDB_UNORDERED) {
    this->query->set_layout(this->array_schema->tile_order());
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

    if (!compare_configs(cfg, this->config)) {
      this->config = cfg;
      this->ctx = build_context(this->config);
    }
    this->array = std::make_shared<tiledb::Array>(
        this->ctx, this->uri, TILEDB_WRITE,
        encryption_key.empty() ? TILEDB_NO_ENCRYPTION : TILEDB_AES_256_GCM,
        encryption_key);
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

  this->query->set_layout(tiledb_layout_t::TILEDB_UNORDERED);
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
  int rc = init_scan(
      this->ha_thd(),
      std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free));

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
  DBUG_ENTER("tile::mytile::index_read_map");
  // If no conditions or index have been pushed, use key scan info to push a
  // range down
  if ((!this->valid_pushed_ranges() && !this->valid_pushed_in_ranges()) ||
      this->query_complete()) {
    this->reset_pushdowns_for_key(key, key_len, find_flag);
    int rc = init_scan(
        this->ha_thd(),
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free));

    if (rc)
      DBUG_RETURN(rc);

    // Index scans are expected in row major order
    this->query->set_layout(tiledb_layout_t::TILEDB_ROW_MAJOR);
  }
  DBUG_RETURN(index_read_scan(key, key_len, find_flag));
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

int tile::mytile::index_read_scan(const uchar *key, uint key_len,
                                  enum ha_rkey_function find_flag) {
  DBUG_ENTER("tile::mytile::index_read_scan");
  int rc = 0;
  const char *query_status;

  // If the query is empty, we should abort early
  // We have to check this here and not in rnd_init because
  // only index_read_scan can return empty
  if (this->empty_read) {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  // We must set the bitmap for debug purpose, it is "write_set" because we use
  // Field->store
  my_bitmap_map *original_bitmap =
      dbug_tmp_use_all_columns(table, table->write_set);
  tiledb_query_status_to_str(static_cast<tiledb_query_status_t>(status),
                             &query_status);

  bool restarted_scan = false;
begin:
  if (this->query_complete()) {
    // Reset bitmap to original
    dbug_tmp_restore_column_map(table->write_set, original_bitmap);
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  try {
    // If the cursor has passed the number of records from the previous query
    // (or if this is the first time), (re)submit the query
    if (static_cast<int64_t>(this->record_index) >= this->records) {
      do {
        this->status = query->submit();

        // Compute the number of cells (records) that were returned by the query
        this->records = this->coord_buffer->buffer_size / this->ndim /
                        tiledb_datatype_size(this->coord_buffer->type);

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
          // Handle when the query doesn't return results by exiting early
        } else if (this->records == 0 &&
                   status == tiledb::Query::Status::COMPLETE) {
          // Reset bitmap to original
          dbug_tmp_restore_column_map(table->write_set, original_bitmap);
          DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
        }
      } while (status == tiledb::Query::Status::INCOMPLETE);
    }

    // Compare keys to see if we are in the proper position
    uint64_t size = tiledb_datatype_size(this->coord_buffer->type);

    do {
      int key_cmp = tile::compare_typed_buffers(
          key,
          static_cast<char *>(this->coord_buffer->buffer) +
              (this->coord_buffer->fixed_size_elements * size *
               this->record_index),
          key_len, this->coord_buffer->type);
      // If the current index coordinates matches the key we are looking for we
      // must set the fields
      if ((key_cmp == 0 &&
           (find_flag == ha_rkey_function::HA_READ_KEY_EXACT ||
            find_flag == ha_rkey_function::HA_READ_KEY_OR_NEXT ||
            find_flag == ha_rkey_function::HA_READ_KEY_OR_PREV)) ||
          (key_cmp < 0 &&
           (find_flag == ha_rkey_function::HA_READ_BEFORE_KEY ||
            find_flag == ha_rkey_function::HA_READ_KEY_OR_PREV)) ||
          (key_cmp > 0 &&
           (find_flag == ha_rkey_function::HA_READ_AFTER_KEY ||
            find_flag == ha_rkey_function::HA_READ_KEY_OR_NEXT))) {
        tileToFields(record_index, false, table);
        this->record_index++;
        this->records_read++;
        break;
        // If we have passed the record we must start back at the top of the
        // current incomplete batch
      } else if (key_cmp < 0) {

        // First lets make sure the "start" record of this (potentially
        // incomplete) query buffers is before what we are looking for If the
        // key we are looking for is before the first record it means we've
        // missed the key and the request can not be resolved
        if (tile::compare_typed_buffers(key, this->coord_buffer->buffer,
                                        key_len,
                                        this->coord_buffer->type) < 0) {

          // If we are at the start of the query this means this key doesn't
          // exist in the query
          if (this->records_read == this->record_index) {
            // Reset bitmap to original
            dbug_tmp_restore_column_map(table->write_set, original_bitmap);
            DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
          }

          // If we have already tried rerunning the query from the start then
          // the request can not be satisifed we need to return key not found to
          // prevent a infinit loop
          if (restarted_scan) {
            // Reset bitmap to original
            dbug_tmp_restore_column_map(table->write_set, original_bitmap);
            DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
          }

          // rerunning incomplete query from the beginning
          rc = init_scan(
              this->ha_thd(),
              std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free));
          // Index scans are expected in row major order
          this->query->set_layout(tiledb_layout_t::TILEDB_ROW_MAJOR);
          restarted_scan = true;
          goto begin;
        }

        // As an optimization instead of going back to the top let's scan in
        // reverse. Often MariaDB will call index_next to see if the next record
        // is of the same key or not This causes use to move forward one and we
        // end up past the key by the time we get to this function So instead of
        // starting at the top let's just go in reverse and maybe we'll only
        // have to reverse one position
        this->records_read--;
        this->record_index--;

        // Now that we have moved back one we need to do a quick check to make
        // sure that the new position is not before the record. This covers the
        // case where a key does not exist in the result set but we might get
        // stuck in a loop bouncing between two coordinates which are before and
        // after the non-existent key.
        if (tile::compare_typed_buffers(
                key,
                static_cast<char *>(this->coord_buffer->buffer) +
                    (this->coord_buffer->fixed_size_elements * size *
                     this->record_index),
                key_len, this->coord_buffer->type) > 0) {
          // Reset bitmap to original
          dbug_tmp_restore_column_map(table->write_set, original_bitmap);
          DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
        }

        // If we are not yet to the record we must continue scanning.
      } else if (key_cmp > 0) {
        this->record_index++;
        this->records_read++;
      }

      // If we have run out of records but the index isn't found, move to the
      // next incomplete batch We do this by going back to the start of the
      // function so we trigger the next batch call We can't use recursion
      // because DEBUG_ENTER has a limit to the depth in which it can be called
      if (static_cast<int64_t>(this->record_index) >= this->records) {
        goto begin;
      }
    } while (static_cast<int64_t>(this->record_index) < this->records);

  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR,
                    "[index_read_scan] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = -131;
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR,
                    "[index_read_scan] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = -132;
  }

  // Reset bitmap to original
  dbug_tmp_restore_column_map(table->write_set, original_bitmap);
  DBUG_RETURN(rc);
}

int tile::mytile::index_read_idx_map(uchar *buf, uint idx, const uchar *key,
                                     key_part_map keypart_map,
                                     enum ha_rkey_function find_flag) {
  DBUG_ENTER("tile::mytile::index_read_idx_map");

  uint key_len = calculate_key_len(table, idx, key, keypart_map);

  // If no conditions or index have been pushed, use key scan info to push a
  // range down
  if (!this->valid_pushed_ranges() && !this->valid_pushed_in_ranges())
    this->reset_pushdowns_for_key(key, key_len, find_flag);

  // If we are doing an index scan we need to use row-major order to get the
  // results in the expected order

  int rc = init_scan(
      this->ha_thd(),
      std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free));
  if (rc)
    DBUG_RETURN(rc);

  this->query->set_layout(tiledb_layout_t::TILEDB_ROW_MAJOR);

  DBUG_RETURN(index_read_scan(key, key_len, find_flag));
}

ha_rows tile::mytile::records_in_range(uint inx, key_range *min_key,
                                       key_range *max_key) {
  return (ha_rows)10000;
}

int tile::mytile::reset_pushdowns_for_key(const uchar *key, uint key_len,
                                          enum ha_rkey_function find_flag) {
  DBUG_ENTER("tile::mytile::reset_pushdowns_for_key");
  std::vector<std::shared_ptr<tile::range>> ranges_from_keys =
      tile::build_ranges_from_key(key, key_len, find_flag,
                                  this->array_schema->domain().type());

  if (!ranges_from_keys.empty()) {
    this->pushdown_ranges.clear();
    this->pushdown_in_ranges.clear();

    this->pushdown_ranges.resize(this->ndim);
    this->pushdown_in_ranges.resize(this->ndim);

    // Copy shared pointer to main range pushdown
    for (uint64_t i = 0; i < ranges_from_keys.size(); i++) {
      this->pushdown_ranges[i].push_back(ranges_from_keys[i]);
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

  std::vector<std::vector<std::shared_ptr<tile::range>>> tmp_ranges;
  this->pushdown_ranges.resize(this->ndim);
  this->pushdown_in_ranges.resize(this->ndim);
  tmp_ranges.resize(this->ndim);

  tiledb_datatype_t datatype = this->array_schema->domain().type();
  uint64_t datatype_size = tiledb_datatype_size(datatype);

  std::vector<std::shared_ptr<tile::range>> ranges_from_key_start;
  std::vector<std::shared_ptr<tile::range>> ranges_from_key_end;
  // get first key
  mrr_funcs.next(mrr_iter, &mrr_cur_range);
  if (mrr_cur_range.start_key.key != nullptr) {
    ranges_from_key_start = tile::build_ranges_from_key(
        mrr_cur_range.start_key.key, mrr_cur_range.start_key.length,
        HA_READ_KEY_OR_NEXT, this->array_schema->domain().type());
  }
  if (mrr_cur_range.end_key.key != nullptr) {
    ranges_from_key_end = tile::build_ranges_from_key(
        mrr_cur_range.end_key.key, mrr_cur_range.end_key.length,
        HA_READ_KEY_OR_PREV, this->array_schema->domain().type());
  }

  for (uint64_t i = 0; i < ranges_from_key_start.size(); i++) {
    std::vector<std::shared_ptr<tile::range>> combined_ranges_for_dimension = {
        ranges_from_key_start[i]};
    if (ranges_from_key_end.size() > i) {
      combined_ranges_for_dimension.push_back(ranges_from_key_end[i]);
    }

    auto merged_range = merge_ranges(combined_ranges_for_dimension,
                                     this->array_schema->domain().type());
    tmp_ranges[i].push_back(merged_range);
  }

  // Loop over all keys
  while (!mrr_funcs.next(mrr_iter, &mrr_cur_range)) {

    if (mrr_cur_range.start_key.key != nullptr) {
      for (uint64_t i = 0; i < mrr_cur_range.start_key.length / datatype_size;
           i++) {
        auto &range = tmp_ranges[i][0];
        update_range_from_key_for_super_range(range, mrr_cur_range.start_key, i,
                                              datatype);
      }
    }

    // If the end key is not the same as the start key we need to also build the
    // range from it
    if (mrr_cur_range.end_key.key != nullptr &&
        (mrr_cur_range.start_key.length != mrr_cur_range.end_key.length ||
         memcmp(mrr_cur_range.start_key.key, mrr_cur_range.end_key.key,
                mrr_cur_range.start_key.length) != 0)) {
      for (uint64_t i = 0; i < mrr_cur_range.end_key.length / datatype_size;
           i++) {
        auto range = tmp_ranges[i][0];
        update_range_from_key_for_super_range(range, mrr_cur_range.start_key, i,
                                              datatype);
      }
    }
  }

  // Now that we have all ranges, let's build them into super ranges
  for (size_t i = 0; i < tmp_ranges.size(); i++) {
    const auto &ranges = tmp_ranges[i];
    auto merged_range =
        merge_ranges_to_super(ranges, this->array_schema->domain().type());
    if (merged_range != nullptr) {
      this->pushdown_ranges[i].push_back(std::move(merged_range));
    }
  }

  int rc = init_scan(
      this->ha_thd(),
      std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free));
  if (rc)
    DBUG_RETURN(rc);

  this->query->set_layout(tiledb_layout_t::TILEDB_ROW_MAJOR);
  DBUG_RETURN(rc);
}

int tile::mytile::multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param,
                                        uint n_ranges, uint mode,
                                        HANDLER_BUFFER *buf) {

  DBUG_ENTER("tile::mytile::multi_range_read_init");
  mrr_iter = seq->init(seq_init_param, n_ranges, mode);
  mrr_funcs = *seq;

  build_mrr_ranges();

  // Never use default implementation also use sort key access
  mode &= ~HA_MRR_USE_DEFAULT_IMPL;
  DBUG_RETURN(
      ds_mrr.dsmrr_init(this, seq, seq_init_param, n_ranges, mode, buf));
}

int tile::mytile::multi_range_read_next(range_id_t *range_info) {
  DBUG_ENTER("tile::mytile::multi_range_read_next");
  int res = ds_mrr.dsmrr_next(range_info);
  DBUG_RETURN(res);
}

ha_rows tile::mytile::multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq,
                                                  void *seq_init_param,
                                                  uint n_ranges, uint *bufsz,
                                                  uint *flags,
                                                  Cost_estimate *cost) {
  /*
    This call is here because there is no location where this->table would
    already be known.
    TODO: consider moving it into some per-query initialization call.
  */
  ds_mrr.init(this, table);
  *flags &= ~HA_MRR_USE_DEFAULT_IMPL;
  bool rc = ds_mrr.dsmrr_info_const(keyno, seq, seq_init_param, n_ranges, bufsz,
                                    flags, cost);

  *flags &= ~HA_MRR_USE_DEFAULT_IMPL;
  return rc;
}

ha_rows tile::mytile::multi_range_read_info(uint keyno, uint n_ranges,
                                            uint keys, uint key_parts,
                                            uint *bufsz, uint *flags,
                                            Cost_estimate *cost) {
  ds_mrr.init(this, table);
  *flags &= ~HA_MRR_USE_DEFAULT_IMPL;
  bool rc =
      ds_mrr.dsmrr_info(keyno, n_ranges, keys, key_parts, bufsz, flags, cost);
  *flags &= ~HA_MRR_USE_DEFAULT_IMPL;
  return rc;
}

int tile::mytile::multi_range_read_explain_info(uint mrr_mode, char *str,
                                                size_t size) {
  mrr_mode &= ~HA_MRR_USE_DEFAULT_IMPL;
  return ds_mrr.dsmrr_explain_info(mrr_mode, str, size);
}
/* MyISAM MRR implementation ends */

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
    0x0003,                     /* version number (0.3) */
    NULL,                       /* status variables */
    tile::sysvars::mytile_system_variables, /* system variables */
    NULL,                                   /* config options */
    0,                                      /* flags */
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
    0x0003,                     /* version number (0.3) */
    NULL,                       /* status variables */
    tile::sysvars::mytile_system_variables, /* system variables */
    "0.3",                                  /* string version */
    MariaDB_PLUGIN_MATURITY_EXPERIMENTAL    /* maturity */
} maria_declare_plugin_end;
