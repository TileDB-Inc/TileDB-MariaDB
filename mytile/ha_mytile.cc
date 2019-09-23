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
#include <cstring>
#include <log.h>
#include <my_config.h>
#include <mysql/plugin.h>
#include <mysqld_error.h>
#include <vector>
#include <unordered_map>

// Handler for mytile engine
handlerton *mytile_hton;

/**
  @brief
  Function we use in the creation of our hash to get key.
*/

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key ex_key_mutex_mytile_share_mutex;

static PSI_mutex_info all_mytile_mutexes[] = {
    {&ex_key_mutex_mytile_share_mutex, "mytile_share::mutex", 0}};

static void init_mytile_psi_keys() {
  const char *category = "mytile";
  int count;

  count = array_elements(all_mytile_mutexes);
  mysql_mutex_register(category, all_mytile_mutexes, count);
}

#endif

// system variables
struct st_mysql_sys_var *mytile_system_variables[] = {
    MYSQL_SYSVAR(read_buffer_size), MYSQL_SYSVAR(write_buffer_size), NULL};

// Structure for table options
ha_create_table_option mytile_table_option_list[] = {
    HA_TOPTION_STRING("uri", array_uri),
    HA_TOPTION_ENUM("array_type", array_type, "DENSE,SPARSE", 1),
    HA_TOPTION_NUMBER("capacity", capacity, 10000, 0, UINT64_MAX, 1),
    HA_TOPTION_ENUM("cell_order", cell_order, "ROW_MAJOR,COLUMN_MAJOR",
                    TILEDB_ROW_MAJOR),
    HA_TOPTION_ENUM("tile_order", tile_order, "ROW_MAJOR,COLUMN_MAJOR",
                    TILEDB_ROW_MAJOR),
    HA_TOPTION_END};

// Structure for specific field options
ha_create_table_option mytile_field_option_list[] = {
    /*
      If the engine wants something more complex than a string, number, enum,
      or boolean - for example a list - it needs to specify the option
      as a string and parse it internally.
    */
    HA_FOPTION_BOOL("dimension", dimension, false),
    HA_FOPTION_STRING("lower_bound", lower_bound),
    HA_FOPTION_STRING("upper_bound", upper_bound),
    HA_FOPTION_STRING("tile_extent", tile_extent), HA_FOPTION_END};

/**
 * Basic lock structure
 */
tile::mytile_share::mytile_share() {
  thr_lock_init(&lock);
  mysql_mutex_init(ex_key_mutex_mytile_share_mutex, &mutex, MY_MUTEX_INIT_FAST);
}

/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each mytile handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

tile::mytile_share *tile::mytile::get_share() {
  tile::mytile_share *tmp_share;

  DBUG_ENTER("tile::mytile::get_share()");

  lock_shared_ha_data();
  if (!(tmp_share = static_cast<tile::mytile_share *>(get_ha_share_ptr()))) {
    tmp_share = new mytile_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share *>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  DBUG_RETURN(tmp_share);
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

#ifdef HAVE_PSI_INTERFACE
  // Initialize performance schema keys
  init_mytile_psi_keys();
#endif

  mytile_hton = static_cast<handlerton *>(p);
  mytile_hton->state = SHOW_OPTION_YES;
  mytile_hton->create = mytile_create_handler;
  mytile_hton->tablefile_extensions = mytile_exts;
  mytile_hton->table_options = mytile_table_option_list;
  mytile_hton->field_options = mytile_field_option_list;
  // Set table discovery functions
  mytile_hton->discover_table = tile::mytile_discover_table;
  mytile_hton->discover_table_existence = tile::mytile_discover_table_existence;

  DBUG_RETURN(0);
}

// Storage engine interface
struct st_mysql_storage_engine mytile_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

/**
 * Store a lock, we aren't using table or row locking at this point.
 * @param thd
 * @param to
 * @param lock_type
 * @return
 */
THR_LOCK_DATA **tile::mytile::store_lock(THD *thd, THR_LOCK_DATA **to,
                                         enum thr_lock_type lock_type) {
  DBUG_ENTER("tile::mytile::store_lock");
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type = lock_type;
  *to++ = &lock;
  DBUG_RETURN(to);
};

/**
 * Not implemented until transaction support added
 * @param thd
 * @param lock_type
 * @return
 */
int tile::mytile::external_lock(THD *thd, int lock_type) {
  DBUG_ENTER("tile::mytile::external_lock");
  DBUG_RETURN(0);
}

/**
 * Main handler
 * @param hton
 * @param table_arg
 */
tile::mytile::mytile(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg) {
  config = tiledb::Config();
  ctx = tiledb::Context(config);
};

/**
 * Create a table structure and TileDB array schema
 * @param name
 * @param table_arg
 * @param create_info
 * @return
 */
int tile::mytile::create(const char *name, TABLE *table_arg,
                         HA_CREATE_INFO *create_info) {
  DBUG_ENTER("tile::mytile::create");
  DBUG_RETURN(create_array(name, table_arg, create_info, ctx));
}

/**
 * Open array
 * @param name
 * @param mode
 * @param test_if_locked
 * @return
 */
int tile::mytile::open(const char *name, int mode, uint test_if_locked) {
  DBUG_ENTER("tile::mytile::open");

  // We are suppose to get a lock here, but tiledb doesn't require locks.
  if (!(share = get_share()))
    DBUG_RETURN(1);
  thr_lock_data_init(&share->lock, &lock, nullptr);

  // Open TileDB Array
  try {
    uri = name;
    if (this->table->s->option_struct->array_uri != nullptr)
      uri = this->table->s->option_struct->array_uri;

    // Always open in read only mode, we'll re-open for writes if we hit init
    // write
    tiledb_query_type_t openType = tiledb_query_type_t::TILEDB_READ;

    //    if (mode == O_RDWR)
    //      openType = tiledb_query_type_t::TILEDB_WRITE;

    this->array = std::make_shared<tiledb::Array>(this->ctx, uri, openType);

    auto domain = this->array->schema().domain();
    this->ndim = domain.ndim();

    // Set ref length used for storing reference in position(), this is the size
    // of a subarray for querying
    this->ref_length = (this->ndim * tiledb_datatype_size(domain.type()) * 2);

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

/**
 * Close array
 * @return
 */
int tile::mytile::close(void) {
  DBUG_ENTER("tile::mytile::close");
  try {
    this->array->close();
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

/**
 * Creates the actual tiledb array
 * @param name
 * @param table_arg
 * @param create_info
 * @param ctx
 * @return
 */
int tile::mytile::create_array(const char *name, TABLE *table_arg,
                               HA_CREATE_INFO *create_info,
                               tiledb::Context ctx) {
  DBUG_ENTER("tile::create_array");
  int rc = 0;

  tiledb_array_type_t arrayType = TILEDB_SPARSE;
  if (create_info->option_struct->array_type == 1) {
    arrayType = TILEDB_SPARSE;
  } else {
    arrayType = TILEDB_DENSE;
  }

  // Create array schema
  tiledb::ArraySchema schema(ctx, arrayType);

  // Create domain
  tiledb::Domain domain(ctx);

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
      // Onyl tile extent is checked because for the dimension domain we use the
      // datatypes min/max values
      if (field->option_struct->tile_extent == nullptr ||
          strcmp(field->option_struct->tile_extent, "") == 0) {
        my_printf_error(
            ER_UNKNOWN_ERROR,
            "Dimension field %s tile_extent was not set, can not create table",
            ME_ERROR_LOG | ME_FATAL, field->field_name);
        DBUG_RETURN(-13);
      }
      domain.add_dimension(create_field_dimension(ctx, field));
    } else { // Else this is treated as a dimension
      // Currently hard code the filter list to zstd compression
      tiledb::FilterList filterList(ctx);
      tiledb::Filter filter(ctx, TILEDB_FILTER_ZSTD);
      filterList.add_filter(filter);
      tiledb::Attribute attr = create_field_attribute(ctx, field, filterList);
      schema.add_attribute(attr);
    };
  }

  schema.set_domain(domain);

  // Set capacity
  schema.set_capacity(create_info->option_struct->capacity);

  // Set cell ordering if configured
  if (create_info->option_struct->cell_order == 0) {
    schema.set_cell_order(TILEDB_ROW_MAJOR);
  } else if (create_info->option_struct->cell_order == 1) {
    schema.set_cell_order(TILEDB_COL_MAJOR);
  }

  // Set tile ordering if configured
  if (create_info->option_struct->tile_order == 0) {
    schema.set_tile_order(TILEDB_ROW_MAJOR);
  } else if (create_info->option_struct->tile_order == 1) {
    schema.set_tile_order(TILEDB_COL_MAJOR);
  }

  // Get array uri from name or table option
  std::string create_uri = name;
  if (create_info->option_struct->array_uri != nullptr)
    create_uri = create_info->option_struct->array_uri;
  else
    create_info->option_struct->array_uri = const_cast<char *>(name);

  // Check array schema
  try {
    schema.check();
  } catch (tiledb::TileDBError &e) {
    my_printf_error(ER_UNKNOWN_ERROR, "Error in building schema %s",
                    ME_ERROR_LOG | ME_FATAL, e.what());
    DBUG_RETURN(-10);
  }

  // Create the array on storage
  tiledb::Array::create(create_uri, schema);
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

  this->read_buffer_size = THDVAR(thd, read_buffer_size);

  try {
    if ((this->array->is_open() && this->array->query_type() != TILEDB_READ) ||
        !this->array->is_open()) {
      if (this->array->is_open())
        this->array->close();

      this->array->open(TILEDB_READ);
    }
    if (this->query == nullptr || this->query->query_type() != TILEDB_READ) {
      this->query =
          std::make_unique<tiledb::Query>(this->ctx, *this->array, TILEDB_READ);
      this->query->set_layout(tiledb_layout_t::TILEDB_UNORDERED);
    }

    alloc_read_buffers(this->read_buffer_size);

    auto domain = this->array->schema().domain();
    auto dims = domain.dimensions();

    uint64_t subarray_size =
        tiledb_datatype_size(domain.type()) * dims.size() * 2;
    if (subarray == nullptr) {
      subarray = std::unique_ptr<void, decltype(&std::free)>(
          std::malloc(subarray_size), &std::free);
    }
    int empty;

    ctx.handle_error(tiledb_array_get_non_empty_domain(
        ctx.ptr().get(), this->array->ptr().get(), subarray.get(), &empty));

    this->total_num_records_UB = computeRecordsUB(this->array, subarray.get());
    if (this->pushdown_ranges.empty()) { // No pushdown
      if (empty)
        DBUG_RETURN(HA_ERR_END_OF_FILE);

      sql_print_information("no pushdowns possible for query");

      // Set subarray using capi
      ctx.handle_error(tiledb_query_set_subarray(
          ctx.ptr().get(), this->query->ptr().get(), subarray.get()));

    } else {
      for (uint64_t dim_idx = 0; dim_idx < this->ndim; dim_idx++) {
        const auto &ranges = this->pushdown_ranges[dim_idx];
        void *lower = static_cast<char *>(subarray.get()) +
                      (dim_idx * 2 * tiledb_datatype_size(domain.type()));

        if (!ranges.empty()) {
          sql_print_information("Pushdown for %s",
                                dims[dim_idx].name().c_str());
          for (const std::shared_ptr<range> &range : ranges) {
            setup_range(range, lower, dims[dim_idx]);

            ctx.handle_error(tiledb_query_add_range(
                ctx.ptr().get(), this->query->ptr().get(), dim_idx,
                range->lower_value.get(), range->upper_value.get(), nullptr));
          }
        } else { // If the range is empty we need to use the non-empty-domain

          void *upper =
              static_cast<char *>(lower) + tiledb_datatype_size(domain.type());
          ctx.handle_error(
              tiledb_query_add_range(ctx.ptr().get(), this->query->ptr().get(),
                                     dim_idx, lower, upper, nullptr));
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

/* Table Scanning */
int tile::mytile::rnd_init(bool scan) {
  DBUG_ENTER("tile::mytile::rnd_init");
  DBUG_RETURN(init_scan(
      this->ha_thd(),
      std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free)));
};

int tile::mytile::rnd_row(TABLE *table) {
  DBUG_ENTER("tile::mytile::rnd_row");
  int rc = 0;
  const char *query_status;
  tiledb_query_status_to_str(static_cast<tiledb_query_status_t>(status),
                             &query_status);

  // If we have run out of records report EOF
  // note the upper bound of records might be *more* than actual results, thus
  // this check is not guaranteed see the next check were we look for complete
  // query and row position
  if (this->records_read >= this->total_num_records_UB) {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  // If we are complete and there is no more records we report EOF
  if (this->status == tiledb::Query::Status::COMPLETE &&
      static_cast<int64_t>(this->record_index) >= this->records) {
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
        }
      } while (status == tiledb::Query::Status::INCOMPLETE);
    }

    tileToFields(record_index, false, table);

    this->record_index++;
    this->records_read++;

  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[rnd_row] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = -121;
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR, "[rnd_row] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = -122;
  }
  DBUG_RETURN(rc);
}

/**
 * Read next row
 * @param buf
 * @return
 */
int tile::mytile::rnd_next(uchar *buf) {
  DBUG_ENTER("tile::mytile::rnd_next");
  DBUG_RETURN(rnd_row(table));
}

/**
 * End read
 * @return
 */
int tile::mytile::rnd_end() {
  DBUG_ENTER("tile::mytile::rnd_end");
  dealloc_buffers();
  this->pushdown_ranges.clear();
  // Reset indicators
  this->record_index = 0;
  this->records = 0;
  this->records_read = 0;
  this->status = tiledb::Query::Status::UNINITIALIZED;
  this->total_num_records_UB = 0;
  this->query.reset();
  DBUG_RETURN(0);
};

/**
 * Read position
 * Will this ever be interleaved with table scans? I am not sure, need to ask
 * MariaDB We assume it wont :/
 * @param buf
 * @param pos
 * @return
 */
int tile::mytile::rnd_pos(uchar *buf, uchar *pos) {
  DBUG_ENTER("tile::mytile::rnd_pos");
  auto domain = this->array->schema().domain();
  ctx.handle_error(tiledb_query_set_subarray(ctx.ptr().get(),
                                             this->query->ptr().get(), pos));
  this->total_num_records_UB = computeRecordsUB(this->array, pos);

  // Reset indicators
  this->record_index = 0;
  this->records = 0;
  this->records_read = 0;
  this->status = tiledb::Query::Status::UNINITIALIZED;
  DBUG_RETURN(rnd_next(buf));
}

/**
 * Get current record coordinates and save to allow for later lookup
 * @param record
 */
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
  default:
    DBUG_RETURN(nullptr);
  } // endswitch functype

  List<Item> *arglist = cond_item->argument_list();
  List_iterator<Item> li(*arglist);
  const Item *subitem;

  for (uint32_t i = 0; i < arglist->elements; i++) {
    if ((subitem = li++)) {
      // COND_ITEMs
      cond_push(dynamic_cast<const COND *>(subitem));
    }
  }
  DBUG_RETURN(nullptr);
}
/**
 *  Handle func condition pushdowns
 * @param func_item
 * @return
 */
const COND *tile::mytile::cond_push_func(const Item_func *func_item) {
  DBUG_ENTER("tile::mytile::cond_push_func");
  Item **args = func_item->arguments();
  bool neg = FALSE;

  Item_field *column_field = dynamic_cast<Item_field *>(args[0]);
  // If the condition is not a dimension we can't handle it
  if (!this->array->schema().domain().has_dimension(
          column_field->field_name.str)) {
    DBUG_RETURN(func_item);
  }

  uint64_t dim_idx = 0;
  auto dims = this->array->schema().domain().dimensions();
  for (uint64_t j = 0; j < this->ndim; j++) {
    if (dims[j].name() == column_field->field_name.str)
      dim_idx = j;
  }

  switch (func_item->functype()) {
  case Item_func::NE_FUNC:
    DBUG_RETURN(func_item); /* Not equal is not supported */
  case Item_func::IN_FUNC:
    // In is special because we need to do a tiledb range per argument
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
          func_item->functype(), tiledb_datatype_t::TILEDB_ANY});
      int ret = set_range_from_item_consts(lower_const, upper_const, range);

      if (ret)
        DBUG_RETURN(func_item);

      // Add the range to the pushdown ranges
      auto &range_vec = this->pushdown_ranges[dim_idx];
      range_vec.push_back(std::move(range));
    }

    break;
  case Item_func::BETWEEN:
    neg = (dynamic_cast<const Item_func_opt_neg *>(func_item))->negated;
    if (neg) // don't support negations!
      DBUG_RETURN(func_item);
    // fall through
  default: // Handle all cases where there is 1 or 2 arguments we must set on
    // the range

    Item_basic_constant *lower_const =
        dynamic_cast<Item_basic_constant *>(args[1]);
    // Init upper to be lower
    Item_basic_constant *upper_const =
        dynamic_cast<Item_basic_constant *>(args[1]);

    if (func_item->argument_count() == 3) {
      upper_const = dynamic_cast<Item_basic_constant *>(args[2]);
    }

    // Create unique ptrs
    std::shared_ptr<range> range = std::make_shared<tile::range>(tile::range{
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free),
        func_item->functype(), tiledb_datatype_t::TILEDB_ANY});

    int ret = set_range_from_item_consts(lower_const, upper_const, range);

    if (ret)
      DBUG_RETURN(func_item);

    // Add the range to the pushdown ranges
    auto &range_vec = this->pushdown_ranges[dim_idx];
    range_vec.push_back(std::move(range));

    break;
  } // endswitch functype
  DBUG_RETURN(nullptr);
}

/**
  Push condition down to the table handler.

  @param  cond   Condition to be pushed. The condition tree must not be
                 modified by the by the caller.

  @return
    The 'remainder' condition that caller must use to filter out records.
    NULL means the handler will not return rows that do not match the
    passed condition.

  @note
  The pushed conditions form a stack (from which one can remove the
  last pushed condition using cond_pop).
  The table handler filters out rows using (pushed_cond1 AND pushed_cond2
  AND ... AND pushed_condN)
  or less restrictive condition, depending on handler's capabilities.

  handler->ha_reset() call empties the condition stack.
  Calls to rnd_init/rnd_end, index_init/index_end etc do not affect the
  condition stack.
*/
const COND *tile::mytile::cond_push(const COND *cond) {
  DBUG_ENTER("tile::mytile::cond_push");
  // NOTE: This is called one or more times by handle interface. Once for each
  // condition

  // Make sure pushdown ranges is not empty
  if (this->pushdown_ranges.empty())
    for (uint64_t i = 0; i < this->ndim; i++)
      this->pushdown_ranges.emplace_back();

  switch (cond->type()) {
  case Item::COND_ITEM: {
    Item_cond *cond_item = dynamic_cast<Item_cond *>(const_cast<COND *>(cond));
    const COND *ret = cond_push_cond(cond_item);
    if (ret != nullptr) {
      DBUG_RETURN(ret);
    }
    break;
  }
  case Item::FUNC_ITEM: {
    const Item_func *func_item = dynamic_cast<const Item_func *>(cond);
    const COND *ret = cond_push_func(func_item);
    if (ret != nullptr) {
      DBUG_RETURN(ret);
    }
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
};

/**
  Pop the top condition from the condition stack of the storage engine
  for each partition.
*/

void tile::mytile::cond_pop() {
  DBUG_ENTER("tile::mytile::cond_pop");

  DBUG_VOID_RETURN;
}

/**
 * Delete a table by rm'ing the tiledb directory
 * @param name
 * @return
 */
int tile::mytile::delete_table(const char *name) {
  DBUG_ENTER("tile::mytile::delete_table");
  // Delete dir
  try {
    tiledb::VFS vfs(ctx);
    vfs.remove_dir(name);
  } catch (const tiledb::TileDBError &e) {
    // Log errors
    sql_print_error("delete_table error for table %s : %s", name, e.what());
    DBUG_RETURN(-20);
  } catch (const std::exception &e) {
    // Log errors
    sql_print_error("delete_table error for table %s : %s", name, e.what());
    DBUG_RETURN(-21);
  }
  DBUG_RETURN(0);
}

/**
 * This should return relevant stats of the underlying tiledb map,
 * currently just sets row count to 2, to avoid 0/1 row optimizations
 * @return
 */
int tile::mytile::info(uint) {
  DBUG_ENTER("tile::mytile::info");
  // Need records to be greater than 1 to avoid 0/1 row optimizations by query
  // optimizer
  stats.records = 2;
  DBUG_RETURN(0);
};

/**
 * Flags for table features supported
 * @return
 */
ulonglong tile::mytile::table_flags(void) const {
  DBUG_ENTER("tile::mytile::table_flags");
  DBUG_RETURN(HA_PARTIAL_COLUMN_READ | HA_REC_NOT_IN_SEQ | HA_CAN_SQL_HANDLER |
              // HA_REQUIRE_PRIMARY_KEY | HA_PRIMARY_KEY_IN_READ_INDEX |
              HA_CAN_TABLE_CONDITION_PUSHDOWN | HA_CAN_EXPORT |
              HA_CONCURRENT_OPTIMIZE | HA_CAN_ONLINE_BACKUPS |
              HA_CAN_BIT_FIELD | HA_FILE_BASED | HA_BINLOG_ROW_CAPABLE |
              HA_BINLOG_STMT_CAPABLE);
}

void tile::mytile::alloc_buffers(uint64_t size) {
  DBUG_ENTER("tile::mytile::alloc_buffers");
  // Set Attribute Buffers
  auto schema = this->array->schema();
  auto domain = schema.domain();
  auto dims = domain.dimensions();

  const char *array_type;
  tiledb_array_type_to_str(schema.array_type(), &array_type);

  // Set Coordinate Buffer
  // We don't use set_coords to avoid needing to switch on datatype
  auto coords_buffer = alloc_buffer(domain.type(), size);

  if (this->buffers.empty()) {
    for (size_t i = 0; i < table->s->fields; i++)
      this->buffers.emplace_back();
  }

  for (size_t fieldIndex = 0; fieldIndex < table->s->fields; fieldIndex++) {
    Field *field = table->field[fieldIndex];
    // Only set buffers for fields that are asked for
    if (!bitmap_is_set(this->table->read_set, fieldIndex)) {
      continue;
    }
    std::string field_name = field->field_name.str;
    // Create buffer
    std::shared_ptr<buffer> buff = std::make_shared<buffer>();
    buff->name = field_name;
    buff->dimension = false;
    buff->buffer_offset = 0;
    buff->fixed_size_elements = 1;
    buff->buffer_size = size;
    buff->allocated_buffer_size = size;

    if (schema.domain().has_dimension(field_name)) {
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
      tiledb::Attribute attr = schema.attribute(field_name);
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
  auto domain = this->array->schema().domain();

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

/**
 * Converts a tiledb record to mysql buffer using mysql fields
 * @param index
 * @return
 */
int tile::mytile::tileToFields(uint64_t orignal_index, bool dimensions_only,
                               TABLE *table) {
  DBUG_ENTER("tile::mytile::tileToFields");
  int rc = 0;
  // We must set the bitmap for debug purpose, it is "write_set" because we use
  // Field->store
  my_bitmap_map *orig = dbug_tmp_use_all_columns(table, table->write_set);
  try {
    for (size_t fieldIndex = 0; fieldIndex < table->s->fields; fieldIndex++) {
      Field *field = table->field[fieldIndex];
      // Only read fields that are asked for
      if (!bitmap_is_set(this->table->read_set, fieldIndex)) {
        continue;
      }
      uint64_t index = orignal_index;
      field->set_notnull();

      std::shared_ptr<buffer> buff = this->buffers[fieldIndex];

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
  // Reset bitmap to original
  dbug_tmp_restore_column_map(table->write_set, orig);
  DBUG_RETURN(rc);
};

int tile::mytile::mysql_row_to_tiledb_buffers(const uchar *buf) {
  DBUG_ENTER("tile::mytile::mysql_row_to_tiledb_buffers");
  int error = 0;

  // We must set the bitmap for debug purpose, it is "write_set" because we use
  // Field->store
  my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);

  try {
    // for (Field **field = table->field; *field; field++) {
    for (size_t fieldIndex = 0; fieldIndex < table->s->fields; fieldIndex++) {
      Field *field = table->field[fieldIndex];
      // Error if there is a field missing from writting
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

  // Reset bitmap to original

  dbug_tmp_restore_column_map(table->read_set, old_map);
  DBUG_RETURN(error);
}

void tile::mytile::start_bulk_insert(ha_rows rows, uint flags) {
  DBUG_ENTER("tile::mytile::start_bulk_insert");
  this->bulk_write = true;
  DBUG_VOID_RETURN;
}

int tile::mytile::end_bulk_insert() {
  DBUG_ENTER("tile::mytile::end_bulk_insert");

  int rc = 0;
  // Set all buffers with proper size
  try {
    // Submit query
    flush_write();
    query->finalize();
    this->query = nullptr;
    this->array->close();

  } catch (const tiledb::TileDBError &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR,
                    "[end_bulk_insert] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = -301;
  } catch (const std::exception &e) {
    // Log errors
    my_printf_error(ER_UNKNOWN_ERROR,
                    "[end_bulk_insert] error for table %s : %s",
                    ME_ERROR_LOG | ME_FATAL, this->uri.c_str(), e.what());
    rc = -302;
  }

  DBUG_RETURN(rc);
}

int tile::mytile::flush_write() {
  DBUG_ENTER("tile::mytile::flush_write");

  int rc = 0;
  // Set all buffers with proper size
  try {
    for (auto &buff : this->buffers) {
      if (this->array->schema().domain().has_dimension(buff->name)) {
        this->ctx.handle_error(tiledb_query_set_buffer(
            this->ctx.ptr().get(), this->query->ptr().get(), tiledb_coords(),
            buff->buffer, &buff->buffer_size));
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

    query->submit();
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
/**
 * Write row
 * @param buf
 * @return
 */
int tile::mytile::write_row(const uchar *buf) {
  DBUG_ENTER("tile::mytile::write_row");
  int rc = 0;
  // We must set the bitmap for debug purpose, it is "read_set" because we use
  // Field->val_*
  my_bitmap_map *orig = dbug_tmp_use_all_columns(table, table->read_set);

  if ((this->array->is_open() && this->array->query_type() != TILEDB_WRITE) ||
      !this->array->is_open()) {
    if (this->array->is_open())
      this->array->close();

    this->array->open(TILEDB_WRITE);
  }
  if (this->query == nullptr || this->query->query_type() != TILEDB_WRITE) {
    this->query =
        std::make_unique<tiledb::Query>(this->ctx, *this->array, TILEDB_WRITE);
    this->query->set_layout(tiledb_layout_t::TILEDB_UNORDERED);
  }

  if (!this->bulk_write) {
    this->write_buffer_size = THDVAR(this->ha_thd(), write_buffer_size);
    alloc_buffers(this->write_buffer_size);
    this->record_index = 0;

    // Reset buffer sizes to 0 for writes
    // We increase the size for every cell/row we are given to write
    for (auto &buff : this->buffers) {
      buff->buffer_size = 0;
      buff->offset_buffer_size = 0;
    }
  }

  try {
    rc = mysql_row_to_tiledb_buffers(buf);
    if (rc == ERR_WRITE_FLUSH_NEEDED) {
      flush_write();
      DBUG_RETURN(write_row(buf));
    }
    this->record_index++;
    auto domain = this->array->schema().domain();

    if (!this->bulk_write) {
      flush_write();
      query->finalize();
      this->query = nullptr;
      this->array->close();
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
  dbug_tmp_restore_column_map(table->read_set, orig);
  DBUG_RETURN(rc);
}

/**
 *
 * @param idx
 * @param part
 * @param all_parts
 * @return
 */
ulong tile::mytile::index_flags(uint idx, uint part, bool all_parts) const {
  DBUG_ENTER("tile::mytile::index_flags");
  DBUG_RETURN(HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER | HA_READ_RANGE |
              HA_DO_INDEX_COND_PUSHDOWN | HA_DO_RANGE_FILTER_PUSHDOWN);
}

mysql_declare_plugin(mytile){
    MYSQL_STORAGE_ENGINE_PLUGIN, /* the plugin type (a MYSQL_XXX_PLUGIN value)
                                  */
    &mytile_storage_engine,  /* pointer to type-specific plugin descriptor   */
    "MyTile",                /* plugin name                                  */
    "Seth Shelnutt",         /* plugin author (for I_S.PLUGINS)              */
    "MyTile storage engine", /* general descriptive text (for I_S.PLUGINS)   */
    PLUGIN_LICENSE_PROPRIETARY, /* the plugin license (PLUGIN_LICENSE_XXX) */
    mytile_init_func,           /* Plugin Init */
    NULL,                       /* Plugin Deinit */
    0x0001,                     /* version number (0.1) */
    NULL,                       /* status variables */
    mytile_system_variables,    /* system variables */
    NULL,                       /* config options */
    0,                          /* flags */
} mysql_declare_plugin_end;
maria_declare_plugin(mytile){
    MYSQL_STORAGE_ENGINE_PLUGIN, /* the plugin type (a MYSQL_XXX_PLUGIN value)
                                  */
    &mytile_storage_engine,  /* pointer to type-specific plugin descriptor   */
    "MyTile",                /* plugin name                                  */
    "Seth Shelnutt",         /* plugin author (for I_S.PLUGINS)              */
    "MyTile storage engine", /* general descriptive text (for I_S.PLUGINS)   */
    PLUGIN_LICENSE_PROPRIETARY, /* the plugin license (PLUGIN_LICENSE_XXX) */
    mytile_init_func,           /* Plugin Init */
    NULL,                       /* Plugin Deinit */
    0x0001,                     /* version number (0.1) */
    NULL,                       /* status variables */
    mytile_system_variables,    /* system variables */
    "0.1",                      /* string version */
    MariaDB_PLUGIN_MATURITY_EXPERIMENTAL /* maturity */
} maria_declare_plugin_end;
