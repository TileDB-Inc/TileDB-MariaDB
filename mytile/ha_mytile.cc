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

// Structure for table options
ha_create_table_option mytile_table_option_list[] = {
    HA_TOPTION_STRING("uri", array_uri),
    HA_TOPTION_ENUM("array_type", array_type, "DENSE,SPARSE", TILEDB_SPARSE),
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
    HA_FOPTION_BOOL("dimension", dimension, 0),
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

  mytile_hton = (handlerton *)p;
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
 * Create a table structure and TileDB array schema
 * @param name
 * @param table_arg
 * @param create_info
 * @return
 */
int tile::mytile::create(const char *name, TABLE *table_arg,
                         HA_CREATE_INFO *create_info) {
  DBUG_ENTER("tile::mytile::create");
  DBUG_RETURN(
      create_array(table_arg->s->table_name.str, table_arg, create_info, ctx));
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
  if (create_info->option_struct->array_type == 0) {
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
  KEY key_info = table_arg->key_info[0];
  std::map<std::string, bool> primaryKeyParts;
  for (uint i = 0; i < key_info.user_defined_key_parts; i++) {
    Field *field = key_info.key_part[i].field;
    primaryKeyParts[field->field_name.str] = true;
  }

  // Create attributes or dimensions
  for (Field **ffield = table_arg->field; *ffield; ffield++) {
    Field *field = (*ffield);
    // If the field has the dimension flag set or it is part of the primary key
    // we treat it is a dimension
    if (field->option_struct->dimension ||
        primaryKeyParts.find(field->field_name.str) == primaryKeyParts.end()) {
      if (field->option_struct->lower_bound == nullptr ||
          strcmp(field->option_struct->lower_bound, "") == 0) {
        my_printf_error(
            ER_UNKNOWN_ERROR,
            "Dimension field %s lower_bound was not set, can not create table",
            ME_ERROR_LOG | ME_FATAL, field->field_name);
        DBUG_RETURN(-11);
      }
      if (field->option_struct->upper_bound == nullptr ||
          strcmp(field->option_struct->lower_bound, "") == 0) {
        my_printf_error(
            ER_UNKNOWN_ERROR,
            "Dimension field %s upper_bound was not set, can not create table",
            ME_ERROR_LOG | ME_FATAL, field->field_name);
        DBUG_RETURN(-12);
      }
      if (field->option_struct->tile_extent == nullptr ||
          strcmp(field->option_struct->lower_bound, "") == 0) {
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
      const char *typeString;
      tiledb_datatype_to_str(attr.type(), &typeString);
      std::cout << attr.name() << typeString << std::endl;
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
  DBUG_RETURN(
      HA_REC_NOT_IN_SEQ | HA_CAN_SQL_HANDLER | HA_REQUIRE_PRIMARY_KEY |
      HA_PRIMARY_KEY_IN_READ_INDEX | HA_PRIMARY_KEY_REQUIRED_FOR_POSITION |
      HA_CAN_TABLE_CONDITION_PUSHDOWN | HA_CAN_EXPORT | HA_CONCURRENT_OPTIMIZE |
      HA_CAN_ONLINE_BACKUPS | HA_CAN_BIT_FIELD | HA_FILE_BASED |
      HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE);
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