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
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This is the main handler implementation
 */

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation                          // gcc: Class implementation
#endif

#include <my_global.h>

#define MYSQL_SERVER 1

#include <my_config.h>
#include <mysql/plugin.h>
#include "ha_mytile.h"
#include "mytile-discovery.h"
#include "mytile-sysvars.h"
#include <cstring>
//#include "sql_class.h"
#include <vector>

// Handler for mytile engine
handlerton *mytile_hton;

/**
  @brief
  Function we use in the creation of our hash to get key.
*/

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key ex_key_mutex_mytile_share_mutex;

static PSI_mutex_info all_mytile_mutexes[] =
        {
                {&ex_key_mutex_mytile_share_mutex, "mytile_share::mutex", 0}
        };

static void init_mytile_psi_keys() {
    const char *category = "mytile";
    int count;

    count = array_elements(all_mytile_mutexes);
    mysql_mutex_register(category, all_mytile_mutexes, count);
}

#endif

struct ha_table_option_struct
{
    const char *array_uri;
    ulonglong capacity;
    uint array_type;
    ulonglong cell_order;
    ulonglong tile_order;
};

ha_create_table_option mytile_table_option_list[] = {
        HA_TOPTION_STRING("uri", array_uri),
        HA_TOPTION_ENUM("array_type", array_type, "DENSE,SPARSE", TILEDB_SPARSE),
        HA_TOPTION_NUMBER("capacity", capacity, 10000, 0, UINT64_MAX, 1),
        HA_TOPTION_ENUM("cell_order", cell_order, "ROW_MAJOR,COLUMN_MAJOR", TILEDB_ROW_MAJOR),
        HA_TOPTION_ENUM("tile_order", tile_order, "ROW_MAJOR,COLUMN_MAJOR", TILEDB_ROW_MAJOR),
        HA_TOPTION_END
};

struct ha_field_option_struct
{
    bool *dimension;
    const char *lower_bound;
    const char *upper_bound;
    const char *tile_extent;
};

ha_create_table_option mytile_field_option_list[] =
        {
                /*
                  If the engine wants something more complex than a string, number, enum,
                  or boolean - for example a list - it needs to specify the option
                  as a string and parse it internally.
                */
                HA_FOPTION_BOOL("dimension", dimension, 0),
                HA_FOPTION_STRING("lower_bound", lower_bound),
                HA_FOPTION_STRING("upper_bound", upper_bound),
                HA_FOPTION_STRING("tile_extent", tile_extent),
                HA_FOPTION_END
        };

tile::mytile_share::mytile_share() {
    thr_lock_init(&lock);
    mysql_mutex_init(ex_key_mutex_mytile_share_mutex,
                     &mutex, MY_MUTEX_INIT_FAST);
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
static handler *mytile_create_handler(handlerton *hton,
                                      TABLE_SHARE *table,
                                      MEM_ROOT *mem_root) {
    return new(mem_root) tile::mytile(hton, table);
}

// mytile file extensions
static const char *mytile_exts[] = {
        NullS
};

// Initialization function
static int mytile_init_func(void *p) {
    DBUG_ENTER("mytile_init_func");

#ifdef HAVE_PSI_INTERFACE
    // Initialize performance schema keys
    init_mytile_psi_keys();
#endif

    mytile_hton = (handlerton *) p;
    mytile_hton->state = SHOW_OPTION_YES;
    mytile_hton->create = mytile_create_handler;
    mytile_hton->tablefile_extensions = mytile_exts;
    mytile_hton->table_options = mytile_table_option_list;
    mytile_hton->field_options = mytile_field_option_list;
    mytile_hton->discover_table = tile::mytile_discover_table;
    mytile_hton->discover_table_existence = tile::mytile_discover_table_existence;

    DBUG_RETURN(0);
}

// Storage engine interface
struct st_mysql_storage_engine mytile_storage_engine =
        {MYSQL_HANDLERTON_INTERFACE_VERSION};

/**
 * Store a lock, we aren't using table or row locking at this point.
 * @param thd
 * @param to
 * @param lock_type
 * @return
 */
THR_LOCK_DATA **tile::mytile::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) {
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
 * This should return relevant stats of the underlying tiledb map,
 * currently just sets row count to 2, to avoid 0/1 row optimizations
 * @return
 */
int tile::mytile::info(uint) {
    DBUG_ENTER("tile::mytile::info");
    //Need records to be greater than 1 to avoid 0/1 row optimizations by query optimizer
    stats.records = 2;
    DBUG_RETURN(0);
};

/**
 * Flags for table features supported
 * @return
 */
ulonglong tile::mytile::table_flags(void) const {
    DBUG_ENTER("tile::mytile::table_flags");
    DBUG_RETURN(HA_REC_NOT_IN_SEQ | HA_CAN_SQL_HANDLER //| HA_NULL_IN_KEY | //HA_REQUIRE_PRIMARY_KEY
                | HA_CAN_BIT_FIELD | HA_FILE_BASED | HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE);
}

mysql_declare_plugin(mytile)
                {
                        MYSQL_STORAGE_ENGINE_PLUGIN,                  /* the plugin type (a MYSQL_XXX_PLUGIN value)   */
                        &mytile_storage_engine,                       /* pointer to type-specific plugin descriptor   */
                        "MyTile",                                     /* plugin name                                  */
                        "Seth Shelnutt",                              /* plugin author (for I_S.PLUGINS)              */
                        "MyTile storage engine",                      /* general descriptive text (for I_S.PLUGINS)   */
                        PLUGIN_LICENSE_PROPRIETARY,                           /* the plugin license (PLUGIN_LICENSE_XXX)      */
                        mytile_init_func,                             /* Plugin Init */
                        NULL,                                         /* Plugin Deinit */
                        0x0001,                                       /* version number (0.1) */
                        NULL,                                         /* status variables */
                        mytile_system_variables,                      /* system variables */
                        NULL,                                         /* config options */
                        0,                                            /* flags */
                }mysql_declare_plugin_end;
maria_declare_plugin(mytile)
                {
                        MYSQL_STORAGE_ENGINE_PLUGIN,                  /* the plugin type (a MYSQL_XXX_PLUGIN value)   */
                        &mytile_storage_engine,                       /* pointer to type-specific plugin descriptor   */
                        "MyTile",                                     /* plugin name                                  */
                        "Seth Shelnutt",                              /* plugin author (for I_S.PLUGINS)              */
                        "MyTile storage engine",                      /* general descriptive text (for I_S.PLUGINS)   */
                        PLUGIN_LICENSE_PROPRIETARY,                           /* the plugin license (PLUGIN_LICENSE_XXX)      */
                        mytile_init_func,                             /* Plugin Init */
                        NULL,                                         /* Plugin Deinit */
                        0x0001,                                       /* version number (0.1) */
                        NULL,                                         /* status variables */
                        mytile_system_variables,                      /* system variables */
                        "0.1",                                        /* string version */
                        MariaDB_PLUGIN_MATURITY_EXPERIMENTAL          /* maturity */
                }maria_declare_plugin_end;