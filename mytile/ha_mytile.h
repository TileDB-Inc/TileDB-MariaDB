/**
 * @file   ha_mytile.h
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
 * This is the main handler implementation
 */

#pragma once

#ifdef USE_PRAGMA_INTERFACE
#pragma interface /* gcc class implementation */
#endif

#include "mytile-sysvars.h"
#include <handler.h>
#include <memory>
#include <tiledb/tiledb>

#include "handler.h"   /* handler */
#include "my_base.h"   /* ha_rows */
#include "my_global.h" /* ulonglong */
#include "thr_lock.h"  /* THR_LOCK, THR_LOCK_DATA */

#define MYSQL_SERVER 1 // required for THD class

// Handler for mytile engine
extern handlerton *mytile_hton;
namespace tile {

/** @brief
  mytile_share is a class that will be shared among all open handlers.
  This mytile implements the minimum of what you will probably need.
*/

class mytile_share : public Handler_share {
public:
  mysql_mutex_t mutex;
  THR_LOCK lock;

  mytile_share();

  ~mytile_share() override {
    thr_lock_delete(&lock);
    mysql_mutex_destroy(&mutex);
  }
};

class mytile : public handler {
public:
  /**
   * Main handler
   * @param hton
   * @param table_arg
   */
  mytile(handlerton *hton, TABLE_SHARE *table_arg) : handler(hton, table_arg){};

  ~mytile() noexcept(true){};

  /**
   * flags for supported table features
   * @return
   */
  ulonglong table_flags(void) const override;

  /**
   * Create table
   * @param name
   * @param table_arg
   * @param create_info
   * @return
   */
  int create(const char *name, TABLE *table_arg,
             HA_CREATE_INFO *create_info) override;

  /**
   * Create array functionality
   * @param name
   * @param table_arg
   * @param create_info
   * @param ctx
   * @return
   */
  int create_array(const char *name, TABLE *table_arg,
                   HA_CREATE_INFO *create_info, tiledb::Context ctx);

  /**
   * Delete table, not implemented
   * @param name
   * @return
   */
  int delete_table(const char *name) override { return 0; };

  // int rename_table(const char *from, const char *to) override;

  /**
   * Open array, not implemented
   * @param name
   * @param mode
   * @param test_if_locked
   * @return
   */
  int open(const char *name, int mode, uint test_if_locked) override {
    return 0;
  };

  /**
   * Close array, not implemented
   * @return
   */
  int close(void) override { return 0; };

  /* Table Scanning */
  int rnd_init(bool scan) override { return 0; };

  /**
   * Read next row, not implemented
   * @param buf
   * @return
   */
  int rnd_next(uchar *buf) override { return 0; };

  /**
   * Read position, not implemented
   * @param buf
   * @param pos
   * @return
   */
  int rnd_pos(uchar *buf, uchar *pos) override { return 0; };

  /**
   * End read, not implemented
   * @return
   */
  int rnd_end() override { return 0; };

  /**
   * Get current record coordinates and save to allow for later lookup
   * @param record
   */
  void position(const uchar *record) override{};

  /**
   * Write row, not implemented
   * @param buf
   * @return
   */
  int write_row(const uchar *buf) override { return 0; };

  ulong index_flags(uint idx, uint part, bool all_parts) const override;

  /**
   * Returns limit on the number of keys imposed by tokudb.
   * @return
   */
  uint max_supported_keys() const override { return 1; }

  /*
   * Store lock
   */
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type) override;

  /*
   * External lock for table locking
   */
  int external_lock(THD *thd, int lock_type) override;

  /**
   * Table info
   * @return
   */
  int info(uint) override;

private:
  THR_LOCK_DATA lock;        ///< MySQL lock
  mytile_share *share;       ///< Shared lock info
  mytile_share *get_share(); ///< Get the share

  // Table uri
  std::string uri;

  // TileDB context
  tiledb::Context ctx;
};
} // namespace tile
