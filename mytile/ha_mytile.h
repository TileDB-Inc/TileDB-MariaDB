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

#include "ha_mytile_share.h"
#include "mytile-buffer.h"
#include "mytile-range.h"
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

class mytile : public handler {
public:
  /**
   * Main handler
   * @param hton
   * @param table_arg
   */
  mytile(handlerton *hton, TABLE_SHARE *table_arg);

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
   * Open array
   * @param name
   * @param mode
   * @param test_if_locked
   * @return
   */
  int open(const char *name, int mode, uint test_if_locked) override;

  /**
   * Close array
   * @return
   */
  int close(void) override;

  /**
   * Initialize table scanning
   * @return
   */
  int init_scan(THD *thd, std::unique_ptr<void, decltype(&std::free)> subarray);

  /* Table Scanning */
  int rnd_init(bool scan) override;

  /**
   * Rext next Row
   * @return
   */
  int rnd_row(TABLE *table);

  /**
   * Read next row
   * @param buf
   * @return
   */
  int rnd_next(uchar *buf) override;

  /**
   * End read
   * @return
   */
  int rnd_end() override;

  /**
   * Read position
   * @param buf
   * @param pos
   * @return
   */
  int rnd_pos(uchar *buf, uchar *pos) override;

  /**
   * Get current record coordinates and save to allow for later lookup
   * @param record
   */
  void position(const uchar *record) override;

  /**
   * Write row, not implemented
   * @param buf
   * @return
   */
  int write_row(const uchar *buf) override { return 0; };

  /**
   *  Handle condition pushdown of sub conditions
   * @param cond_item
   * @return
   */
  const COND *cond_push_cond(Item_cond *cond_item);

  /**
   *  Handle func conditoin pushdowns
   * @param func_item
   * @return
   */
  const COND *cond_push_func(const Item_func *func_item);

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
  const COND *cond_push(const COND *cond) override;

  /**
    Pop the top condition from the condition stack of the storage engine
    for each partition.
  */
  void cond_pop() override;

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
   * Helper function to allocate all buffers
   */
  void alloc_buffers(uint64_t size);

  /**
   * Helper to free buffers
   */
  void dealloc_buffers();

  /**
   *
   * @param item
   * @param dimensions_only
   * @return
   */
  int tileToFields(uint64_t record_position, bool dimensions_only,
                   TABLE *table);

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

  // TileDB Config
  tiledb::Config config;

  // TileDB Array
  std::shared_ptr<tiledb::Array> array;

  // TileDB Query
  std::shared_ptr<tiledb::Query> query;

  // Current record row
  uint64_t record_index = 0;

  // Vector of buffers in field index order
  std::vector<std::shared_ptr<buffer>> buffers;
  std::shared_ptr<buffer> coord_buffer;

  // Number of dimensions, this is used frequently so let's cache it
  uint64_t ndim = 0;

  // Upper bound for number of records so we know stopping condition
  uint64_t total_num_records_UB = 0;

  int64_t records = -2;
  uint64_t records_read = 0;
  tiledb::Query::Status status = tiledb::Query::Status::UNINITIALIZED;

  uint64_t read_buffer_size = 0;

  // Vector of pushdowns
  std::vector<std::vector<std::shared_ptr<tile::range>>> pushdown_ranges;

  friend class mytile_select_handler;
};
} // namespace tile
