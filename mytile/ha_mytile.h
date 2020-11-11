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
#include <map>
#include <tiledb/tiledb>

#include "handler.h"   /* handler */
#include "my_base.h"   /* ha_rows */
#include "my_global.h" /* ulonglong */
#include "thr_lock.h"  /* THR_LOCK, THR_LOCK_DATA */

#define MYSQL_SERVER 1 // required for THD class

#define MYTILE_MAX_KEY_LENGTH MAX_DATA_LENGTH_FOR_KEY
// Handler for mytile engine
extern handlerton *mytile_hton;
namespace tile {

class mytile : public handler {
public:
  // Set max support key size
  uint max_supported_key_length() const override {
    return MYTILE_MAX_KEY_LENGTH;
  }
  uint max_supported_key_part_length() const override {
    return MYTILE_MAX_KEY_LENGTH;
  }

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
   * check if a field has a default value
   * @param field
   */
  bool field_has_default_value(Field *field) const;

  /**
   * get a default value's size
   * @param value
   * @param type
   */
  uint64_t get_default_value_size(const void* value,
                                  tiledb_datatype_t type) const;

  /**
   * get a buffer containing the default value of a field 
   * @param table_arg
   * @param field_idx
   * @param attr
   * @param buff
   */
  void get_field_default_value(TABLE *table_arg, 
                          size_t field_idx, 
                          tiledb::Attribute *attr,
                          std::shared_ptr<buffer> buff);

  /**
   * Create array functionality
   * @param name
   * @param table_arg
   * @param create_info
   * @param context
   * @return
   */
  int create_array(const char *name, TABLE *table_arg,
                   HA_CREATE_INFO *create_info, tiledb::Context context);

  /**
   * Drop a table
   * note: we implement drop_table not delete_table because in drop_table the
   * table is open
   * @param name
   * @return
   */
  void drop_table(const char *name) override;

  /**
   * Drop a table by rm'ing the tiledb directory if mytile_delete_arrays=1 is
   * set. If mytile_delete_arrays is not set we just deregister the table
   *
   * @param name
   * @return
   */
  int delete_table(const char *name) override;

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
  int init_scan(THD *thd);

  /* Table Scanning */
  int rnd_init(bool scan) override;

  /**
   * Read next Row
   * @return
   */
  int scan_rnd_row(TABLE *table);

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
   * Read position based on cordinates stored in pos buffer
   * @param buf ignored
   * @param pos coordinate
   * @return
   */
  int rnd_pos(uchar *buf, uchar *pos) override;

  /**
   * Get current record coordinates and save to allow for later lookup
   * @param record
   */
  void position(const uchar *record) override;

  /**
   * Fetches the current coordinates as a byte vector
   * <uint64_t>-<data>-<uint64_t>-<data>
   *
   * Where the prefix of <uint64_t> is the length of the data to follow
   * @param index of buffers to use
   * @return byte vector
   */
  std::vector<uint8_t> get_coords_as_byte_vector(uint64_t index);

  /**
   * Realloc the ref based on current size of data
   * This is needed for string dims
   * @param size
   * @return
   */
  int realloc_ref_based_size(uint64_t size);

  /**
   * Write row
   * @param buf
   * @return
   */
  int write_row(const uchar *buf) override;

  /**
   *
   * @param rows
   * @param flags
   */
  void start_bulk_insert(ha_rows rows, uint flags) override;

  /**
   *
   * @return
   */
  int end_bulk_insert() override;

  /**
   * flush_write
   * @return
   */
  int flush_write();

  /**
   * Convert a mysql row to attribute/coordinate buffers (columns)
   * @param buf
   * @return
   */
  int mysql_row_to_tiledb_buffers(const uchar *buf);

  /**
   *  Handle condition pushdown of sub conditions
   * @param cond_item
   * @return
   */
  const COND *cond_push_cond(Item_cond *cond_item);

  /**
   *  Handle function condition pushdowns
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
   * Handle the actual pushdown, this is a common function for cond_push and
   * indx_cond_push
   *
   * @param cond condition being pushed
   * @return condition stack which could not be pushed
   */
  const COND *cond_push_local(const COND *cond);

  /**
    Pop the top condition from the condition stack of the storage engine
    for each partition.
  */
  void cond_pop() override;

  /**
   *
   * @param idx
   * @param part
   * @param all_parts
   * @return
   */
  ulong index_flags(uint idx, uint part, bool all_parts) const override;

  /**
   * Returns limit on the number of keys imposed.
   * @return
   */
  uint max_supported_keys() const override { return MAX_INDEXES; }

  /**
   * Mytile doesn't need locks, so we just ignore the store_lock request by
   * returning the original lock data
   * @param thd
   * @param to
   * @param lock_type
   * @return the original lock, to
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
   * Helper function to alloc and set read buffers
   * @param size
   */
  void alloc_read_buffers(uint64_t size);

  /**
   * Helper to free buffer
   */
  void dealloc_buffer(std::shared_ptr<buffer> buff) const;

  /**
   * Helper to free buffers
   */
  void dealloc_buffers();

  /**
   * Helper to get field attribute value specified as DEFAULT during table creation
   * @param table_arg
   * @param field_idx
   * @param default_value
   * @param default_value_size
   */
  void get_field_default_value(TABLE *table_arg, size_t field_idx,
                                void *&default_value,
                                uint64_t &default_value_size) const;

  /**
   *
   * Converts a tiledb record to mysql buffer using mysql fields
   * @param record_position
   * @param dimensions_only
   * @param TABLE
   * @return status
   */
  int tileToFields(uint64_t record_position, bool dimensions_only,
                   TABLE *table);

  /**
   * Table info
   * @return
   */
  int info(uint) override;

  /**
   * Index read will optional push a key down to tiledb if no existing pushdown
   * has already happened
   *
   * This will then initiate a table scan
   *
   * @param buf unused mysql buffer
   * @param key key to pushdown
   * @param key_len length of key
   * @param find_flag operation (lt, le, eq, ge, gt)
   * @return status
   */
  int index_read(uchar *buf, const uchar *key, uint key_len,
                 enum ha_rkey_function find_flag) override;

  /**
   * Fetch a single row based on a single key
   *
   * This will then initiate a table scan
   *
   * @param buf unused mysql buffer
   * @param idx index number (mytile only support 1 primary key currently)
   * @param key key to pushdown
   * @param keypart_map bitmap of parts of key which are included
   * @param find_flag operation (lt, le, eq, ge, gt)
   * @return status
   */
  int index_read_idx_map(uchar *buf, uint idx, const uchar *key,
                         key_part_map keypart_map,
                         enum ha_rkey_function find_flag) override;

  /**
   * Is the primary key clustered
   * @return true because tiledb data is storted based on dimensions and layout
   */
  bool primary_key_is_clustered() override { return TRUE; }

  /**
   * Pushdown an index condition
   * @param keyno key number
   * @param idx_cond Condition
   * @return Left over conditions not pushdown
   */
  Item *idx_cond_push(uint keyno, Item *idx_cond) override;

  /**
   * Prepare for index usage, treated here similar to rnd_init
   * @param idx key number to use
   * @param sorted unused
   * @return
   */
  int index_init(uint idx, bool sorted) override;

  /**
   * Treated like rnd_end
   * @return
   */
  int index_end() override;

  /**
   * Compares two buffers checking their bytes
   * Iterates through dimension buffers
   * First buffer is the key, second buffer is the dimension buffer
   * @param key
   * @param key_len
   * @param index
   * @return minus(<0) if key is less than buffer, 0 if equal, positive (>0) if
   * key is greater than buffer
   */
  int8_t compare_key_to_dims(const uchar *key, uint key_length, uint64_t index);

  int8_t compare_key_to_dim(const uchar *key, uint64_t *dim_key_length,
                            const uint64_t index,
                            const std::shared_ptr<buffer> &buf);

  template <typename T>
  int8_t compare_key_to_dim(const uchar *key, uint64_t key_length,
                            const void *buffer,
                            uint64_t buffer_size = sizeof(T)) {
    // If the lower is null, set it
    if (std::is_same<T, char>()) {
      auto cmp = memcmp(buffer, key, std::min(key_length, buffer_size));
      if (cmp == 0 && key_length < buffer_size) {
        return -1;
      } else if (cmp == 0 && key_length > buffer_size) {
        return 1;
      }
      return cmp;
    }

    const T *key_typed = reinterpret_cast<const T *>(key);
    const T *buffer_typed = reinterpret_cast<const T *>(buffer);

    if (*key_typed < *buffer_typed)
      return -1;
    if (*key_typed == *buffer_typed)
      return 0;

    // last case is always if (key_typed > buffer_size)
    return 1;
  }

  /**
   * Returns an estimation for number of records expected from the current query
   * @return number of rows
   */
  uint64_t computeRecordsUB();

  /**
   * Perform a scan over query results to find the specified key
   * This performs a sequence scan to find a key. It is used for index scans or
   * MRR
   * @param key key to find
   * @param key_len length of key indicating the key parts
   * @param find_flag
   * @param reset clean up as if index_end were called
   * @return
   */
  int index_read_scan(const uchar *key, uint key_len,
                      enum ha_rkey_function find_flag,
                      bool reset);

  /**
   * Read "first" row
   * @param buf
   * @return
   */
  int index_first(uchar *buf) override;

  /**
   * Read next row
   * @param buf
   * @return
   */
  int index_next(uchar *buf) override;

  /**
   * Implement initial records in range
   * Currently returns static large value
   */
  ha_rows records_in_range(uint inx, key_range *min_key,
                           key_range *max_key) override;

  /**
   * Multi Range Read interface
   */
  int multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param,
                            uint n_ranges, uint mode,
                            HANDLER_BUFFER *buf) override;
  int multi_range_read_next(range_id_t *range_info) override;
  ha_rows multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq,
                                      void *seq_init_param, uint n_ranges,
                                      uint *bufsz, uint *flags,
                                      Cost_estimate *cost) override;
  ha_rows multi_range_read_info(uint keyno, uint n_ranges, uint keys,
                                uint key_parts, uint *bufsz, uint *flags,
                                Cost_estimate *cost) override;
  int multi_range_read_explain_info(uint mrr_mode, char *str,
                                    size_t size) override;

  /**
   * We override the default implementation only so we can reset the
   * index counters at the end of checking the function, otherwise
   * most of the implementation is straight from handler.cc
   * @param buf
   * @param key
   * @param keylen
   * @return
   */
  int index_next_same(uchar *buf, const uchar *key, uint keylen) override;

private:
  DsMrr_impl ds_mrr;
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

  // Number of dimensions, this is used frequently so let's cache it
  uint64_t ndim = 0;

  // Array Schema
  std::unique_ptr<tiledb::ArraySchema> array_schema;

  uint64_t records = 0;
  uint64_t records_read = 0;
  tiledb::Query::Status status = tiledb::Query::Status::UNINITIALIZED;

  // Vector of pushdowns
  std::vector<std::vector<std::shared_ptr<tile::range>>> pushdown_ranges;

  // Vector of pushdown in ranges
  std::vector<std::vector<std::shared_ptr<tile::range>>> pushdown_in_ranges;

  // read buffer size
  uint64_t read_buffer_size = 0;

  // write buffer size
  uint64_t write_buffer_size = 0;

  // in bulk write mode
  bool bulk_write = false;

  // query is mrr
  bool mrr_query = false;

  // Upper bound for records, used for table stats by optimized
  uint64_t records_upper_bound = 2;

  /**
   * Helper to setup writes
   */
  void setup_write();

  /**
   * Helper to end and finalize writes
   * @return
   */
  int finalize_write();

  /**
   * Helper function which validates the array is open for reads
   */
  void open_array_for_reads(THD *thd);

  /**
   * Helper function which validates the array is open for writes
   */
  void open_array_for_writes(THD *thd);

  /**
   * Checks if there are any ranges pushed
   * @return
   */
  bool valid_pushed_ranges();

  /**
   * Checks if there are any in ranges pushed
   * @return
   */
  bool valid_pushed_in_ranges();

  /**
   * Reset condition pushdowns to be for key conditions
   * @param key key(s) to pushdown
   * @param key_len
   * @param start_key is key start key
   * @param find_flag equality condition of last key
   * @return
   */
  int reset_pushdowns_for_key(const uchar *key, uint key_len, bool start_key,
                              enum ha_rkey_function find_flag);

  /**
   * Build MRR ranges from current handle mrr details
   * @return
   */
  int build_mrr_ranges();

  /**
   * Check if a query is complete or not
   * @return true if query is complete, false otherwise
   */
  bool query_complete();

  // Indicator for when non_empty_domain is empty and reads will return empty
  // dataset
  int empty_read = 0;

  // Indicates that a user is querying metadata only
  bool metadata_query;

  // map for holding array metadata
  std::unordered_map<std::string, std::string> metadata_map;

  // Metadata iterator
  std::unordered_map<std::string, std::string>::iterator metadata_map_iterator;

  // Last metadata value accessed
  std::pair<std::string, std::string> metadata_last_value;

  /**
   * Load metadata into unordered_map
   * @return int status
   */
  int load_metadata();

  /**
   *
   * @return int status
   */
  int metadata_next();

  /**
   * Convert metadata pair to fields
   * @param metadata to convert to fields
   * @return int status
   */
  int metadata_to_fields(const std::pair<std::string, std::string> &metadata);
};
} // namespace tile
