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

#pragma once

#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#include "mytile-sysvars.h"
#include <handler.h>
#include <memory>
#include <tiledb/tiledb>

#include "my_global.h"                   /* ulonglong */
#include "thr_lock.h"                    /* THR_LOCK, THR_LOCK_DATA */
#include "handler.h"                     /* handler */
#include "my_base.h"                     /* ha_rows */

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

        mytile(handlerton *hton, TABLE_SHARE *table_arg) : handler(hton, table_arg) {};

        ~mytile() noexcept(true) {};

        ulonglong table_flags(void) const override;

        int create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info) override {return 0;};

        int delete_table(const char *name) override {return 0;};

        //int rename_table(const char *from, const char *to) override;

        int open(const char *name, int mode, uint test_if_locked) override {return 0;};

        int close(void) override {return 0;};

        /* Table Scanning */
        int rnd_init(bool scan) override {return 0;};

        int rnd_next(uchar *buf) override {return 0;};

        int rnd_pos(uchar *buf, uchar *pos) override {return 0;};

        int rnd_end() override {return 0;};

        void position(const uchar *record) override {};

        int write_row(uchar *buf __attribute__((unused))) override {return 0;};

        ulong index_flags(uint idx, uint part, bool all_parts) const override {return 0;};

        //const COND *cond_push(const COND *cond) override;

        // bool check_primary_key_exists(uchar *buf);

       // int tile_write_row(uchar *buf);

        // int delete_row(const uchar *buf) override;

        //int update_row(const uchar *old_data, uchar *new_data) override;

        THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) override;

        int external_lock(THD *thd, int lock_type) override;

        int info(uint) override;

    private:

        THR_LOCK_DATA lock;      ///< MySQL lock
        mytile_share *share;    ///< Shared lock info
        mytile_share *get_share(); ///< Get the share

        // Table uri
        std::string uri;

        // TileDB context
        tiledb::Context ctx;

        //std::unique_ptr<tiledb::Array> array;

        //std::unique_ptr<tiledb::Query> query;

        //void allocBuffers(uint64_t readBufferSize);

        //void setSubarray(void *subarray);

        /*void *buildSubArray(bool tableScan);

        template<typename T>
        T *buildSubArray(bool tableScan) {
            DBUG_ENTER("mytile::buildSubArray");
            tiledb::ArraySchema schema = array->schema();
            tiledb::Domain domain = schema.domain();
            auto dimensions = domain.dimensions();
            auto nonEmptyDomains = array->non_empty_domain<T>();

            T *subarray = new T[domain.ndim()];

            for (size_t i = 0; i < nonEmptyDomains.size(); i++) {
                subarray[2 * i] = nonEmptyDomains[i].second.first;
                subarray[2 * i + 1] = nonEmptyDomains[i].second.second;
            }

            DBUG_RETURN(subarray);
        }

        uint64_t computeRecordsUB(void *subarray);

        template<typename T>
        uint64_t computeRecordsUB(void *subarray) {
            T* s = static_cast<T*>(subarray);
            size_t elements = array->schema().domain().ndim();
            // Get max buffer sizes to build
            const std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> &maxSizes = array->max_buffer_elements<T>(
                    std::vector<T>(s, s + elements));

            // Compute an upper bound on the number of results in the subarray.
            return maxSizes.find(TILEDB_COORDS)->second.second / dimensionIndexes.size();
        }*/

        //bool isDimension(std::string name);

        //bool isDimension(LEX_CSTRING name);

        //std::unordered_map<std::string, uint64_t> dimensionIndexes;

        // This is a vector holding buffers based the MariaDB field index
        //std::vector<std::unique_ptr<buffer>> buffers;

        // Coordinates have dedicated buffer since they are referenced often.
//        buffer coordinates;
//
//        uint64_t bufferSizeBytes = 0;
//        int64_t currentRowPosition = -1;
//        int64_t currentNumRecords = -2;
//        uint64_t totalNumRecordsUB = 0;
//        uint64_t numRecordsRead = 0;
//        tiledb::Query::Status status = tiledb::Query::Status::UNINITIALIZED;
    };
}
