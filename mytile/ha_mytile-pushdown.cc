/**
 * @file   ha_mytile-pushdown.cc
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
 * This is the select pushdown handler implementation
 */

#include "ha_mytile-pushdown.h"
#include "ha_mytile.h"
#include "mytile-sysvars.h"
#include <handler.h>
#include <table.h>

/*
  Implementation class of the select_handler interface for mytile:
  class implementation
*/

tile::mytile_select_handler::mytile_select_handler(THD *thd, SELECT_LEX *sel)
    : select_handler(thd, mytile_hton), share(NULL) {
  this->select = sel;
}

tile::mytile_select_handler::~mytile_select_handler() {}

int tile::mytile_select_handler::init_scan() {
  DBUG_ENTER("mytile_select_handler::init_scan");
  int rc = 0;

  // Find table to get handler
  TABLE *table = 0;
  for (TABLE_LIST *tbl = this->thd->lex->query_tables; tbl;
       tbl = tbl->next_global) {
    if (!tbl->table)
      continue;
    table = tbl->table;
    break;
  }

  std::cerr << "this->select->select_n_where_fields="
            << this->select->select_n_where_fields << std::endl;
  if (this->select->select_n_where_fields > 0) {
    for (uint i = 0; i < this->select->select_n_where_fields; i++) {
      Item *item = this->select->where;
      if (item != nullptr) {
        std::cerr << "where_clause[" << i << "]->"
                  << "name=" << item->name.str << std::endl;
      }
    }
  }

  this->mytile_handler = (mytile *)table->file;
  rc = this->mytile_handler->init_scan(
      this->thd,
      std::unique_ptr<void, decltype(&std::free)>(nullptr, &std::free));
  DBUG_RETURN(rc);
}

int tile::mytile_select_handler::next_row() {
  DBUG_ENTER("mytile_select_handler::next_row");
  int rc = 0;
  /**
   * The `table` field here is a tmp table mysql creates with the proper schema
   * and it is used only for setting the buffer. Its not materialized. We pass
   * it as a function so the results will be stored (via FIELDs) into this tmp
   * table and not the TABLE object of the handler (the real table)
   */
  rc = this->mytile_handler->rnd_row(table);
  DBUG_RETURN(rc);
}

int tile::mytile_select_handler::end_scan() {
  DBUG_ENTER("mytile_select_handler::end_scan");
  int rc = 0;
  this->mytile_handler->dealloc_buffers();
  DBUG_RETURN(rc);
}

void tile::mytile_select_handler::print_error(int, unsigned long) {}
