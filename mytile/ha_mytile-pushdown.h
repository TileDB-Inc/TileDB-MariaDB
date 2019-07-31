/**
 * @file   ha_mytile-pushdown.h
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
 * This is the select handler pushdown implementation
 */

#pragma once

#define MYSQL_SERVER 1 // required for THD class

#include <my_global.h> /* ulonglong */
#include <handler.h>
#include <select_handler.h>
#include <sql_class.h>
#include <sql_lex.h>
#include "ha_mytile.h"
#include "ha_mytile_share.h"

class mytile_share;
class mytile;
namespace tile {
/*
  Implementation class of the select_handler interface for mytile:
  class declaration
*/

class mytile_select_handler : public select_handler {
private:
  mytile_share *share;
  mytile *mytile_handler;

public:
  mytile_select_handler(THD *thd_arg, SELECT_LEX *sel);

  ~mytile_select_handler() override;

  int init_scan() override;

  int next_row() override;

  int end_scan() override;

  void print_error(int, unsigned long) override;

  static select_handler *create_mytile_select_handler(THD *thd,
                                                      SELECT_LEX *sel) {

    return 0;

    // Currentl global order pushdown is not supported
    if (sel->gorder_list.elements > 0)
      return 0;

    // Currentl order by pushdown is not supported
    if (sel->order_list.elements > 0)
      return 0;

    // Currentl group by pushdown is not supported
    if (sel->group_list.elements > 0)
      return 0;

    mytile_select_handler *handler = NULL;
    handlerton *ht = 0;

    for (TABLE_LIST *tbl = thd->lex->query_tables; tbl;
         tbl = tbl->next_global) {
      if (!tbl->table)
        return 0;
      if (!ht)
        ht = tbl->table->file->partition_ht();
      else if (ht != tbl->table->file->partition_ht())
        return 0;
    }

    handler = new mytile_select_handler(thd, sel);

    return handler;
  }
};
} // namespace tile
