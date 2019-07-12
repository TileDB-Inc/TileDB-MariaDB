/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#pragma once

#include <field.h>
#include <tiledb/tiledb>

struct ha_table_option_struct
{
    const char *array_uri;
    ulonglong capacity;
    uint array_type;
    ulonglong cell_order;
    ulonglong tile_order;
};

struct ha_field_option_struct
{
    bool dimension;
    const char *lower_bound;
    const char *upper_bound;
    const char *tile_extent;
};

namespace tile {
    typedef struct ::ha_table_option_struct ha_table_option_struct;
    typedef struct ::ha_field_option_struct ha_field_option_struct;

    tiledb::Attribute create_field_attribute(tiledb::Context &ctx, Field *field, tiledb::FilterList filterList);

    tiledb::Dimension create_field_dimension(tiledb::Context &ctx, Field *field);
}
