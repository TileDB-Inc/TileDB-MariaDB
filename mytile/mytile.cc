/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#include "mytile.h"
#include <log.h>
#include <my_global.h>

tiledb::Attribute
tile::create_field_attribute(tiledb::Context &ctx, Field *field,
                             const tiledb::FilterList &filterList) {

  std::cout << field->field_name.str << " - " << field->type() << std::endl;
  switch (field->type()) {

  case MYSQL_TYPE_DOUBLE:
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_NEWDECIMAL:
    return tiledb::Attribute::create<double>(ctx, field->field_name.str,
                                             filterList);

  case MYSQL_TYPE_FLOAT:
    return tiledb::Attribute::create<float>(ctx, field->field_name.str,
                                            filterList);

  case MYSQL_TYPE_TINY:
    if (((Field_num *)field)->unsigned_flag)
      return tiledb::Attribute::create<uint8_t>(ctx, field->field_name.str,
                                                filterList);
    else
      return tiledb::Attribute::create<int8_t>(ctx, field->field_name.str,
                                               filterList);

  case MYSQL_TYPE_SHORT:
    if (((Field_num *)field)->unsigned_flag) {
      return tiledb::Attribute::create<uint16_t>(ctx, field->field_name.str,
                                                 filterList);
    } else {
      return tiledb::Attribute::create<int16_t>(ctx, field->field_name.str,
                                                filterList);
    }
  case MYSQL_TYPE_YEAR:
    return tiledb::Attribute::create<uint16_t>(ctx, field->field_name.str,
                                               filterList);

  case MYSQL_TYPE_INT24:
    if (((Field_num *)field)->unsigned_flag)
      return tiledb::Attribute::create<uint32_t>(ctx, field->field_name.str,
                                                 filterList);
    else
      return tiledb::Attribute::create<int32_t>(ctx, field->field_name.str,
                                                filterList);

  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_LONGLONG:
    std::cout << "Creating attribute int64 for " << field->field_name.str
              << std::endl;
    if (((Field_num *)field)->unsigned_flag)
      return tiledb::Attribute::create<uint64_t>(ctx, field->field_name.str,
                                                 filterList);
    else
      return tiledb::Attribute::create<int64_t>(ctx, field->field_name.str,
                                                filterList);

  case MYSQL_TYPE_NULL:
    return tiledb::Attribute::create<uint8_t>(ctx, field->field_name.str,
                                              filterList);

  case MYSQL_TYPE_BIT:
    return tiledb::Attribute::create<uint8_t>(ctx, field->field_name.str,
                                              filterList);

  case MYSQL_TYPE_VARCHAR:
  case MYSQL_TYPE_VARCHAR_COMPRESSED:
  case MYSQL_TYPE_STRING:
  case MYSQL_TYPE_VAR_STRING:
  case MYSQL_TYPE_SET:
    return tiledb::Attribute::create<std::string>(ctx, field->field_name.str,
                                                  filterList);

  case MYSQL_TYPE_GEOMETRY:
  case MYSQL_TYPE_BLOB_COMPRESSED:
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_ENUM:
    return tiledb::Attribute::create<std::string>(ctx, field->field_name.str,
                                                  filterList);

  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_DATETIME2:
  case MYSQL_TYPE_TIME:
  case MYSQL_TYPE_TIME2:
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_TIMESTAMP2:
  case MYSQL_TYPE_NEWDATE:
    return tiledb::Attribute::create<uint64_t>(ctx, field->field_name.str,
                                               filterList);
  }
  sql_print_error("Unknown mysql data type in creating tiledb field attribute");
  return tiledb::Attribute::create<std::string>(ctx, field->field_name.str,
                                                filterList);
}

tiledb::Dimension tile::create_field_dimension(tiledb::Context &ctx,
                                               Field *field) {
  switch (field->type()) {

  case MYSQL_TYPE_DOUBLE:
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_NEWDECIMAL: {
    std::array<double, 2> domain = {DBL_MIN, DBL_MAX - 1};
    if (field->option_struct->lower_bound != nullptr) {
      domain[0] = std::strtod(field->option_struct->lower_bound, nullptr);
    }
    if (field->option_struct->upper_bound != nullptr) {
      domain[1] = std::strtod(field->option_struct->upper_bound, nullptr);
    }

    // If a user passed a tile extent use it
    if (field->option_struct->tile_extent != nullptr) {
      double tileExtent =
          std::strtod(field->option_struct->tile_extent, nullptr);
      return tiledb::Dimension::create<double>(ctx, field->field_name.str,
                                               domain, tileExtent);
    }
    sql_print_error("Invalid dimension, must specify tile extent");
    break;
  }

  case MYSQL_TYPE_FLOAT: {
    std::array<float, 2> domain = {FLT_MIN, FLT_MAX - 1};
    if (field->option_struct->lower_bound != nullptr) {
      domain[0] = std::strtof(field->option_struct->lower_bound, nullptr);
    }
    if (field->option_struct->upper_bound != nullptr) {
      domain[1] = std::strtof(field->option_struct->upper_bound, nullptr);
    }

    // If a user passed a tile extent use it
    if (field->option_struct->tile_extent != nullptr) {
      float tileExtent =
          std::strtof(field->option_struct->tile_extent, nullptr);
      return tiledb::Dimension::create<float>(ctx, field->field_name.str,
                                              domain, tileExtent);
    }
    sql_print_error("Invalid dimension, must specify tile extent");
    break;
  }

  case MYSQL_TYPE_TINY: {
    if (((Field_num *)field)->unsigned_flag) {
      std::array<uint8_t, 2> domain = {0, UINT8_MAX - 1};
      if (field->option_struct->lower_bound != nullptr) {
        domain[0] =
            std::strtoul(field->option_struct->lower_bound, nullptr, 10);
      }
      if (field->option_struct->upper_bound != nullptr) {
        domain[1] =
            std::strtoul(field->option_struct->upper_bound, nullptr, 10);
      }

      // If a user passed a tile extent use it
      if (field->option_struct->tile_extent != nullptr) {
        uint8_t tileExtent =
            std::strtoul(field->option_struct->tile_extent, nullptr, 10);
        return tiledb::Dimension::create<uint8_t>(ctx, field->field_name.str,
                                                  domain, tileExtent);
      }
    } else {
      std::array<int8_t, 2> domain = {INT8_MIN, INT8_MAX - 1};
      if (field->option_struct->lower_bound != nullptr) {
        domain[0] = std::strtol(field->option_struct->lower_bound, nullptr, 10);
      }
      if (field->option_struct->upper_bound != nullptr) {
        domain[1] = std::strtol(field->option_struct->upper_bound, nullptr, 10);
      }

      // If a user passed a tile extent use it
      if (field->option_struct->tile_extent != nullptr) {
        int8_t tileExtent =
            std::strtol(field->option_struct->tile_extent, nullptr, 10);
        return tiledb::Dimension::create<int8_t>(ctx, field->field_name.str,
                                                 domain, tileExtent);
      }
    }
    sql_print_error("Invalid dimension, must specify tile extent");
    break;
  }

  case MYSQL_TYPE_SHORT: {
    if (((Field_num *)field)->unsigned_flag) {
      std::array<uint16_t, 2> domain = {0, UINT16_MAX - 1};
      if (field->option_struct->lower_bound != nullptr) {
        domain[0] =
            std::strtoul(field->option_struct->lower_bound, nullptr, 10);
      }
      if (field->option_struct->upper_bound != nullptr) {
        domain[1] =
            std::strtoul(field->option_struct->upper_bound, nullptr, 10);
      }

      // If a user passed a tile extent use it
      if (field->option_struct->tile_extent != nullptr) {
        uint16_t tileExtent =
            std::strtoul(field->option_struct->tile_extent, nullptr, 10);
        return tiledb::Dimension::create<uint16_t>(ctx, field->field_name.str,
                                                   domain, tileExtent);
      }
    } else {
      std::array<int16_t, 2> domain = {INT16_MIN, INT16_MAX - 1};
      if (field->option_struct->lower_bound != nullptr) {
        domain[0] = std::strtol(field->option_struct->lower_bound, nullptr, 10);
      }
      if (field->option_struct->upper_bound != nullptr) {
        domain[1] = std::strtol(field->option_struct->upper_bound, nullptr, 10);
      }

      // If a user passed a tile extent use it
      if (field->option_struct->tile_extent != nullptr) {
        int16_t tileExtent =
            std::strtol(field->option_struct->tile_extent, nullptr, 10);
        return tiledb::Dimension::create<int16_t>(ctx, field->field_name.str,
                                                  domain, tileExtent);
      }
    }
    sql_print_error("Invalid dimension, must specify tile extent");
    break;
  }
  case MYSQL_TYPE_YEAR: {
    std::array<uint16_t, 2> domain = {0, UINT16_MAX - 1};
    if (field->option_struct->lower_bound != nullptr) {
      domain[0] = std::strtoul(field->option_struct->lower_bound, nullptr, 10);
    }
    if (field->option_struct->upper_bound != nullptr) {
      domain[1] = std::strtoul(field->option_struct->upper_bound, nullptr, 10);
    }

    // If a user passed a tile extent use it
    if (field->option_struct->tile_extent != nullptr) {
      uint64_t tileExtent =
          std::strtoul(field->option_struct->tile_extent, nullptr, 10);
      return tiledb::Dimension::create<uint16_t>(ctx, field->field_name.str,
                                                 domain, tileExtent);
    }
    sql_print_error("Invalid dimension, must specify tile extent");
    break;
  }

  case MYSQL_TYPE_INT24: {
    if (((Field_num *)field)->unsigned_flag) {
      std::array<uint32_t, 2> domain = {0, UINT32_MAX - 1};
      if (field->option_struct->lower_bound != nullptr) {
        domain[0] =
            std::strtoul(field->option_struct->lower_bound, nullptr, 10);
      }
      if (field->option_struct->upper_bound != nullptr) {
        domain[1] =
            std::strtoul(field->option_struct->upper_bound, nullptr, 10);
      }

      // If a user passed a tile extent use it
      if (field->option_struct->tile_extent != nullptr) {
        uint32_t tileExtent =
            std::strtoul(field->option_struct->tile_extent, nullptr, 10);
        return tiledb::Dimension::create<uint32_t>(ctx, field->field_name.str,
                                                   domain, tileExtent);
      }
    } else {
      std::array<int32_t, 2> domain = {INT32_MIN, INT32_MAX - 1};
      if (field->option_struct->lower_bound != nullptr) {
        domain[0] = std::strtol(field->option_struct->lower_bound, nullptr, 10);
      }
      if (field->option_struct->upper_bound != nullptr) {
        domain[1] = std::strtol(field->option_struct->upper_bound, nullptr, 10);
      }

      // If a user passed a tile extent use it
      if (field->option_struct->tile_extent != nullptr) {
        int32_t tileExtent =
            std::strtol(field->option_struct->tile_extent, nullptr, 10);
        return tiledb::Dimension::create<int32_t>(ctx, field->field_name.str,
                                                  domain, tileExtent);
      }
    }
    sql_print_error("Invalid dimension, must specify tile extent");
    break;
  }

  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_LONGLONG: {
    if (((Field_num *)field)->unsigned_flag) {
      std::array<uint64_t, 2> domain = {0, UINT64_MAX - 1};
      if (field->option_struct->lower_bound != nullptr) {
        domain[0] =
            std::strtoul(field->option_struct->lower_bound, nullptr, 10);
      }
      if (field->option_struct->upper_bound != nullptr) {
        domain[1] =
            std::strtoul(field->option_struct->upper_bound, nullptr, 10);
      }

      // If a user passed a tile extent use it
      if (field->option_struct->tile_extent != nullptr) {
        uint64_t tileExtent =
            std::strtoul(field->option_struct->tile_extent, nullptr, 10);
        return tiledb::Dimension::create<uint64_t>(ctx, field->field_name.str,
                                                   domain, tileExtent);
      }
    } else {
      std::array<int64_t, 2> domain = {INT64_MIN, INT64_MAX - 1};
      if (field->option_struct->lower_bound != nullptr) {
        domain[0] = std::strtol(field->option_struct->lower_bound, nullptr, 10);
      }
      if (field->option_struct->upper_bound != nullptr) {
        domain[1] = std::strtol(field->option_struct->upper_bound, nullptr, 10);
      }

      // If a user passed a tile extent use it
      if (field->option_struct->tile_extent != nullptr) {
        int64_t tileExtent =
            std::strtol(field->option_struct->tile_extent, nullptr, 10);
        return tiledb::Dimension::create<int64_t>(ctx, field->field_name.str,
                                                  domain, tileExtent);
      }
    }
    sql_print_error("Invalid dimension, must specify tile extent");
    break;
  }

  case MYSQL_TYPE_NULL:
  case MYSQL_TYPE_BIT: {
    std::array<uint8_t, 2> domain = {0, UINT8_MAX - 1};
    if (field->option_struct->lower_bound != nullptr) {
      domain[0] = std::strtoul(field->option_struct->lower_bound, nullptr, 10);
    }
    if (field->option_struct->upper_bound != nullptr) {
      domain[1] = std::strtoul(field->option_struct->upper_bound, nullptr, 10);
    }

    // If a user passed a tile extent use it
    if (field->option_struct->tile_extent != nullptr) {
      uint64_t tileExtent =
          std::strtoul(field->option_struct->tile_extent, nullptr, 10);
      return tiledb::Dimension::create<uint8_t>(ctx, field->field_name.str,
                                                domain, tileExtent);
    }
    sql_print_error("Invalid dimension, must specify tile extent");
    break;
  }

  case MYSQL_TYPE_VARCHAR:
  case MYSQL_TYPE_STRING:
  case MYSQL_TYPE_VAR_STRING:
  case MYSQL_TYPE_SET: {
    sql_print_error("Varchar fields not supported for tiledb dimension fields");
    break;
  }

  case MYSQL_TYPE_GEOMETRY:
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_ENUM: {
    sql_print_error(
        "Blob or enum fields not supported for tiledb dimension fields");
    break;
  }
  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_DATETIME2:
  case MYSQL_TYPE_TIME:
  case MYSQL_TYPE_TIME2:
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_TIMESTAMP2:
  case MYSQL_TYPE_NEWDATE: {
    std::array<uint64_t, 2> domain = {0, UINT64_MAX - 1};
    if (field->option_struct->lower_bound != nullptr) {
      domain[0] = std::strtoul(field->option_struct->lower_bound, nullptr, 10);
    }
    if (field->option_struct->upper_bound != nullptr) {
      domain[1] = std::strtoul(field->option_struct->upper_bound, nullptr, 10);
    }

    // If a user passed a tile extent use it
    if (field->option_struct->tile_extent != nullptr) {
      uint64_t tileExtent =
          std::strtoul(field->option_struct->tile_extent, nullptr, 10);
      return tiledb::Dimension::create<uint64_t>(ctx, field->field_name.str,
                                                 domain, tileExtent);
    }
    sql_print_error("Invalid dimension, must specify tile extent");
    break;
  }
  default: {
    sql_print_error(
        "Unknown mysql data type in creating tiledb field attribute");
    break;
  }
  }
  return tiledb::Dimension::create<uint8>(ctx, field->field_name.str,
                                          std::array<uint8, 2>{0, 0}, 10);
}
