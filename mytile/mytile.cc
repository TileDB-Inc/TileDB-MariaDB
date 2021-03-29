/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#include "mytile.h"
#include "utils.h"
#include <log.h>
#include <my_global.h>
#include <mysqld_error.h>
#include <sql_class.h>
#include <tztime.h>

MYSQL_TIME epoch{1970, 1, 1, 0, 0, 0, 0, false, MYSQL_TIMESTAMP_DATETIME};

tiledb_datatype_t tile::mysqlTypeToTileDBType(int type, bool signedInt) {
  switch (type) {

  case MYSQL_TYPE_DOUBLE:
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_NEWDECIMAL: {
    return tiledb_datatype_t::TILEDB_FLOAT64;
  }

  case MYSQL_TYPE_FLOAT: {
    return tiledb_datatype_t::TILEDB_FLOAT32;
  }

  case MYSQL_TYPE_TINY: {
    if (signedInt)
      return tiledb_datatype_t::TILEDB_INT8;
    return tiledb_datatype_t::TILEDB_UINT8;
  }

  case MYSQL_TYPE_SHORT: {
    if (signedInt)
      return tiledb_datatype_t::TILEDB_INT16;
    return tiledb_datatype_t::TILEDB_UINT16;
  }
  case MYSQL_TYPE_YEAR: {
    return tiledb_datatype_t::TILEDB_DATETIME_YEAR;
  }

  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_INT24: {
    if (signedInt)
      return tiledb_datatype_t::TILEDB_INT32;
    return tiledb_datatype_t::TILEDB_UINT32;
  }

  case MYSQL_TYPE_LONGLONG: {
    if (signedInt)
      return tiledb_datatype_t::TILEDB_INT64;
    return tiledb_datatype_t::TILEDB_UINT64;
  }

  case MYSQL_TYPE_NULL:
  case MYSQL_TYPE_BIT: {
    return tiledb_datatype_t::TILEDB_UINT8;
  }

  case MYSQL_TYPE_VARCHAR:
  case MYSQL_TYPE_STRING:
  case MYSQL_TYPE_VAR_STRING:
  case MYSQL_TYPE_SET: {
    return tiledb_datatype_t::TILEDB_STRING_ASCII;
  }

  case MYSQL_TYPE_GEOMETRY:
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_ENUM: {
    // TODO: We really should differentiate between blobs and text fields
    return tiledb_datatype_t::TILEDB_CHAR;
  }

  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_NEWDATE:
    return tiledb_datatype_t::TILEDB_DATETIME_DAY;

  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_DATETIME2:
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_TIMESTAMP2:
    return tiledb_datatype_t::TILEDB_DATETIME_NS;

  case MYSQL_TYPE_TIME:
  case MYSQL_TYPE_TIME2:
    return tiledb_datatype_t::TILEDB_DATETIME_NS;
  default: {
    sql_print_error("Unknown mysql data type in determining tiledb type");
  }
    return tiledb_datatype_t::TILEDB_ANY;
  }
}

std::string tile::MysqlTypeString(int type) {
  switch (type) {

  case MYSQL_TYPE_DOUBLE:
    return "DOUBLE";
  case MYSQL_TYPE_DECIMAL:
    return "DECIMAL";
  case MYSQL_TYPE_NEWDECIMAL:
    return "DECIMAL";
  case MYSQL_TYPE_FLOAT:
    return "FLOAT";
  case MYSQL_TYPE_TINY:
    return "TINYINT";
  case MYSQL_TYPE_SHORT:
    return "SMALLINT";
  case MYSQL_TYPE_YEAR:
    return "YEAR";
  case MYSQL_TYPE_INT24:
    return "MEDIUMINT";
  case MYSQL_TYPE_LONG:
    return "INTEGER";
  case MYSQL_TYPE_LONGLONG:
    return "BIGINT";
  case MYSQL_TYPE_NULL:
    return "NULL";
  case MYSQL_TYPE_BIT:
    return "BIT";
  case MYSQL_TYPE_VARCHAR:
    return "VARCHAR(255)";
  case MYSQL_TYPE_STRING:
    return "TEXT";
  case MYSQL_TYPE_VAR_STRING:
    return "VARCHAR";
  case MYSQL_TYPE_SET:
    return "SET";

  case MYSQL_TYPE_GEOMETRY:
    return "GEOMETRY";
  case MYSQL_TYPE_BLOB:
    return "BLOB";
  case MYSQL_TYPE_LONG_BLOB:
    return "LONGBLOB";
  case MYSQL_TYPE_MEDIUM_BLOB:
    return "MEDIUMBLOB";
  case MYSQL_TYPE_TINY_BLOB:
    return "TINYBLOB";
  case MYSQL_TYPE_ENUM:
    return "ENUM";

  case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_NEWDATE:
    return "DATE";

  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_DATETIME2:
    return "DATETIME(6)";

  case MYSQL_TYPE_TIME:
  case MYSQL_TYPE_TIME2:
    return "TIME(6)";
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_TIMESTAMP2:
    return "TIMESTAMP(6)";
  default: {
    sql_print_error("Unknown mysql data type in determining string");
  }
    return "";
  }
}

int tile::TileDBTypeToMysqlType(tiledb_datatype_t type, bool multi_value) {
  switch (type) {

  case tiledb_datatype_t::TILEDB_FLOAT64: {
    return MYSQL_TYPE_DOUBLE;
  }

  case tiledb_datatype_t::TILEDB_FLOAT32: {
    return MYSQL_TYPE_FLOAT;
  }

  case tiledb_datatype_t::TILEDB_INT8:
  case tiledb_datatype_t::TILEDB_UINT8: {
    if (multi_value)
      return MYSQL_TYPE_LONG_BLOB;
    return MYSQL_TYPE_TINY;
  }

  case tiledb_datatype_t::TILEDB_INT16:
  case tiledb_datatype_t::TILEDB_UINT16: {
    return MYSQL_TYPE_SHORT;
  }
  case tiledb_datatype_t::TILEDB_INT32:
  case tiledb_datatype_t::TILEDB_UINT32: {
    return MYSQL_TYPE_LONG;
  }

  case tiledb_datatype_t::TILEDB_INT64:
  case tiledb_datatype_t::TILEDB_UINT64: {
    return MYSQL_TYPE_LONGLONG;
  }

  case tiledb_datatype_t::TILEDB_CHAR:
  case tiledb_datatype_t::TILEDB_STRING_ASCII: {
    return MYSQL_TYPE_VARCHAR;
  }
  case tiledb_datatype_t::TILEDB_STRING_UTF8:
  case tiledb_datatype_t::TILEDB_STRING_UTF16:
  case tiledb_datatype_t::TILEDB_STRING_UTF32:
  case tiledb_datatype_t::TILEDB_STRING_UCS2:
  case tiledb_datatype_t::TILEDB_STRING_UCS4: {
    return MYSQL_TYPE_STRING;
  }

  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
    return MYSQL_TYPE_NEWDATE;

  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
    return MYSQL_TYPE_YEAR;

  case tiledb_datatype_t::TILEDB_DATETIME_MONTH:
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK:
  case tiledb_datatype_t::TILEDB_DATETIME_HR:
  case tiledb_datatype_t::TILEDB_DATETIME_MIN:
  case tiledb_datatype_t::TILEDB_DATETIME_SEC:
  case tiledb_datatype_t::TILEDB_DATETIME_MS:
  case tiledb_datatype_t::TILEDB_DATETIME_US:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
    return MYSQL_TYPE_TIMESTAMP;

  default: {
    const char *datatype_str;
    tiledb_datatype_to_str(type, &datatype_str);
    sql_print_error("Unknown tiledb data type in determining mysql type: %s",
                    datatype_str);
  }
    return tiledb_datatype_t::TILEDB_ANY;
  }
}

std::string tile::TileDBTypeValueToString(tiledb_datatype_t type,
                                          const void *value,
                                          uint64_t value_size) {
  switch (type) {
  case TILEDB_INT8: {
    auto v = static_cast<const int8_t *>(value);
    return std::to_string(*v);
  }
  case TILEDB_UINT8: {
    auto v = static_cast<const uint8_t *>(value);
    return std::to_string(*v);
  }
  case TILEDB_INT16: {
    auto v = static_cast<const int16_t *>(value);
    return std::to_string(*v);
  }
  case TILEDB_UINT16: {
    auto v = static_cast<const uint16_t *>(value);
    return std::to_string(*v);
  }
  case TILEDB_INT32: {
    auto v = static_cast<const int32_t *>(value);
    return std::to_string(*v);
  }
  case TILEDB_UINT32: {
    auto v = static_cast<const uint32_t *>(value);
    return std::to_string(*v);
  }
  case TILEDB_INT64: {
    auto v = static_cast<const int64_t *>(value);
    return std::to_string(*v);
  }
  case TILEDB_UINT64: {
    auto v = static_cast<const uint64_t *>(value);
    return std::to_string(*v);
  }
  case TILEDB_FLOAT32: {
    auto v = static_cast<const float *>(value);
    if (std::isnan(*v)) {
      return std::to_string(0.0f);
    }
    return std::to_string(*v);
  }
  case TILEDB_FLOAT64: {
    auto v = static_cast<const double *>(value);
    if (std::isnan(*v)) {
      return std::to_string(0.0f);
    }
    return std::to_string(*v);
  }
  case TILEDB_DATETIME_YEAR:
  case TILEDB_DATETIME_MONTH:
  case TILEDB_DATETIME_WEEK:
  case TILEDB_DATETIME_DAY:
  case TILEDB_DATETIME_HR:
  case TILEDB_DATETIME_MIN:
  case TILEDB_DATETIME_SEC:
  case TILEDB_DATETIME_MS:
  case TILEDB_DATETIME_US:
  case TILEDB_DATETIME_NS:
  case TILEDB_DATETIME_PS:
  case TILEDB_DATETIME_FS:
  case TILEDB_DATETIME_AS: {
    auto v = static_cast<const int64_t *>(value);
    // negative dates are invalid
    if (*v < 0) {
      return std::to_string(0);
    }
    return std::to_string(*v);
  }
  case TILEDB_STRING_ASCII:
  case TILEDB_CHAR:
  case TILEDB_STRING_UTF8:
  case TILEDB_STRING_UTF16:
  case TILEDB_STRING_UTF32:
  case TILEDB_STRING_UCS2:
  case TILEDB_STRING_UCS4:
  case TILEDB_ANY: {
    auto v = reinterpret_cast<const char *>(value);
    std::string s(v, value_size);
    // If the string is the null character we have to us the escaped version
    if (s[0] == '\0')
      s = "\\0";
    return std::string("'") + s + std::string("'");
  }

  default: {
    sql_print_error("Unknown tiledb data type in TileDBTypeValueToString");
  }
  }

  return nullptr;
}

/**
 * Returns if a tiledb datatype is unsigned or not
 * @param type
 * @return
 */
bool tile::TileDBTypeIsUnsigned(tiledb_datatype_t type) {
  switch (type) {
  case tiledb_datatype_t::TILEDB_UINT8:
  case tiledb_datatype_t::TILEDB_UINT16:
  case tiledb_datatype_t::TILEDB_UINT32:
  case tiledb_datatype_t::TILEDB_UINT64:
    return true;

  default: {
    return false;
  }
  }
  return false;
}

/**
 * Returns if a tiedb datatype is a date/time type
 * @param type
 * @return
 */
bool tile::TileDBDateTimeType(tiledb_datatype_t type) {
  switch (type) {
  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH:
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK:
  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
  case tiledb_datatype_t::TILEDB_DATETIME_HR:
  case tiledb_datatype_t::TILEDB_DATETIME_MIN:
  case tiledb_datatype_t::TILEDB_DATETIME_SEC:
  case tiledb_datatype_t::TILEDB_DATETIME_MS:
  case tiledb_datatype_t::TILEDB_DATETIME_US:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS:
    return true;
  default:
    return false;
  }
}

/**
 * Returns if a mysql tpye is a blob
 * @param type
 * @return
 */
bool tile::MysqlBlobType(enum_field_types type) {
  switch (type) {
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
    return true;

  default: {
    return false;
  }
  }
  return false;
}

/**
 * Returns if a mysql type is a datetime
 * @param type
 * @return
 */
bool tile::MysqlDatetimeType(enum_field_types type) {
  switch (type) {
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_TIMESTAMP2:
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_DATETIME2:
    return true;

  default: {
    return false;
  }
  }
  return false;
}

int64_t tile::MysqlTimeToTileDBTimeVal(THD *thd, const MYSQL_TIME &mysql_time,
                                       tiledb_datatype_t datatype) {
  if (datatype == tiledb_datatype_t::TILEDB_DATETIME_YEAR) {
    return mysql_time.year - 1970;

  } else if (datatype == tiledb_datatype_t::TILEDB_DATETIME_MONTH) {
    MYSQL_TIME diff_time;
    calc_time_diff(&epoch, &mysql_time, 1, &diff_time, date_mode_t(0));
    int64_t year_diff = diff_time.hour / (24 * 365);
    return year_diff * 12;

  } else if (datatype == tiledb_datatype_t::TILEDB_DATETIME_WEEK) {
    MYSQL_TIME diff_time;
    calc_time_diff(&epoch, &mysql_time, 1, &diff_time, date_mode_t(0));
    return diff_time.hour / (7 * 24);

  } else if (datatype == tiledb_datatype_t::TILEDB_DATETIME_DAY) {
    uint32_t not_used;
    // Since we are only using the day portion we want to ignore any TZ
    // conversions by assuming it is already in UTC
    my_time_t seconds = my_tz_OFFSET0->TIME_to_gmt_sec(&mysql_time, &not_used);
    return seconds / (60 * 60 * 24);

  } else {
    my_time_t seconds = 0;
    // if we only have the time part
    if (mysql_time.year == 0 && mysql_time.month == 0 && mysql_time.day == 0) {
      seconds = (mysql_time.hour * 60 * 60) + (mysql_time.minute * 60) +
                mysql_time.second;
      // else we have a date and time which must take tz into account
    } else {
      uint32_t not_used;
      seconds = my_tz_OFFSET0->TIME_to_gmt_sec(&mysql_time, &not_used);
    }
    uint64_t microseconds = mysql_time.second_part;

    switch (datatype) {
    case tiledb_datatype_t::TILEDB_DATETIME_HR:
      return (seconds / 60 / 60);
    case tiledb_datatype_t::TILEDB_DATETIME_MIN:
      return (seconds / 60);
    case tiledb_datatype_t::TILEDB_DATETIME_SEC:
      return seconds;
    case tiledb_datatype_t::TILEDB_DATETIME_MS:
      return (seconds * 1000) + (microseconds / 1000);
    case tiledb_datatype_t::TILEDB_DATETIME_US:
      return (seconds * 1000000 + microseconds);
    case tiledb_datatype_t::TILEDB_DATETIME_NS:
      return (seconds * 1000000 + microseconds) * 1000;
    case tiledb_datatype_t::TILEDB_DATETIME_PS:
      return (seconds * 1000000 + microseconds) * 1000000;
    case tiledb_datatype_t::TILEDB_DATETIME_FS:
      return (seconds * 1000000 + microseconds) * 1000000000;
    case tiledb_datatype_t::TILEDB_DATETIME_AS:
      return (seconds * 1000000 + microseconds) * 1000000000000;
    default:
      my_printf_error(ER_UNKNOWN_ERROR,
                      "Unknown tiledb data type in MysqlTimeToTileDBTimeVal",
                      ME_ERROR_LOG | ME_FATAL);
    }
  }

  return 0;
}

tiledb::Attribute
tile::create_field_attribute(tiledb::Context &ctx, Field *field,
                             const tiledb::FilterList &filter_list) {

  tiledb_datatype_t datatype =
      tile::mysqlTypeToTileDBType(field->type(), false);
  tiledb::Attribute attr(ctx, field->field_name.str, datatype);

  // Only support variable length strings and blobs for now
  if ((datatype == tiledb_datatype_t::TILEDB_CHAR ||
       datatype == tiledb_datatype_t::TILEDB_STRING_ASCII) &&
      field->char_length() > 1) {
    const char *datatype_str;
    tiledb_datatype_to_str(datatype, &datatype_str);
    attr.set_cell_val_num(TILEDB_VAR_NUM);
  } else if (MysqlBlobType(field->type())) {
    const char *datatype_str;
    tiledb_datatype_to_str(datatype, &datatype_str);
    attr.set_cell_val_num(TILEDB_VAR_NUM);
  }

  if (filter_list.nfilters() > 0) {
    attr.set_filter_list(filter_list);
  }

  return attr;
}

tiledb::Dimension tile::create_field_dimension(tiledb::Context &ctx,
                                               Field *field) {
  switch (field->type()) {

  case MYSQL_TYPE_DOUBLE:
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_NEWDECIMAL: {
    return create_dim<double>(ctx, field, tiledb_datatype_t::TILEDB_FLOAT64);
  }

  case MYSQL_TYPE_FLOAT: {
    return create_dim<float>(ctx, field, tiledb_datatype_t::TILEDB_FLOAT32);
  }

  case MYSQL_TYPE_TINY: {
    if ((dynamic_cast<Field_num *>(field))->unsigned_flag) {
      return create_dim<uint8_t>(ctx, field, tiledb_datatype_t::TILEDB_UINT8);
    }
    // signed
    return create_dim<int8_t>(ctx, field, tiledb_datatype_t::TILEDB_INT8);
  }

  case MYSQL_TYPE_SHORT: {
    if ((dynamic_cast<Field_num *>(field))->unsigned_flag) {
      return create_dim<uint16_t>(ctx, field, tiledb_datatype_t::TILEDB_UINT16);
    }
    // signed
    return create_dim<int16_t>(ctx, field, tiledb_datatype_t::TILEDB_INT16);
  }
  case MYSQL_TYPE_YEAR: {
    return create_dim<int64_t>(ctx, field,
                               tiledb_datatype_t::TILEDB_DATETIME_YEAR);
  }

  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_INT24: {
    if ((dynamic_cast<Field_num *>(field))->unsigned_flag) {
      return create_dim<uint32_t>(ctx, field, tiledb_datatype_t::TILEDB_UINT32);
    }
    // signed
    return create_dim<int32_t>(ctx, field, tiledb_datatype_t::TILEDB_INT32);
  }

  case MYSQL_TYPE_LONGLONG: {
    if ((dynamic_cast<Field_num *>(field))->unsigned_flag) {
      return create_dim<uint64_t>(ctx, field, tiledb_datatype_t::TILEDB_UINT64);
    }
    // signed
    return create_dim<int64_t>(ctx, field, tiledb_datatype_t::TILEDB_INT64);
  }

  case MYSQL_TYPE_NULL:
  case MYSQL_TYPE_BIT: {
    return create_dim<uint8_t>(ctx, field, tiledb_datatype_t::TILEDB_INT8);
  }

  case MYSQL_TYPE_VARCHAR:
  case MYSQL_TYPE_STRING:
  case MYSQL_TYPE_VAR_STRING:
  case MYSQL_TYPE_SET: {
    return tiledb::Dimension::create(ctx, field->field_name.str,
                                     TILEDB_STRING_ASCII, nullptr, nullptr);
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
  case MYSQL_TYPE_DATE: {
    return create_dim<int64_t>(ctx, field,
                               tiledb_datatype_t::TILEDB_DATETIME_DAY);
  }
  case MYSQL_TYPE_TIME:
  case MYSQL_TYPE_TIME2: {
    return create_dim<int64_t>(ctx, field,
                               tiledb_datatype_t::TILEDB_DATETIME_US);
  }
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_DATETIME2:
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_TIMESTAMP2:
  case MYSQL_TYPE_NEWDATE: {
    // TODO: Need to figure out how to get decimal types
    return create_dim<int64_t>(ctx, field,
                               tiledb_datatype_t::TILEDB_DATETIME_US);
  }
  default: {
    sql_print_error(
        "Unknown mysql data type in creating tiledb field attribute");
    break;
  }
  }
  return tiledb::Dimension::create<uint8_t>(ctx, field->field_name.str,
                                            std::array<uint8, 2>{0, 0}, 10);
}
void *tile::alloc_buffer(tiledb_datatype_t type, uint64_t size) {
  // Round the size to the nearest unit for the datatype using integer division
  uint64_t rounded_size =
      size / tiledb_datatype_size(type) * tiledb_datatype_size(type);
  switch (type) {
  case tiledb_datatype_t::TILEDB_FLOAT64:
    return alloc_buffer<double>(rounded_size);

  case tiledb_datatype_t::TILEDB_FLOAT32:
    return alloc_buffer<float>(rounded_size);

  case tiledb_datatype_t::TILEDB_INT8:
    return alloc_buffer<int8_t>(rounded_size);

  case tiledb_datatype_t::TILEDB_UINT8:
    return alloc_buffer<uint8_t>(rounded_size);

  case tiledb_datatype_t::TILEDB_INT16:
    return alloc_buffer<int16_t>(rounded_size);

  case tiledb_datatype_t::TILEDB_UINT16:
    return alloc_buffer<uint16_t>(rounded_size);

  case tiledb_datatype_t::TILEDB_INT32:
    return alloc_buffer<int32_t>(rounded_size);

  case tiledb_datatype_t::TILEDB_UINT32:
    return alloc_buffer<uint32_t>(rounded_size);

  case tiledb_datatype_t::TILEDB_INT64:
    return alloc_buffer<int64_t>(rounded_size);

  case tiledb_datatype_t::TILEDB_UINT64:
    return alloc_buffer<uint64_t>(rounded_size);

  case tiledb_datatype_t::TILEDB_CHAR:
  case tiledb_datatype_t::TILEDB_STRING_ASCII:
    return alloc_buffer<char>(rounded_size);

  case tiledb_datatype_t::TILEDB_STRING_UTF8:
    return alloc_buffer<uint8_t>(rounded_size);

  case tiledb_datatype_t::TILEDB_STRING_UTF16:
    return alloc_buffer<uint16_t>(rounded_size);

  case tiledb_datatype_t::TILEDB_STRING_UTF32:
    return alloc_buffer<uint32_t>(rounded_size);

  case tiledb_datatype_t::TILEDB_STRING_UCS2:
    return alloc_buffer<uint16_t>(rounded_size);

  case tiledb_datatype_t::TILEDB_STRING_UCS4:
    return alloc_buffer<uint32_t>(rounded_size);

  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH:
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK:
  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
  case tiledb_datatype_t::TILEDB_DATETIME_HR:
  case tiledb_datatype_t::TILEDB_DATETIME_MIN:
  case tiledb_datatype_t::TILEDB_DATETIME_SEC:
  case tiledb_datatype_t::TILEDB_DATETIME_MS:
  case tiledb_datatype_t::TILEDB_DATETIME_US:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS:
    return alloc_buffer<int64_t>(rounded_size);

  default: {
    my_printf_error(ER_UNKNOWN_ERROR,
                    "Unknown tiledb data type in allocating buffers",
                    ME_ERROR_LOG | ME_FATAL);
  }
  }
  return 0;
}

bool tile::set_field_null_from_validity(std::shared_ptr<buffer> &buff,
                                        Field *field, uint64_t i) {
  if (buff->validity_buffer != nullptr) {
    if (buff->validity_buffer[i] == 0) {
      field->set_null();
      return true;
    }
  }
  field->set_notnull();
  return false;
}

int tile::set_datetime_field(THD *thd, Field *field,
                             std::shared_ptr<buffer> &buff, uint64_t i,
                             uint64_t seconds, uint64_t second_part,
                             enum_mysql_timestamp_type type) {

  if (set_field_null_from_validity(buff, field, i)) {
    return 0;
  }

  MYSQL_TIME to;
  if (type != MYSQL_TIMESTAMP_DATE) {
    thd->variables.time_zone->gmt_sec_to_TIME(&to, seconds);
    thd->time_zone_used = true;
    to.second_part = second_part;
    adjust_time_range_with_warn(thd, &to, TIME_SECOND_PART_DIGITS);
  } else {
    // For dates we can't convert tz, so use UTC
    my_tz_OFFSET0->gmt_sec_to_TIME(&to, seconds);
  }
  to.time_type = type;

  return field->store_time(&to);
}

int tile::set_field(THD *thd, Field *field, std::shared_ptr<buffer> &buff,
                    uint64_t i) {
  const char *str;
  tiledb_datatype_to_str(buff->type, &str);
  switch (buff->type) {
  /** 8-bit signed integer */
  case TILEDB_INT8:
    return set_field<int8_t>(field, buff, i);
  /** 8-bit unsigned integer */
  case TILEDB_UINT8:
    return set_field<uint8_t>(field, buff, i);
  /** 16-bit signed integer */
  case TILEDB_INT16:
    return set_field<int16_t>(field, buff, i);
  /** 16-bit unsigned integer */
  case TILEDB_UINT16:
    return set_field<uint16_t>(field, buff, i);
  /** 32-bit signed integer */
  case TILEDB_INT32:
    return set_field<int32_t>(field, buff, i);

  /** 32-bit unsigned integer */
  case TILEDB_UINT32:
    return set_field<uint32_t>(field, buff, i);
  /** 64-bit signed integer */
  case TILEDB_INT64:
    return set_field<int64_t>(field, buff, i);
  /** 64-bit unsigned integer */
  case TILEDB_UINT64:
    return set_field<uint64_t>(field, buff, i);
  /** 32-bit floating point value */
  case TILEDB_FLOAT32:
    return set_field<float>(field, buff, i);
  /** 64-bit floating point value */
  case TILEDB_FLOAT64:
    return set_field<double>(field, buff, i);

  /** Character */
  case TILEDB_CHAR:
    /** ASCII string */
  case TILEDB_STRING_ASCII:
    return set_string_field<char>(field, buff, i, &my_charset_latin1);

    /** UTF-8 string */
  case TILEDB_STRING_UTF8:
    return set_string_field<uint8_t>(field, buff, i, &my_charset_utf8_bin);

    /** UTF-16 string */
  case TILEDB_STRING_UTF16:
    return set_string_field<uint16_t>(field, buff, i, &my_charset_utf16_bin);

  /** UTF-32 string */
  case TILEDB_STRING_UTF32:
    return set_string_field<uint32_t>(field, buff, i, &my_charset_utf32_bin);

  /** UCS2 string */
  case TILEDB_STRING_UCS2:
    return set_string_field<uint16_t>(field, buff, i, &my_charset_ucs2_bin);

    /** UCS4 string */
  case TILEDB_STRING_UCS4:
    return set_string_field<uint32_t>(field, buff, i, &my_charset_utf32_bin);

    /** This can be any datatype. Must store (type tag, value) pairs. */
    // case TILEDB_ANY:
    //    return set_string_field<uint8_t>(field, buff, i,
    //    &my_charset_utf8_bin);

  case tiledb_datatype_t::TILEDB_DATETIME_YEAR: {
    if (set_field_null_from_validity(buff, field, i)) {
      return 0;
    }
    return field->store(static_cast<uint64_t *>(buff->buffer)[i] + 1970, false);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH: {
    // TODO: This isn't a good calculation we should fix it
    uint64_t seconds =
        static_cast<uint64_t *>(buff->buffer)[i] * (60 * 60 * 24 * 365) / 12;
    return set_datetime_field(thd, field, buff, i, seconds, 0,
                              MYSQL_TIMESTAMP_DATE);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK: {
    uint64_t seconds =
        static_cast<uint64_t *>(buff->buffer)[i] * (60 * 60 * 24 * 7);
    return set_datetime_field(thd, field, buff, i, seconds, 0,
                              MYSQL_TIMESTAMP_DATE);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_DAY: {
    uint64_t seconds =
        static_cast<uint64_t *>(buff->buffer)[i] * (60 * 60 * 24);
    return set_datetime_field(thd, field, buff, i, seconds, 0,
                              MYSQL_TIMESTAMP_DATE);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_HR: {
    uint64_t seconds = static_cast<uint64_t *>(buff->buffer)[i] * (60 * 60);
    return set_datetime_field(thd, field, buff, i, seconds, 0,
                              MYSQL_TIMESTAMP_DATETIME);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_MIN: {
    uint64_t seconds = static_cast<uint64_t *>(buff->buffer)[i] * 60;
    return set_datetime_field(thd, field, buff, i, seconds, 0,
                              MYSQL_TIMESTAMP_DATETIME);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_SEC: {
    uint64_t seconds = static_cast<uint64_t *>(buff->buffer)[i];
    return set_datetime_field(thd, field, buff, i, seconds, 0,
                              MYSQL_TIMESTAMP_DATETIME);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_MS: {
    uint64_t ms = static_cast<uint64_t *>(buff->buffer)[i];
    uint64_t seconds = ms / 1000;
    return set_datetime_field(thd, field, buff, i, seconds,
                              (ms - (seconds * 1000)) * 1000,
                              MYSQL_TIMESTAMP_DATETIME);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_US: {
    uint64_t us = static_cast<uint64_t *>(buff->buffer)[i];
    uint64_t seconds = us / 1000000;
    return set_datetime_field(thd, field, buff, i, seconds,
                              us - (seconds * 1000000),
                              MYSQL_TIMESTAMP_DATETIME);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_NS: {
    uint64_t ns = static_cast<uint64_t *>(buff->buffer)[i];
    uint64_t seconds = ns / 1000000000;
    return set_datetime_field(thd, field, buff, i, seconds,
                              (ns - (seconds * 1000000000)) / 1000,
                              MYSQL_TIMESTAMP_DATETIME);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_PS: {
    uint64_t ps = static_cast<uint64_t *>(buff->buffer)[i];
    uint64_t seconds = ps / 1000000000000;
    return set_datetime_field(thd, field, buff, i, seconds,
                              (ps - (seconds * 1000000000000)) / 1000000,
                              MYSQL_TIMESTAMP_DATETIME);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_FS: {
    uint64_t fs = static_cast<uint64_t *>(buff->buffer)[i];
    uint64_t seconds = fs / 1000000000000000;
    return set_datetime_field(thd, field, buff, i, seconds,
                              (fs - (seconds * 1000000000000000)) / 1000000000,
                              MYSQL_TIMESTAMP_DATETIME);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_AS: {
    uint64_t as = static_cast<uint64_t *>(buff->buffer)[i];
    uint64_t seconds = as / 1000000000000000000;
    return set_datetime_field(thd, field, buff, i, seconds,
                              (as - (seconds * 1000000000000000000)) /
                                  1000000000000,
                              MYSQL_TIMESTAMP_DATETIME);
  }
  default: {
    const char *type_str = nullptr;
    tiledb_datatype_to_str(buff->type, &type_str);
    my_printf_error(
        ER_UNKNOWN_ERROR,
        "Unknown or unsupported datatype for converting to MariaDB fields: %s",
        ME_ERROR_LOG | ME_FATAL, type_str);
    break;
  }
  }
  return 0;
}
int tile::set_buffer_from_field(Field *field, std::shared_ptr<buffer> &buff,
                                uint64_t i, THD *thd, bool check_null) {

  bool field_null = check_null ? field->is_null() : false;

  switch (buff->type) {

  /** 8-bit signed integer */
  case TILEDB_INT8:
    return set_buffer_from_field<int8_t>(field->val_int(), field_null, buff, i);

    /** 8-bit unsigned integer */
  case TILEDB_UINT8:
    return set_buffer_from_field<uint8_t>(field->val_uint(), field_null, buff,
                                          i);

    /** 16-bit signed integer */
  case TILEDB_INT16:
    return set_buffer_from_field<int16_t>(field->val_int(), field_null, buff,
                                          i);

    /** 16-bit unsigned integer */
  case TILEDB_UINT16:
    return set_buffer_from_field<uint16_t>(field->val_uint(), field_null, buff,
                                           i);

    /** 32-bit signed integer */
  case TILEDB_INT32:
    return set_buffer_from_field<int32_t>(field->val_int(), field_null, buff,
                                          i);

    /** 32-bit unsigned integer */
  case TILEDB_UINT32:
    return set_buffer_from_field<uint32_t>(field->val_uint(), field_null, buff,
                                           i);

    /** 64-bit signed integer */
  case TILEDB_INT64:
    return set_buffer_from_field<int64_t>(field->val_int(), field_null, buff,
                                          i);

    /** 64-bit unsigned integer */
  case TILEDB_UINT64:
    return set_buffer_from_field<uint64_t>(field->val_uint(), field_null, buff,
                                           i);

    /** 32-bit floating point value */
  case TILEDB_FLOAT32:
    return set_buffer_from_field<float>(field->val_real(), field_null, buff, i);

    /** 64-bit floating point value */
  case TILEDB_FLOAT64:
    return set_buffer_from_field<double>(field->val_real(), field_null, buff,
                                         i);

    /** ASCII string */
  case TILEDB_STRING_ASCII:
    /** Character */
  case TILEDB_CHAR:
    if (buff->offset_buffer != nullptr)
      return set_string_buffer_from_field<char>(field, field_null, buff, i);

    return set_fixed_string_buffer_from_field<char>(field, field_null, buff, i);

  /** UTF-8 string */
  case TILEDB_STRING_UTF8:
    if (buff->offset_buffer != nullptr)
      return set_string_buffer_from_field<uint8_t>(field, field_null, buff, i);

    return set_fixed_string_buffer_from_field<uint8_t>(field, field_null, buff,
                                                       i);

  /** UTF-16 string */
  case TILEDB_STRING_UTF16:
    if (buff->offset_buffer != nullptr)
      return set_string_buffer_from_field<uint16_t>(field, field_null, buff, i);

    return set_fixed_string_buffer_from_field<uint16_t>(field, field_null, buff,
                                                        i);

  /** UTF-32 string */
  case TILEDB_STRING_UTF32:
    if (buff->offset_buffer != nullptr)
      return set_string_buffer_from_field<uint32_t>(field, field_null, buff, i);

    return set_fixed_string_buffer_from_field<uint32_t>(field, field_null, buff,
                                                        i);
  /** UCS2 string */
  case TILEDB_STRING_UCS2:
    return set_string_field<uint16_t>(field, buff, i, &my_charset_ucs2_bin);
    if (buff->offset_buffer != nullptr)
      return set_string_buffer_from_field<uint16_t>(field, field_null, buff, i);

    return set_fixed_string_buffer_from_field<uint16_t>(field, field_null, buff,
                                                        i);

    /** UCS4 string */
  case TILEDB_STRING_UCS4:
    if (buff->offset_buffer != nullptr)
      return set_string_buffer_from_field<uint32_t>(field, field_null, buff, i);

    return set_fixed_string_buffer_from_field<uint32_t>(field, field_null, buff,
                                                        i);

    /** This can be any datatype. Must store (type tag, value) pairs. */
    // case TILEDB_ANY:
    //    return set_string_field<uint8_t>(field, buff, i,
    //    &my_charset_utf8_bin);

  case tiledb_datatype_t::TILEDB_DATETIME_YEAR: {
    MYSQL_TIME year = {static_cast<uint32_t>(field->val_int()),
                       0,
                       0,
                       0,
                       0,
                       0,
                       0,
                       0,
                       MYSQL_TIMESTAMP_TIME};

    int64_t xs = MysqlTimeToTileDBTimeVal(thd, year, buff->type);
    return set_buffer_from_field<int64_t>(xs, field_null, buff, i);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH: {
    MYSQL_TIME mysql_time;
    field->get_date(&mysql_time, date_mode_t(0));

    int64_t xs = MysqlTimeToTileDBTimeVal(thd, mysql_time, buff->type);
    return set_buffer_from_field<int64_t>(xs, field_null, buff, i);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK: {
    MYSQL_TIME mysql_time;
    field->get_date(&mysql_time, date_mode_t(0));

    int64_t xs = MysqlTimeToTileDBTimeVal(thd, mysql_time, buff->type);
    return set_buffer_from_field<int64_t>(xs, field_null, buff, i);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_DAY: {
    MYSQL_TIME mysql_time;
    field->get_date(&mysql_time, date_mode_t(0));

    int64_t xs = MysqlTimeToTileDBTimeVal(thd, mysql_time, buff->type);
    return set_buffer_from_field<int64_t>(xs, field_null, buff, i);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_HR:
  case tiledb_datatype_t::TILEDB_DATETIME_MIN:
  case tiledb_datatype_t::TILEDB_DATETIME_SEC:
  case tiledb_datatype_t::TILEDB_DATETIME_MS:
  case tiledb_datatype_t::TILEDB_DATETIME_US:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS: {
    MYSQL_TIME mysql_time;
    field->get_date(&mysql_time, date_mode_t(0));

    int64_t xs = MysqlTimeToTileDBTimeVal(thd, mysql_time, buff->type);
    return set_buffer_from_field<int64_t>(xs, field_null, buff, i);
  }
  default: {
    const char *type_str = nullptr;
    tiledb_datatype_to_str(buff->type, &type_str);
    my_printf_error(
        ER_UNKNOWN_ERROR,
        "Unknown or unsupported datatype for converting to MariaDB fields: %s",
        ME_ERROR_LOG | ME_FATAL, type_str);
    break;
  }
  }
  return 0;
}

tiledb::FilterList tile::parse_filter_list(tiledb::Context &ctx,
                                           const char *filter_csv) {
  std::vector<std::string> filters = split(filter_csv, ',');

  tiledb::FilterList filter_list(ctx);

  for (auto &filter_str : filters) {
    std::vector<std::string> f = split(filter_str, '=');
    tiledb_filter_type_t filter_type = TILEDB_FILTER_NONE;
    int err = tiledb_filter_type_from_str(f[0].c_str(), &filter_type);
    if (err != TILEDB_OK) {
      my_printf_error(ER_UNKNOWN_ERROR,
                      "Unknown or unsupported filter type: %s",
                      ME_ERROR_LOG | ME_FATAL, f[0].c_str());
    }

    tiledb::Filter filter(ctx, filter_type);
    if (f.size() > 1) {
      switch (filter_type) {
      case TILEDB_FILTER_BIT_WIDTH_REDUCTION: {
        auto value = parse_value<uint32_t>(f[1]);
        sql_print_information("TILEDB_BIT_WIDTH_MAX_WINDOW=%d", value);
        filter.set_option(TILEDB_BIT_WIDTH_MAX_WINDOW, value);
        break;
      }
      case TILEDB_FILTER_POSITIVE_DELTA: {
        auto value = parse_value<uint32_t>(f[1]);
        sql_print_information("TILEDB_POSITIVE_DELTA_MAX_WINDOW=%d", value);
        filter.set_option(TILEDB_POSITIVE_DELTA_MAX_WINDOW, value);
        break;
      }
      // The following have no filter options
      case TILEDB_FILTER_NONE:
      case TILEDB_FILTER_RLE:
      case TILEDB_FILTER_BITSHUFFLE:
      case TILEDB_FILTER_BYTESHUFFLE:
      case TILEDB_FILTER_DOUBLE_DELTA:
      case TILEDB_FILTER_CHECKSUM_MD5:
      case TILEDB_FILTER_CHECKSUM_SHA256:
        break;
      // Handle all compressions with default
      default: {
        auto value = parse_value<int32_t>(f[1]);
        filter.set_option(TILEDB_COMPRESSION_LEVEL, value);
        break;
      }
      }
    }
    filter_list.add_filter(filter);
  }
  return filter_list;
}

std::string tile::filter_list_to_str(const tiledb::FilterList &filter_list) {
  std::stringstream str;
  for (uint64_t i = 0; i < filter_list.nfilters(); i++) {
    tiledb::Filter filter = filter_list.filter(i);
    std::string filter_str = tiledb::Filter::to_str(filter.filter_type());
    str << filter_str;
    // NONE has no filter options
    switch (filter.filter_type()) {
    case TILEDB_FILTER_BIT_WIDTH_REDUCTION: {
      uint32_t value;
      filter.get_option<uint32_t>(TILEDB_BIT_WIDTH_MAX_WINDOW, &value);
      str << "=" << value << ",";
      break;
    }
    case TILEDB_FILTER_POSITIVE_DELTA: {
      uint32_t value;
      filter.get_option<uint32_t>(TILEDB_POSITIVE_DELTA_MAX_WINDOW, &value);
      str << "=" << value << ",";
      break;
    }
    // The following have no filter options
    case TILEDB_FILTER_NONE:
    case TILEDB_FILTER_RLE:
    case TILEDB_FILTER_BITSHUFFLE:
    case TILEDB_FILTER_BYTESHUFFLE:
    case TILEDB_FILTER_DOUBLE_DELTA:
    case TILEDB_FILTER_CHECKSUM_MD5:
    case TILEDB_FILTER_CHECKSUM_SHA256:
      str << ",";
      break;
    // Handle all compressions with default
    default: {
      int32_t value;
      filter.get_option<int32_t>(TILEDB_COMPRESSION_LEVEL, &value);
      str << "=" << value << ",";
      break;
    }
    }
  }
  std::string final = str.str();
  final.pop_back();
  return final;
}

const void *tile::default_tiledb_fill_value(const tiledb_datatype_t &type) {
  switch (type) {
  case tiledb_datatype_t::TILEDB_INT8:
    return &constants::empty_int8;
  case tiledb_datatype_t::TILEDB_UINT8:
    return &constants::empty_uint8;
  case tiledb_datatype_t::TILEDB_INT16:
    return &constants::empty_int16;
  case tiledb_datatype_t::TILEDB_UINT16:
    return &constants::empty_uint16;
  case tiledb_datatype_t::TILEDB_INT32:
    return &constants::empty_int32;
  case tiledb_datatype_t::TILEDB_UINT32:
    return &constants::empty_uint32;
  case tiledb_datatype_t::TILEDB_INT64:
    return &constants::empty_int64;
  case tiledb_datatype_t::TILEDB_UINT64:
    return &constants::empty_uint64;
  case tiledb_datatype_t::TILEDB_FLOAT32:
    return &constants::empty_float32;
  case tiledb_datatype_t::TILEDB_FLOAT64:
    return &constants::empty_float64;
  case tiledb_datatype_t::TILEDB_CHAR:
    return &constants::empty_char;
  case tiledb_datatype_t::TILEDB_ANY:
    return &constants::empty_any;
  case tiledb_datatype_t::TILEDB_STRING_ASCII:
    return &constants::empty_ascii;
  case tiledb_datatype_t::TILEDB_STRING_UTF8:
    return &constants::empty_utf8;
  case tiledb_datatype_t::TILEDB_STRING_UTF16:
    return &constants::empty_utf16;
  case tiledb_datatype_t::TILEDB_STRING_UTF32:
    return &constants::empty_utf32;
  case tiledb_datatype_t::TILEDB_STRING_UCS2:
    return &constants::empty_ucs2;
  case tiledb_datatype_t::TILEDB_STRING_UCS4:
    return &constants::empty_ucs4;
  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH:
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK:
  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
  case tiledb_datatype_t::TILEDB_DATETIME_HR:
  case tiledb_datatype_t::TILEDB_DATETIME_MIN:
  case tiledb_datatype_t::TILEDB_DATETIME_SEC:
  case tiledb_datatype_t::TILEDB_DATETIME_MS:
  case tiledb_datatype_t::TILEDB_DATETIME_US:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS:
    return &constants::empty_int64;
  }

  return nullptr;
}

bool tile::is_fill_value_default(const tiledb_datatype_t &type,
                                 const void *value, const uint64_t &size) {
  // Fill values are only a single value so if the size is larger is custom
  if (size > tiledb_datatype_size(type))
    return false;

  const void *default_value = default_tiledb_fill_value(type);

  return !memcmp(default_value, value, size);
}

bool tile::is_string_datatype(const tiledb_datatype_t &type) {
  switch (type) {
  case tiledb_datatype_t::TILEDB_INT8:
  case tiledb_datatype_t::TILEDB_UINT8:
  case tiledb_datatype_t::TILEDB_INT16:
  case tiledb_datatype_t::TILEDB_UINT16:
  case tiledb_datatype_t::TILEDB_INT32:
  case tiledb_datatype_t::TILEDB_UINT32:
  case tiledb_datatype_t::TILEDB_INT64:
  case tiledb_datatype_t::TILEDB_UINT64:
  case tiledb_datatype_t::TILEDB_FLOAT32:
  case tiledb_datatype_t::TILEDB_FLOAT64:
  case tiledb_datatype_t::TILEDB_ANY:
  case tiledb_datatype_t::TILEDB_DATETIME_YEAR:
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH:
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK:
  case tiledb_datatype_t::TILEDB_DATETIME_DAY:
  case tiledb_datatype_t::TILEDB_DATETIME_HR:
  case tiledb_datatype_t::TILEDB_DATETIME_MIN:
  case tiledb_datatype_t::TILEDB_DATETIME_SEC:
  case tiledb_datatype_t::TILEDB_DATETIME_MS:
  case tiledb_datatype_t::TILEDB_DATETIME_US:
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS:
    return false;
  case tiledb_datatype_t::TILEDB_CHAR:
  case tiledb_datatype_t::TILEDB_STRING_ASCII:
  case tiledb_datatype_t::TILEDB_STRING_UTF8:
  case tiledb_datatype_t::TILEDB_STRING_UTF16:
  case tiledb_datatype_t::TILEDB_STRING_UTF32:
  case tiledb_datatype_t::TILEDB_STRING_UCS2:
  case tiledb_datatype_t::TILEDB_STRING_UCS4:
    return true;
  }

  return true;
}

tile::BufferSizeByType tile::compute_buffer_sizes(
    std::vector<std::tuple<tiledb_datatype_t, bool, bool, bool>> &field_types,
    const uint64_t &memory_budget) {
  // Get count of number of query buffers being allocated
  size_t num_weighted_buffers = 0;
  // Note: we count signed/unsigned as the same because we are really just
  // concerned about bytes size We leave float and strings for convenience
  uint64_t num_char_buffers = 0;
  uint64_t num_int8_buffers = 0;
  uint64_t num_int16_buffers = 0;
  uint64_t num_int32_buffers = 0;
  uint64_t num_int64_buffers = 0;
  uint64_t num_float32_buffers = 0;
  uint64_t num_float64_buffers = 0;
  uint64_t num_var_length_uint8_buffers = 0;

  for (const auto &field_type : field_types) {
    tiledb_datatype_t datatype = std::get<0>(field_type);
    bool var_len = std::get<1>(field_type);
    bool nullable = std::get<2>(field_type);
    bool list = std::get<3>(field_type);

    if (var_len) {
      num_int64_buffers += 1;
      num_var_length_uint8_buffers += 1;
    }

    // Currently we don't support list buffers in TileDB schema but this will
    // just work when we do
    if (list) {
      num_int64_buffers += 1;
    }

    // nullables use a uint8 buffer
    if (nullable) {
      num_int8_buffers += 1;
    }

    // For non var length we want to count the datatype
    if (!var_len) {
      switch (datatype) {
      case TILEDB_UINT32:
      case TILEDB_INT32:
        num_int32_buffers += 1;
        break;
      case TILEDB_FLOAT32:
        num_float32_buffers += 1;
        break;
      case TILEDB_FLOAT64:
        num_float64_buffers += 1;
        break;
      case TILEDB_STRING_UTF8:
      case TILEDB_STRING_ASCII:
      case TILEDB_CHAR:
        num_char_buffers += 1;
        break;
      case TILEDB_UINT8:
      case TILEDB_INT8:
      case TILEDB_ANY:
        num_int8_buffers += 1;
        break;
      case TILEDB_INT16:
      case TILEDB_UINT16:
      case TILEDB_STRING_UTF16:
      case TILEDB_STRING_UCS2:
        num_int16_buffers += 1;
        break;
      case TILEDB_INT64:
      case TILEDB_UINT64:
      case TILEDB_STRING_UTF32:
      case TILEDB_STRING_UCS4:
      case TILEDB_DATETIME_YEAR:
      case TILEDB_DATETIME_MONTH:
      case TILEDB_DATETIME_WEEK:
      case TILEDB_DATETIME_DAY:
      case TILEDB_DATETIME_HR:
      case TILEDB_DATETIME_MIN:
      case TILEDB_DATETIME_SEC:
      case TILEDB_DATETIME_MS:
      case TILEDB_DATETIME_US:
      case TILEDB_DATETIME_NS:
      case TILEDB_DATETIME_PS:
      case TILEDB_DATETIME_FS:
      case TILEDB_DATETIME_AS:
        num_int64_buffers += 1;
        break;
      default: {
        const char *datatype_str;
        tiledb_datatype_to_str(datatype, &datatype_str);
        throw std::runtime_error(
            "Unsupported datatype in compute_buffer_size: " +
            std::string(datatype_str));
      }
      }
    }
  }

  num_weighted_buffers = num_char_buffers + num_int8_buffers +
                         (num_int16_buffers * sizeof(int16_t)) +
                         (num_int32_buffers * sizeof(int32_t)) +
                         (num_int64_buffers * sizeof(uint64_t)) +
                         (num_float32_buffers * sizeof(float)) +
                         (num_float64_buffers * sizeof(double)) +
                         (num_var_length_uint8_buffers * sizeof(uint64_t));

  // Every buffer alloc gets the same size.
  uint64_t nbytes = memory_budget / num_weighted_buffers;

  // Requesting 0 MB will result in a 1 KB allocation. This is used by the
  // tests to test the path of incomplete TileDB queries.
  if (memory_budget == 0) {
    nbytes = 1024;
  }

  tile::BufferSizeByType sizes(
      nbytes, nbytes, nbytes, nbytes * sizeof(uint16_t),
      nbytes * sizeof(int16_t), nbytes * sizeof(int32_t),
      nbytes * sizeof(uint32_t), nbytes * sizeof(uint64_t),
      nbytes * sizeof(int64_t), nbytes * sizeof(float), nbytes * sizeof(double),
      nbytes * sizeof(uint64_t));

  return sizes;
}

/** The special value for an empty int32. */
const int tile::constants::empty_int32 = std::numeric_limits<int32_t>::min();

/** The special value for an empty int64. */
const int64_t tile::constants::empty_int64 =
    std::numeric_limits<int64_t>::min();

/** The special value for an empty float32. */
const float tile::constants::empty_float32 =
    std::numeric_limits<float>::quiet_NaN();

/** The special value for an empty float64. */
const double tile::constants::empty_float64 =
    std::numeric_limits<double>::quiet_NaN();

/** The special value for an empty char. */
const char tile::constants::empty_char = std::numeric_limits<char>::min();

/** The special value for an empty int8. */
const int8_t tile::constants::empty_int8 = std::numeric_limits<int8_t>::min();

/** The special value for an empty uint8. */
const uint8_t tile::constants::empty_uint8 =
    std::numeric_limits<uint8_t>::max();

/** The special value for an empty int16. */
const int16_t tile::constants::empty_int16 =
    std::numeric_limits<int16_t>::min();

/** The special value for an empty uint16. */
const uint16_t tile::constants::empty_uint16 =
    std::numeric_limits<uint16_t>::max();

/** The special value for an empty uint32. */
const uint32_t tile::constants::empty_uint32 =
    std::numeric_limits<uint32_t>::max();

/** The special value for an empty uint64. */
const uint64_t tile::constants::empty_uint64 =
    std::numeric_limits<uint64_t>::max();

/** The special value for an empty ASCII. */
const uint8_t tile::constants::empty_ascii = 0;

/** The special value for an empty UTF8. */
const uint8_t tile::constants::empty_utf8 = 0;

/** The special value for an empty UTF16. */
const uint16_t tile::constants::empty_utf16 = 0;

/** The special value for an empty UTF32. */
const uint32_t tile::constants::empty_utf32 = 0;

/** The special value for an empty UCS2. */
const uint16_t tile::constants::empty_ucs2 = 0;

/** The special value for an empty UCS4. */
const uint32_t tile::constants::empty_ucs4 = 0;

/** The special value for an empty ANY. */
const uint8_t tile::constants::empty_any = 0;