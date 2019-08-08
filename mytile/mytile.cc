/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#include "mytile.h"
#include <log.h>
#include <my_global.h>
#include <mysqld_error.h>

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

  case MYSQL_TYPE_INT24: {
    if (signedInt)
      return tiledb_datatype_t::TILEDB_INT32;
    return tiledb_datatype_t::TILEDB_UINT32;
  }

  case MYSQL_TYPE_LONG:
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
    return tiledb_datatype_t::TILEDB_CHAR;
  }

  case MYSQL_TYPE_GEOMETRY:
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_ENUM: {
    return tiledb_datatype_t::TILEDB_UINT8;
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
    return "VARCHAR";
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
    return "DATE";

  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_DATETIME2:
    return "DATETIME";

  case MYSQL_TYPE_TIME:
  case MYSQL_TYPE_TIME2:
    return "TIME";
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_TIMESTAMP2:
    return "TIMESTAMP";
  case MYSQL_TYPE_NEWDATE:
    return "DATETIME";
  default: {
    sql_print_error("Unknown mysql data type in determining string");
  }
    return "";
  }
}

int tile::TileDBTypeToMysqlType(tiledb_datatype_t type) {
  switch (type) {

  case tiledb_datatype_t::TILEDB_FLOAT64: {
    return MYSQL_TYPE_DOUBLE;
  }

  case tiledb_datatype_t::TILEDB_FLOAT32: {
    return MYSQL_TYPE_FLOAT;
  }

  case tiledb_datatype_t::TILEDB_INT8:
  case tiledb_datatype_t::TILEDB_UINT8: {
    return MYSQL_TYPE_TINY;
  }

  case tiledb_datatype_t::TILEDB_INT16:
  case tiledb_datatype_t::TILEDB_UINT16: {
    return MYSQL_TYPE_SHORT;
  }
  case tiledb_datatype_t::TILEDB_INT32:
  case tiledb_datatype_t::TILEDB_UINT32: {
    return MYSQL_TYPE_INT24;
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

  case tiledb_datatype_t::TILEDB_DATETIME_SEC:
  case tiledb_datatype_t::TILEDB_DATETIME_MIN:
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

tiledb::Attribute
tile::create_field_attribute(tiledb::Context &ctx, Field *field,
                             const tiledb::FilterList &filterList) {

  tiledb_datatype_t datatype =
      tile::mysqlTypeToTileDBType(field->type(), false);
  tiledb::Attribute attr(ctx, field->field_name.str, datatype);

  // Only support variable length strings and blobs for now
  if ((datatype == tiledb_datatype_t::TILEDB_CHAR ||
       datatype == tiledb_datatype_t::TILEDB_STRING_ASCII) &&
      field->char_length() > 1) {
    const char *datatype_str;
    tiledb_datatype_to_str(datatype, &datatype_str);
    std::cerr << "Setting " << field->field_name.str
              << " as variable length because datatype is " << datatype_str
              << " and field->char_length()= " << field->char_length()
              << std::endl;
    attr.set_cell_val_num(TILEDB_VAR_NUM);
  } else if (field->type() == MYSQL_TYPE_TINY_BLOB ||
             field->type() == MYSQL_TYPE_BLOB ||
             field->type() == MYSQL_TYPE_MEDIUM_BLOB ||
             field->type() == MYSQL_TYPE_LONG_BLOB) {
    const char *datatype_str;
    tiledb_datatype_to_str(datatype, &datatype_str);
    std::cerr << "Setting " << field->field_name.str
              << " as variable length because datatype is " << datatype_str
              << " and field->char_length()= " << field->char_length()
              << std::endl;
    attr.set_cell_val_num(TILEDB_VAR_NUM);
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
    std::cerr << "using int8_t" << std::endl;
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

  case MYSQL_TYPE_INT24: {
    if ((dynamic_cast<Field_num *>(field))->unsigned_flag) {
      return create_dim<uint32_t>(ctx, field, tiledb_datatype_t::TILEDB_UINT32);
    }
    // signed
    return create_dim<int32_t>(ctx, field, tiledb_datatype_t::TILEDB_INT32);
  }

  case MYSQL_TYPE_LONG:
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
  return tiledb::Dimension::create<uint8>(ctx, field->field_name.str,
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
    return alloc_buffer<char>(rounded_size);

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

uint64_t tile::computeRecordsUB(std::shared_ptr<tiledb::Array> &array,
                                void *subarray) {
  switch (array->schema().domain().type()) {
  case tiledb_datatype_t::TILEDB_INT8: {
    return computeRecordsUB<int8_t>(array, subarray);
  }
  case tiledb_datatype_t::TILEDB_UINT8: {
    return computeRecordsUB<uint8_t>(array, subarray);
  }
  case tiledb_datatype_t::TILEDB_INT16: {
    return computeRecordsUB<int16_t>(array, subarray);
  }
  case tiledb_datatype_t::TILEDB_UINT16: {
    return computeRecordsUB<uint16_t>(array, subarray);
  }
  case tiledb_datatype_t::TILEDB_INT32: {
    return computeRecordsUB<int32_t>(array, subarray);
  }
  case tiledb_datatype_t::TILEDB_UINT32: {
    return computeRecordsUB<uint32_t>(array, subarray);
  }
  case tiledb_datatype_t::TILEDB_INT64: {
    return computeRecordsUB<int64_t>(array, subarray);
  }
  case tiledb_datatype_t::TILEDB_UINT64: {
    return computeRecordsUB<uint64_t>(array, subarray);
  }
  case tiledb_datatype_t::TILEDB_FLOAT32: {
    return computeRecordsUB<float>(array, subarray);
  }
  case tiledb_datatype_t::TILEDB_FLOAT64: {
    return computeRecordsUB<double>(array, subarray);
  }
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
  case tiledb_datatype_t::TILEDB_DATETIME_AS: {
    return computeRecordsUB<int64_t>(array, subarray);
  }
  case tiledb_datatype_t::TILEDB_CHAR:
  case tiledb_datatype_t::TILEDB_STRING_ASCII:
  case tiledb_datatype_t::TILEDB_STRING_UTF8:
  case tiledb_datatype_t::TILEDB_STRING_UTF16:
  case tiledb_datatype_t::TILEDB_STRING_UTF32:
  case tiledb_datatype_t::TILEDB_STRING_UCS2:
  case tiledb_datatype_t::TILEDB_STRING_UCS4:
  case tiledb_datatype_t::TILEDB_ANY:
    my_printf_error(ER_UNKNOWN_ERROR, "Unknown dimension type",
                    ME_ERROR_LOG | ME_FATAL);
    break;
  }
  return 0;
}

int tile::set_datetime_field(THD *thd, Field *field, INTERVAL interval) {
  MYSQL_TIME local_epoch = epoch;

  date_add_interval(thd, &local_epoch, INTERVAL_YEAR, interval);
  return field->store_time(&local_epoch);
}

int tile::set_field(THD *thd, Field *field, std::shared_ptr<buffer> &buff,
                    uint64_t i) {
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
    return set_string_field<char>(field, buff, i, &my_charset_latin1);

  /** ASCII string */
  case TILEDB_STRING_ASCII:
    return set_string_field<char>(field, buff, i, &my_charset_latin1);
    // Only char is supported for now
    /*


 */ /** UTF-8 string */ /*
case TILEDB_STRING_UTF8:
return set_string_field<uint8_t>(field, buff, i, &my_charset_utf8_bin);

*/                      /** UTF-16 string */
                        /*
case TILEDB_STRING_UTF16:
  return set_string_field<uint16_t>(field, buff, i, &my_charset_utf16_bin);
                    
  */ /** UTF-32 string */ /*
case TILEDB_STRING_UTF32:
return set_string_field<uint32_t>(field, buff, i, &my_charset_utf32_bin);

*/ /** UCS2 string */   /*
case TILEDB_STRING_UCS2:
    return set_string_field<uint16_t>(field, buff, i, &my_charset_ucs2_bin);
  
    */
    /** UCS4 string */  /*
case TILEDB_STRING_UCS4:
    return set_string_field<uint32_t>(field, buff, i, &my_charset_utf32_bin);*/

    /** This can be any datatype. Must store (type tag, value) pairs. */
    // case TILEDB_ANY:
    //    return set_string_field<uint8_t>(field, buff, i,
    //    &my_charset_utf8_bin);

  case tiledb_datatype_t::TILEDB_DATETIME_YEAR: {
    /*INTERVAL interval;
    interval.year = static_cast<uint64_t *>(buff->buffer)[i];
    return set_datetime_field(thd, field, interval);*/
    return set_field<int64_t>(field, buff, i);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH: {
    INTERVAL interval;
    interval.month = static_cast<uint64_t *>(buff->buffer)[i];
    return set_datetime_field(thd, field, interval);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK: {
    INTERVAL interval;
    interval.day = static_cast<uint64_t *>(buff->buffer)[i] * 7;
    return set_datetime_field(thd, field, interval);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_DAY: {
    INTERVAL interval;
    interval.day = static_cast<uint64_t *>(buff->buffer)[i];
    return set_datetime_field(thd, field, interval);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_HR: {
    INTERVAL interval;
    interval.hour = static_cast<uint64_t *>(buff->buffer)[i];
    return set_datetime_field(thd, field, interval);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_MIN: {
    INTERVAL interval;
    interval.minute = static_cast<uint64_t *>(buff->buffer)[i];
    return set_datetime_field(thd, field, interval);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_SEC: {
    INTERVAL interval;
    interval.second = static_cast<uint64_t *>(buff->buffer)[i];
    return set_datetime_field(thd, field, interval);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_MS: {
    INTERVAL interval;
    interval.second_part = static_cast<uint64_t *>(buff->buffer)[i];
    return set_datetime_field(thd, field, interval);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_US: {
    INTERVAL interval;
    interval.second_part = static_cast<uint64_t *>(buff->buffer)[i];
    return set_datetime_field(thd, field, interval);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS: {
    my_printf_error(
        ER_UNKNOWN_ERROR,
        "Unsupported datetime for MariaDB. US/NS/PS/FS/AS not supported",
        ME_ERROR_LOG | ME_FATAL);
    break;
  }
  default: {
    const char *type_str = nullptr;
    tiledb_datatype_to_str(buff->type, &type_str);
    my_printf_error(
        ER_UNKNOWN_ERROR,
        "Unknown or unsupported datatype for converting to MariaDB fields: %s",
        ME_ERROR_LOG | ME_FATAL, type_str);
    if (type_str != nullptr) {
      delete type_str;
    }
    break;
  }
  }
  return 0;
}
int tile::set_buffer_from_field(Field *field, std::shared_ptr<buffer> &buff,
                                uint64_t i, THD *thd) {
  switch (buff->type) {

  /** 8-bit signed integer */
  case TILEDB_INT8:
    std::cerr << "type is int8 " << std::endl;
    return set_buffer_from_field<int8_t>(field->val_int(), buff, i);

    /** 8-bit unsigned integer */
  case TILEDB_UINT8:
    std::cerr << "type is uint8 " << std::endl;
    return set_buffer_from_field<uint8_t>(field->val_uint(), buff, i);

    /** 16-bit signed integer */
  case TILEDB_INT16:
    std::cerr << "type is int16 " << std::endl;
    return set_buffer_from_field<int16_t>(field->val_int(), buff, i);

    /** 16-bit unsigned integer */
  case TILEDB_UINT16:
    std::cerr << "type is uint16 " << std::endl;
    return set_buffer_from_field<uint16_t>(field->val_uint(), buff, i);

    /** 32-bit signed integer */
  case TILEDB_INT32:
    return set_buffer_from_field<int32_t>(field->val_int(), buff, i);

    /** 32-bit unsigned integer */
  case TILEDB_UINT32:
    return set_buffer_from_field<uint32_t>(field->val_uint(), buff, i);

    /** 64-bit signed integer */
  case TILEDB_INT64:
    return set_buffer_from_field<int64_t>(field->val_int(), buff, i);

    /** 64-bit unsigned integer */
  case TILEDB_UINT64:
    return set_buffer_from_field<uint64_t>(field->val_uint(), buff, i);

    /** 32-bit floating point value */
  case TILEDB_FLOAT32:
    return set_buffer_from_field<float>(field->val_real(), buff, i);

    /** 64-bit floating point value */
  case TILEDB_FLOAT64:
    return set_buffer_from_field<double>(field->val_real(), buff, i);

    /** Character */
  case TILEDB_CHAR:
    if (buff->offset_buffer != nullptr)
      return set_string_buffer_from_field<char>(field, buff, i);

    return set_fixed_string_buffer_from_field<char>(field, buff, i);

    // Only char is supported for now
    /*    */ /** ASCII string */
    /*
case TILEDB_STRING_ASCII:
return set_string_field<uint8_t>(field, buff, i, &my_charset_latin1);

*/ /** UTF-8 string */ /*
case TILEDB_STRING_UTF8:
return set_string_field<uint8_t>(field, buff, i, &my_charset_utf8_bin);

*/                     /** UTF-16 string */
    /*
case TILEDB_STRING_UTF16:
return set_string_field<uint16_t>(field, buff, i, &my_charset_utf16_bin);

*/ /** UTF-32 string */ /*
case TILEDB_STRING_UTF32:
return set_string_field<uint32_t>(field, buff, i, &my_charset_utf32_bin);

*/ /** UCS2 string */   /*
case TILEDB_STRING_UCS2:
    return set_string_field<uint16_t>(field, buff, i, &my_charset_ucs2_bin);
  
    */
    /** UCS4 string */  /*
case TILEDB_STRING_UCS4:
  return set_string_field<uint32_t>(field, buff, i, &my_charset_utf32_bin);*/

    /** This can be any datatype. Must store (type tag, value) pairs. */
    // case TILEDB_ANY:
    //    return set_string_field<uint8_t>(field, buff, i,
    //    &my_charset_utf8_bin);

  case tiledb_datatype_t::TILEDB_DATETIME_YEAR: {

    return set_buffer_from_field<int64_t>(field->val_int(), buff, i);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_MONTH: {
    MYSQL_TIME mysql_time, diff_time;
    field->get_date(&mysql_time, date_mode_t(0));

    calc_time_diff(&epoch, &mysql_time, 1, &diff_time, date_mode_t(0));
    adjust_time_range_with_warn(thd, &diff_time, TIME_SECOND_PART_DIGITS);

    return set_buffer_from_field<int64_t>(diff_time.year * 12 + diff_time.month,
                                          buff, i);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_WEEK: {
    MYSQL_TIME mysql_time, diff_time;
    field->get_date(&mysql_time, date_mode_t(0));

    calc_time_diff(&epoch, &mysql_time, 1, &diff_time, date_mode_t(0));
    adjust_time_range_with_warn(thd, &diff_time, TIME_SECOND_PART_DIGITS);

    uint64_t daynr = calc_daynr(diff_time.year, diff_time.month, diff_time.day);
    return set_buffer_from_field<int64_t>(diff_time.year * 52 + daynr / 7, buff,
                                          i);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_DAY: {
    MYSQL_TIME mysql_time;
    field->get_date(&mysql_time, date_mode_t(0));

    ulonglong second = 0;
    ulong microsecond = 0;
    calc_time_diff(&epoch, &mysql_time, 1, &second, &microsecond);

    return set_buffer_from_field<int64_t>(second / (60 * 60 * 24), buff, i);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_HR: {
    MYSQL_TIME mysql_time;
    field->get_date(&mysql_time, date_mode_t(0));

    ulonglong second = 0;
    ulong microsecond = 0;
    calc_time_diff(&epoch, &mysql_time, 1, &second, &microsecond);

    return set_buffer_from_field<int64_t>(second / (3600), buff, i);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_MIN: {
    MYSQL_TIME mysql_time;
    field->get_date(&mysql_time, date_mode_t(0));

    ulonglong second = 0;
    ulong microsecond = 0;
    calc_time_diff(&epoch, &mysql_time, 1, &second, &microsecond);

    return set_buffer_from_field<int64_t>(second / 60, buff, i);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_SEC: {
    MYSQL_TIME mysql_time;
    field->get_date(&mysql_time, date_mode_t(0));

    ulonglong second = 0;
    ulong microsecond = 0;
    calc_time_diff(&epoch, &mysql_time, 1, &second, &microsecond);

    return set_buffer_from_field<int64_t>(second, buff, i);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_MS: {
    MYSQL_TIME mysql_time;
    field->get_date(&mysql_time, date_mode_t(0));

    ulonglong second = 0;
    ulong microsecond = 0;
    calc_time_diff(&epoch, &mysql_time, 1, &second, &microsecond);

    return set_buffer_from_field<int64_t>(second * 1000 + (microsecond / 1000),
                                          buff, i);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_US: {
    MYSQL_TIME mysql_time;
    field->get_date(&mysql_time, date_mode_t(0));

    ulonglong second = 0;
    ulong microsecond = 0;
    calc_time_diff(&epoch, &mysql_time, 1, &second, &microsecond);

    return set_buffer_from_field<int64_t>(second * 1000000 + microsecond, buff,
                                          i);
  }
  case tiledb_datatype_t::TILEDB_DATETIME_NS:
  case tiledb_datatype_t::TILEDB_DATETIME_PS:
  case tiledb_datatype_t::TILEDB_DATETIME_FS:
  case tiledb_datatype_t::TILEDB_DATETIME_AS: {
    my_printf_error(
        ER_UNKNOWN_ERROR,
        "Unsupported datetime for MariaDB. NS/PS/FS/AS not supported",
        ME_ERROR_LOG | ME_FATAL);
    break;
  }
  default: {
    const char *type_str = nullptr;
    tiledb_datatype_to_str(buff->type, &type_str);
    my_printf_error(
        ER_UNKNOWN_ERROR,
        "Unknown or unsupported datatype for converting to MariaDB fields: %s",
        ME_ERROR_LOG | ME_FATAL, type_str);
    if (type_str != nullptr) {
      delete type_str;
    }
    break;
  }
  }
  return 0;
}
