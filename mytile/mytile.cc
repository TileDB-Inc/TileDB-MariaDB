/*
** Licensed under the GNU Lesser General Public License v3 or later
*/
#include "mytile.h"
#include <log.h>
#include <my_global.h>

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
    return tiledb_datatype_t::TILEDB_UINT16;
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
    return "TINY";
  case MYSQL_TYPE_SHORT:
    return "SHORT";
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

  case tiledb_datatype_t::TILEDB_DATETIME_NS:
    return MYSQL_TYPE_TIMESTAMP2;

  default: {
    sql_print_error("Unknown tiledb data type in determining mysql type");
  }
    return tiledb_datatype_t::TILEDB_ANY;
  }
}

tiledb::Attribute
tile::create_field_attribute(tiledb::Context &ctx, Field *field,
                             const tiledb::FilterList &filterList) {

  std::cout << field->field_name.str << " - " << field->type() << std::endl;
  tiledb_datatype_t datatype =
      tile::mysqlTypeToTileDBType(field->type(), false);
  return tiledb::Attribute(ctx, field->field_name.str, datatype);
}

tiledb::Dimension tile::create_field_dimension(tiledb::Context &ctx,
                                               Field *field) {
  switch (field->type()) {

  case MYSQL_TYPE_DOUBLE:
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_NEWDECIMAL: {
    return create_dim<double>(ctx, field);
  }

  case MYSQL_TYPE_FLOAT: {
    return create_dim<float>(ctx, field);
  }

  case MYSQL_TYPE_TINY: {
    if (((Field_num *)field)->unsigned_flag) {
      return create_dim<uint8_t>(ctx, field);
    }
    // signed
    return create_dim<int8_t>(ctx, field);
  }

  case MYSQL_TYPE_SHORT: {
    if (((Field_num *)field)->unsigned_flag) {
      return create_dim<uint16_t>(ctx, field);
    }
    // signed
    return create_dim<int16_t>(ctx, field);
  }
  case MYSQL_TYPE_YEAR: {
    return create_dim<uint16_t>(ctx, field);
  }

  case MYSQL_TYPE_INT24: {
    if (((Field_num *)field)->unsigned_flag) {
      return create_dim<uint32_t>(ctx, field);
    }
    // signed
    return create_dim<int32_t>(ctx, field);
  }

  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_LONGLONG: {
    if (((Field_num *)field)->unsigned_flag) {
      return create_dim<uint64_t>(ctx, field);
    }
    // signed
    return create_dim<int64_t>(ctx, field);
  }

  case MYSQL_TYPE_NULL:
  case MYSQL_TYPE_BIT: {
    return create_dim<uint8_t>(ctx, field);
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
    return create_dim<uint8_t>(ctx, field);
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
