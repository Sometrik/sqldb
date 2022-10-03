#ifndef _SQLDB_COLUMNTYPE_H_
#define _SQLDB_COLUMNTYPE_H_

namespace sqldb {
  enum class ColumnType {
    UNDEF = 0,
      INT,
      INT64,
      CHAR,
      BOOL,
      VARCHAR, // single line of text
      TEXT, // multiple lines of text
      DATETIME,
      DOUBLE,
      URL,
      FOREIGN_KEY,
      ENUM,
      IMAGE // image (blob)
      };

  static inline bool is_numeric(ColumnType type) {
    switch (type) {
    case ColumnType::INT:
    case ColumnType::INT64:
    case ColumnType::BOOL:
    case ColumnType::DATETIME:
    case ColumnType::DOUBLE:
      return true;
    case ColumnType::UNDEF:
    case ColumnType::CHAR:
    case ColumnType::VARCHAR:
    case ColumnType::TEXT:
    case ColumnType::URL:
    case ColumnType::FOREIGN_KEY:
    case ColumnType::ENUM:
    case ColumnType::IMAGE:
      break;
    }
    return false;
  }
};

#endif

