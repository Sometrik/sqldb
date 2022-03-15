#ifndef _SQLDB_COLUMNTYPE_H_
#define _SQLDB_COLUMNTYPE_H_

namespace sqldb {
  enum class ColumnType {
    UNDEF = 0,
      INT,
      INT64,
      CHAR,
      TEXT,
      DATETIME,
      DOUBLE,
      URL,
      FOREIGN_KEY
      };
};

#endif

