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
      FOREIGN_KEY
      };
};

#endif

