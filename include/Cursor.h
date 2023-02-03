#ifndef _SQLDB_CURSOR_H_
#define _SQLDB_CURSOR_H_

#include "DataStream.h"

#include <charconv>

namespace sqldb {
  class Cursor : public DataStream {
  public:
    Cursor() { }

    virtual Key getRowKey() const = 0;
    virtual size_t update(const Key & key) = 0;
  };
};

#endif
