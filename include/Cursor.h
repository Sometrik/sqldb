#ifndef _SQLDB_CURSOR_H_
#define _SQLDB_CURSOR_H_

#include "DataStream.h"

namespace sqldb {
  class Cursor : public DataStream {
  public:
    Cursor() { }

    virtual std::string getRowKey() const = 0;
  };
};

#endif
