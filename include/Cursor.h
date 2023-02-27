#ifndef _SQLDB_CURSOR_H_
#define _SQLDB_CURSOR_H_

#include "DataStream.h"

#include <charconv>

namespace sqldb {
  class Cursor : public DataStream {
  public:
    Cursor() { }

    virtual size_t update(const Key & key) = 0;

    const Key & getRowKey() const { return row_key_; }

  protected:
    void setRowKey(sqldb::Key key) { row_key_ = std::move(key); }
		   
  private:
    sqldb::Key row_key_;
  };
};

#endif
