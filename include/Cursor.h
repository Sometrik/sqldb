#ifndef _CURSOR_H_
#define _CURSOR_H_

#include "DataStream.h"

#include <string>

namespace sqldb {
  class Cursor : public DataStream {
  public:
    Cursor() { }

    virtual std::string getRowKey() const = 0;

    virtual void set(int column_idx, std::string value) = 0;
    virtual void set(int column_idx, int value) { set(column_idx, std::to_string(value)); }
    virtual void set(int column_idx, long long value) { set(column_idx, std::to_string(value)); }
    virtual void set(int column_idx, float value) { set(column_idx, std::to_string(value)); }
  };
};

#endif
