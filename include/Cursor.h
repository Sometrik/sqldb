#ifndef _SQLDB_CURSOR_H_
#define _SQLDB_CURSOR_H_

#include "DataStream.h"
#include "Key.h"

#include <charconv>

namespace sqldb {
  class Cursor : public DataStream {
  public:
    Cursor() { }

    virtual Key getRowKey() const = 0;
    virtual size_t update(const Key & key) = 0;

    // accessors for dynamically typed key
    
    virtual Key getKey(int column_index) {
      auto type = getColumnType(column_index);
      if (type == ColumnType::ANY) {
	auto s = getText(column_index);
	long long ll;
	auto [ ptr, ec ] = std::from_chars(s.data(), s.data() + s.size(), ll);
	return ec == std::errc() ? Key(ll) : Key(s);
      } else if (is_numeric(type)) {
	return Key(getLongLong(column_index));
      } else {
	return Key(getText(column_index));
      }
    }

    virtual void set(int column_idx, const sqldb::Key & key) {
      if (key.empty()) {
	set(column_idx, 0, false);
      } else if (key.size() >= 2) {
	set(column_idx, key.serializeToText());
      } else if (sqldb::is_numeric(key.getType(0))) {
	set(column_idx, key.getLongLong(0));
      } else {
	set(column_idx, key.getText(0));
      }
    }

    void bind(const Key & value) {
      set(getNextBindIndex(), value);
    }
  };
};

#endif
