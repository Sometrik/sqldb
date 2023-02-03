#ifndef _LOG_H_
#define _LOG_H_

#include "Key.h"

#include <vector>

namespace sqldb {
  class Log {
  public:
    enum class Event { ADD = 1, REMOVE };
    
    Log() { }

    void add(sqldb::Key key) { data_.emplace_back(Event::ADD, key); }
    void remove(sqldb::Key key) { data_.emplace_back(Event::REMOVE, key); }
    size_t size() const { return data_.size(); }

    const std::pair<Event, sqldb::Key> & getEvent(int row) const {
      if (row >= 0 && row < static_cast<int>(data_.size())) {
	return data_[row];
      } else {
	return null_row;
      }
    }
    
  private:
    std::vector<std::pair<Event, sqldb::Key>> data_;

    static inline std::pair<Event, sqldb::Key> null_row;
  };
};

#endif
