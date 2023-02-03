#ifndef _LOG_H_
#define _LOG_H_

#include "Key.h"

#include <vector>
#include <mutex>

namespace sqldb {
  class Log {
  public:
    enum class Event { ADD = 1, REMOVE };
    
    Log() { }

    void add(sqldb::Key key) {
      std::lock_guard<std::mutex> guard(mutex_);
      data_.emplace_back(Event::ADD, std::move(key));
    }
    void remove(sqldb::Key key) {
      std::lock_guard<std::mutex> guard(mutex_);
      data_.emplace_back(Event::REMOVE, std::move(key));
    }
    size_t size() const {
      std::lock_guard<std::mutex> guard(mutex_);
      return data_.size();
    }

    std::pair<Event, sqldb::Key> getEvent(int row) const {
      std::lock_guard<std::mutex> guard(mutex_);
      if (row >= 0 && row < static_cast<int>(data_.size())) {
	return data_[row];
      }
      return std::pair<Event, sqldb::Key>();      
    }
    
  private:
    std::vector<std::pair<Event, sqldb::Key>> data_;
    mutable std::mutex mutex_;
  };
};

#endif
