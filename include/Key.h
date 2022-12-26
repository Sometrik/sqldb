#ifndef _SQLDB_KEY_H_
#define _SQLDB_KEY_H_

#include <iomanip>
#include <sstream>

namespace sqldb {
  class Key {
  public:
    explicit Key() { }
    explicit Key(std::string value) : value_(std::move(value)) { }
    explicit Key(const char * value) : value_(value) { }
    explicit Key(std::string value1, std::string value2) {
      value_ = std::move(value1);
      value_ += '|';
      value_ += value2;
    }
    explicit Key(std::string value1, std::string value2, std::string value3) {
      value_ = std::move(value1);
      value_ += '|';
      value_ += value2;
      value_ += '|';
      value_ += value3;
    }

    bool operator < (const Key & other) const {
      return value_.compare(other.value_) == -1;
    }
    
    const std::string & getValue() const { return value_; }

    void clear() { value_.clear(); }
    bool empty() const { return value_.empty(); }
    size_t size() const { return value_.empty() ? 0 : 1; }

    std::vector<std::string> getComponents() const {
      std::vector<std::string> r;
      size_t pos0 = 0;
  
      while (pos0 < value_.size()) {
	auto pos1 = value_.find_first_of('|', pos0);
	if (pos1 == std::string::npos) pos1 = value_.size();
	r.push_back(value_.substr(pos0, pos1 - pos0));
	pos0 = pos1 + 1;
      }
  
      return r;
    }

    void formatHex(int value) {
      std::stringstream stream;
      stream << std::setfill('0') << std::setw(8) << std::hex << value;
      setValue(stream.str());
    }

    void formatDec(int value) { value_ = std::to_string(value); }

    void setValue(std::string value) {
      value_ = std::move(value);
    }
    
  private:
    std::string value_;
  };
};

#endif
