#ifndef _SQLDB_KEY_H_
#define _SQLDB_KEY_H_

#include "ColumnType.h"

#include <string>
#include <vector>
#include <variant>

#include "robin_hood.h"

namespace sqldb {
  static size_t hash_combine(size_t seed, size_t v) noexcept {
    // see https://www.boost.org/doc/libs/1_55_0/doc/html/hash/reference.html#boost.hash_combine
    seed ^= v + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    return seed;
  }
  
  class Key {
  public:
    Key() noexcept { }
    explicit Key(int value) noexcept {
      addComponent(value);
    }
    explicit Key(std::string value) noexcept {
      if (!value.empty()) addComponent(std::move(value));
    }
    explicit Key(std::string value1, std::string value2) noexcept {
      addComponent(std::move(value1));
      addComponent(std::move(value2));
    }
    explicit Key(Key value1, Key value2, Key value3) noexcept {
      addComponent(std::move(value1));
      addComponent(std::move(value2));
      addComponent(std::move(value3));
    }
    explicit Key(int value1, int value2, int value3, int value4) noexcept {
      addComponent(value1);
      addComponent(value2);
      addComponent(value3);
      addComponent(value4);
    }

    Key & operator=(int value) {
      clear();
      addComponent(value);
      return *this;
    }

    bool operator < (const Key & other) const {
      for (size_t pos = 0; pos < components_.size() && pos < other.components_.size(); pos++) {
	if (components_[pos] < other.components_[pos]) return true;
      }
      if (components_.size() < other.components_.size()) return true;
      else return false;      
    }

    bool operator == (const Key & other) const {
      return components_ == other.components_;
    }
    bool operator != (const Key & other) const {
      return components_ != other.components_;
    }

    void clear() { components_.clear(); }
    bool empty() const { return components_.empty(); }
    size_t size() const { return components_.size(); }

    void addComponent(int value) noexcept {
      components_.push_back(value);
    }

    void addComponent(long long value) noexcept {
      components_.push_back(value);
    }

    void addComponent(std::string value) noexcept {
      components_.push_back(std::move(value));
    }

    void addComponent(std::string_view value) noexcept {
      components_.push_back(std::string(value));
    }

    void addComponent(const Key & other) {
      if (other.getType(0) == ColumnType::INT64) {
	addComponent(other.getLongLong(0));
      } else {
	addComponent(other.getText(0));
      }
    }

    ColumnType getType(size_t idx) const {
      if (idx < components_.size()) {
	auto & component = components_[idx];
	if (std::holds_alternative<long long>(component)) {
	  return ColumnType::INT64;
	} else if (std::holds_alternative<std::string>(component)) {
	  return ColumnType::VARCHAR;
	}
      }
      return ColumnType::UNDEF;
    }
    
    int getInt(size_t idx) const {
      return getLongLong(idx);
    }
    
    long long getLongLong(size_t idx) const {
      if (idx < components_.size()) {
	if (std::holds_alternative<long long>(components_[idx])) {
	  return std::get<long long>(components_[idx]);
	} else {
	  return std::stoll(std::get<std::string>(components_[idx]));
	}
      }
      return 0LL;
    }

    // return by value, since Key is temporary when returned from Cursor
    std::string getText(size_t idx) const {
      if (idx < components_.size()) {
	if (std::holds_alternative<std::string>(components_[idx])) {
	  return std::get<std::string>(components_[idx]);
	} else {
	  return std::to_string(std::get<long long>(components_[idx]));
	}
      }
      return "";
    }

    size_t getHash() const noexcept {
      size_t seed = 0;
      for (auto & c : components_) {
	if (std::holds_alternative<long long>(c)) {
	  seed = hash_combine(seed, robin_hood::hash<long long>{}(std::get<long long>(c)));
	} else if (std::holds_alternative<std::string>(c)) {
	  seed = hash_combine(seed, robin_hood::hash<std::string>{}(std::get<std::string>(c)));
	} else {
	  seed = hash_combine(seed, robin_hood::hash<long long>{}(0));
	}
      }
      return seed;
    }

    std::string serialize() const {
      return getText(0); // FIXME: serialize all the parts to binary
    }

  private:    
    std::vector<std::variant<long long, std::string>> components_;
  };

  static inline std::string to_string(const Key & key) {
    std::string s;
    for (size_t i = 0; i < key.size(); i++) {
      if (!s.empty()) s += "|";
      if (key.getType(i) == ColumnType::VARCHAR) {
	s += "s=" + key.getText(i);
      } else {
	s += "i=" + std::to_string(key.getLongLong(i));
      }
    }
    return s;
  }
};

namespace robin_hood {
  template<>
  struct hash<sqldb::Key> {
    size_t operator()(sqldb::Key const & key) const noexcept {
      return key.getHash();      
    }
  };
}

namespace std {
  template <>
  struct hash<sqldb::Key> {
    std::size_t operator()(const sqldb::Key & key) const {
      return key.getHash();      
    }
  };
}

#endif
