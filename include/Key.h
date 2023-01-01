#ifndef _SQLDB_KEY_H_
#define _SQLDB_KEY_H_

#include "ColumnType.h"

#include <vector>

namespace sqldb {
  template <class T>
  inline void hash_combine(std::size_t & seed, const T & v) noexcept {
    std::hash<T> hasher;
    seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  }

  class Key {
  public:
    Key() noexcept { }
    explicit Key(int value) noexcept {
      addIntComponent(value);
    }
    explicit Key(std::string value) noexcept {
      addTextComponent(std::move(value));
    }
    explicit Key(std::string value1, std::string value2) noexcept {
      addTextComponent(std::move(value1));
      addTextComponent(std::move(value2));
    }
    explicit Key(std::string value1, std::string value2, std::string value3) noexcept {
      addTextComponent(std::move(value1));
      addTextComponent(std::move(value2));
      addTextComponent(std::move(value3));
    }
    explicit Key(int value1, int value2, int value3, int value4) noexcept {
      addIntComponent(value1);
      addIntComponent(value2);
      addIntComponent(value3);
      addIntComponent(value4);
    }

    bool operator < (const Key & other) const {
      size_t pos = 0;
      while (pos < components_.size() && pos < other.components_.size()) {
	switch (types_[pos]) {
	case ColumnType::INT:
	case ColumnType::ENUM:
	case ColumnType::BOOL:
	  return stoi(components_[pos]) < stoi(other.components_[pos]);
	case ColumnType::INT64:
	case ColumnType::DATETIME:
	  return stoll(components_[pos]) < stoll(other.components_[pos]);
	default:
	  {
	    auto cmp = components_[pos].compare(other.components_[pos]);
	    if (cmp < 0) return true;
	    else if (cmp > 0) return false;
	    pos++;
	  }
	}
      }
      if (components_.size() < other.components_.size()) return true;
      else return false;      
    }

    bool operator == (const Key & other) const {
      if (components_.size() != other.components_.size()) return false;
      for (size_t i = 0; i < components_.size(); i++) {
	if (types_[i] != other.types_[i]) return false;
	if (components_[i] != other.components_[i]) return false;
      }
      return true;
    }

    bool operator != (const Key & other) const { return !(*this == other); }

    void clear() { components_.clear(); }
    bool empty() const { return components_.empty(); }
    size_t size() const { return components_.size(); }

    void addIntComponent(int value) noexcept {
      types_.push_back(ColumnType::INT);
      components_.push_back(std::to_string(value));
    }

    void addLongLongComponent(long long value) noexcept {
      types_.push_back(ColumnType::INT64);
      components_.push_back(std::to_string(value));
    }

    void addTextComponent(std::string value) noexcept {
      types_.push_back(ColumnType::VARCHAR);
      components_.push_back(std::move(value));
    }

    ColumnType getType(size_t idx) const {
      if (idx < types_.size()) {
	return types_[idx];
      } else {
	return ColumnType::UNDEF;
      }
    }
    
    int getInt(size_t idx) const {
      if (idx < components_.size()) {
	return stoi(components_[idx]);
      } else {
	return 0;
      }
    }

    long long getLongLong(size_t idx) const {
      if (idx < components_.size()) {
	return stoll(components_[idx]);
      } else {
	return 0LL;
      }
    }

    // return by value, since Key is temporary when returned from Cursor
    std::string getText(size_t idx) const {
      if (idx < components_.size()) {
	return components_[idx];
      } else {
	return "";
      }
    }

    std::size_t hash_value() const noexcept {
      std::size_t seed = 0;
      for (auto & component : components_) {
	hash_combine(seed, component);
      }
      return seed;
    }
    
  private:    
    std::vector<ColumnType> types_;
    std::vector<std::string> components_;
  };
};

namespace std {
  template <>
  struct hash<sqldb::Key> {
    std::size_t operator()(const sqldb::Key & key) const {
      return key.hash_value();      
    }
  };
}

#endif
