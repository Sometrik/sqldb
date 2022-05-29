#include <MemoryTable.h>
#include <Cursor.h>

#include <cassert>
#include <iostream>
#include <mutex>

using namespace std;
using namespace sqldb;

namespace sqldb {
  class MemoryTableCursor;
};

class sqldb::MemoryStorage {
public:
  friend class sqldb::MemoryTableCursor;
  
  MemoryStorage() { }

  std::unique_ptr<Cursor> seek(const std::string & key);
  std::unique_ptr<Cursor> seekBegin();
  std::unique_ptr<Cursor> addRow(const std::string & key);
  
  std::unique_ptr<Cursor> addRow() {
    int id;
    {
      std::lock_guard<std::mutex> guard(mutex_);
      id = ++auto_increment_;
    }
    return addRow(std::to_string(id));
  }

  std::unique_ptr<Cursor> incrementRow(const std::string & key);

  void removeRow(const std::string & key);

  void addColumn(std::string name, sqldb::ColumnType type, bool unique = false) {
    std::lock_guard<std::mutex> guard(mutex_);
    header_row_.push_back(std::tuple(type, std::move(name), unique));
  }
  
  int getNumFields() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return static_cast<int>(header_row_.size());
  }
  int getNumRows() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return static_cast<int>(data_.size());
  }

  ColumnType getColumnType(int column_index) const {
    std::lock_guard<std::mutex> guard(mutex_);
    auto idx = static_cast<size_t>(column_index);
    return idx < header_row_.size() ? std::get<0>(header_row_[idx]) : ColumnType::UNDEF;
  }

  std::string getColumnName(int column_index) const {
    std::lock_guard<std::mutex> guard(mutex_);
    auto idx = static_cast<size_t>(column_index);
    return idx < header_row_.size() ? std::get<1>(header_row_[idx]) : "";
  }

  bool isColumnUnique(int column_index) const {
    std::lock_guard<std::mutex> guard(mutex_);
    auto idx = static_cast<size_t>(column_index);
    return idx < header_row_.size() ? std::get<2>(header_row_[idx]) : false;
  }

  void clear() {
    std::lock_guard<std::mutex> guard(mutex_);
    data_.clear();    
  }

private:
  std::unordered_map<std::string, std::vector<std::string> > data_;
  std::vector<std::tuple<ColumnType, std::string, bool> > header_row_;
  long long auto_increment_ = 0;
  mutable std::mutex mutex_;
};

class sqldb::MemoryTableCursor : public Cursor {
public:
  MemoryTableCursor(MemoryStorage * storage,
		    std::unordered_map<std::string, std::vector<std::string> >::iterator it,
		    bool is_increment_op = false)
    : storage_(storage), it_(it), is_increment_op_(is_increment_op) { }

  size_t execute() override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);
    auto & data = storage_->data_;
    if (it_ != data.end()) {
      if (is_increment_op_ && !it_->second.empty()) {
	for (size_t i = 0; i < it_->second.size(); i++) {
	  auto & v0 = it_->second[i];
	  if (v0.empty()) {
	    v0 = pending_row_[i];
	  } else if (getColumnType(i) != sqldb::ColumnType::FOREIGN_KEY && getColumnType(i) != sqldb::ColumnType::TEXT &&
		     getColumnType(i) != sqldb::ColumnType::VARCHAR) {
	    v0 = std::to_string(stoi(v0) + stoi(pending_row_[i]));
	  }
	}
      } else {
	it_->second = pending_row_;
      }
      pending_row_.clear();
      return 1;
    } else {
      return 0;
    }
  }
    
  void set(int column_idx, const std::string & value, bool is_defined = true) override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);

    while (column_idx >= static_cast<int>(pending_row_.size())) pending_row_.push_back("");
    pending_row_[column_idx] = is_defined ? value : "";
  }

  void set(int column_idx, int value, bool is_defined = true) override { set(column_idx, std::to_string(value), is_defined); }
  void set(int column_idx, long long value, bool is_defined = true) override { set(column_idx, std::to_string(value), is_defined); }
  void set(int column_idx, double value, bool is_defined = true) override { set(column_idx, std::to_string(value), is_defined); }
  void set(int column_idx, const void * data, size_t len, bool is_defined = true) { set(column_idx, std::string(reinterpret_cast<const char *>(data), len), is_defined); }

  bool next() override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);

    auto & data = storage_->data_;
    if (it_ != data.end()) {
      return ++it_ != data.end();
    } else {
      return false;
    }
  }

  std::string getRowKey() const override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);

    auto & data = storage_->data_;
    if (it_ != data.end()) return it_->first;
    else return "";
  }

  std::string getText(int column_index, std::string default_value) override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);

    auto & data = storage_->data_;
    if (column_index >= 0 && it_ != data.end()) {
      auto idx = static_cast<size_t>(column_index);
      auto & row = it_->second;
      if (idx < row.size()) return row[idx];
    }
    return std::move(default_value);    
  }
    
  std::vector<uint8_t> getBlob(int column_index) override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);

    auto & data = storage_->data_;
    std::vector<uint8_t> r;
    if (column_index >= 0 && it_ != data.end()) {
      auto idx = static_cast<size_t>(column_index);
      auto & row = it_->second;
      if (idx < row.size()) {
	auto & v = row[idx];
	r.reserve(v.size());
	for (size_t i = 0; i < v.size(); i++) r.push_back(static_cast<uint8_t>(v[i]));
      }
    }
    return r;
  }
    
  int getNumFields() const override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);

    auto & header_row = storage_->header_row_;
    return static_cast<int>(header_row.size());
  }

  ColumnType getColumnType(int column_index) const override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);

    auto & header_row = storage_->header_row_;
    auto idx = static_cast<size_t>(column_index);
    return idx < header_row.size() ? std::get<0>(header_row[idx]) : ColumnType::UNDEF;
  }
    
  std::string getColumnName(int column_index) const override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);

    auto & header_row = storage_->header_row_;
    auto idx = static_cast<size_t>(column_index);
    return idx < header_row.size() ? std::get<1>(header_row[idx]) : "";
  }
    
  bool isNull(int column_index) const override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);

    auto & data = storage_->data_;
    if (column_index >= 0 && it_ != data.end()) {
      auto idx = static_cast<size_t>(column_index);
      auto & row = it_->second;
      if (idx < row.size()) return row[idx].empty();
    }
    return true;
  }

  long long getLastInsertId() const override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);

    return storage_->auto_increment_;
  }

private:
  MemoryStorage * storage_;
  std::unordered_map<std::string, std::vector<std::string> >::iterator it_;
  bool is_increment_op_;
  
  std::vector<std::string> pending_row_;
};

std::unique_ptr<Cursor>
MemoryStorage::seek(const std::string & key) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = data_.find(key);
  if (it != data_.end()) {
    return std::make_unique<MemoryTableCursor>(this, move(it));
  } else {
    return std::unique_ptr<Cursor>(nullptr);
  }
}

std::unique_ptr<Cursor>
MemoryStorage::seekBegin() {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = data_.begin();
  if (it != data_.end()) {
    return std::make_unique<MemoryTableCursor>(this, move(it));
  } else {
    return std::unique_ptr<Cursor>(nullptr);
  }
}

std::unique_ptr<Cursor>
MemoryStorage::addRow(const std::string & key) {
  std::lock_guard<std::mutex> guard(mutex_);
  assert(!key.empty());
  auto it = data_.find(key);
  if (it == data_.end()) {
    data_[key] = std::vector<std::string>();
    it = data_.find(key);
  }
  
  assert(it != data_.end());
  return std::make_unique<MemoryTableCursor>(this, move(it));
}

std::unique_ptr<Cursor>
MemoryStorage::incrementRow(const std::string & key) {
  std::lock_guard<std::mutex> guard(mutex_);
  
  assert(!key.empty());
  auto it = data_.find(key);
  if (it == data_.end()) {
    data_[key] = std::vector<std::string>();
    it = data_.find(key);
  }
  
  assert(it != data_.end());
  return std::make_unique<MemoryTableCursor>(this, move(it), true);
}

void
MemoryStorage::removeRow(const std::string & key) {
  data_.erase(key);
}

MemoryTable::MemoryTable(bool numeric_key)
  : numeric_key_(numeric_key), storage_(make_shared<MemoryStorage>()) { }

void
MemoryTable::addColumn(std::string name, sqldb::ColumnType type, bool unique) {
  storage_->addColumn(name, type, unique);
}

std::unique_ptr<Cursor>
MemoryTable::addRow(const std::string & key) {
  return storage_->addRow(key);
}

std::unique_ptr<Cursor>
MemoryTable::addRow() {
  return storage_->addRow();
}

std::unique_ptr<Cursor>
MemoryTable::incrementRow(const std::string & key) {
  return storage_->incrementRow(key);
}

void
MemoryTable::removeRow(const std::string & key) {
  return storage_->removeRow(key);
}

std::unique_ptr<Cursor>
MemoryTable::seekBegin() {
  return storage_->seekBegin(); 
}

std::unique_ptr<Cursor>
MemoryTable::seek(const std::string & key) {
  return storage_->seek(key);
}

int
MemoryTable::getNumFields() const {
  return storage_->getNumFields();
}

int
MemoryTable::getNumRows() const {
  return storage_->getNumRows();
}

ColumnType
MemoryTable::getColumnType(int column_index) const {
  return storage_->getColumnType(column_index);
}

std::string
MemoryTable::getColumnName(int column_index) const {
  return storage_->getColumnName(column_index);
}

bool
MemoryTable::isColumnUnique(int column_index) const {
  return storage_->isColumnUnique(column_index);
}

void
MemoryTable::append(Table & other) {
  if (!getNumFields()) { // FIXME 
    for (int i = 0; i < other.getNumFields(); i++) {
      addColumn(other.getColumnName(i), other.getColumnType(i), other.isColumnUnique(i));
    }
  }
  
  if (auto cursor = other.seekBegin()) {
    do {
      auto my_cursor = addRow(cursor->getRowKey());	  
      for (int i = 0; i < cursor->getNumFields(); i++) {
	my_cursor->set(i, cursor->getText(i));
      }
      my_cursor->execute();
    } while (cursor->next());
  }
}

void
MemoryTable::clear() {
  storage_->clear();
}
