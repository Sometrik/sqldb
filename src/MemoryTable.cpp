#include <MemoryTable.h>
#include <Cursor.h>

#include <cassert>
#include <iostream>

using namespace std;
using namespace sqldb;

class MemoryTableCursor : public Cursor {
public:
  MemoryTableCursor(std::unordered_map<std::string, std::vector<std::string> > * data,
		    std::vector<std::tuple<ColumnType, std::string, bool> > * header_row,
		    long long * auto_increment,
		    std::unordered_map<std::string, std::vector<std::string> >::iterator it)
    : data_(data), header_row_(header_row), auto_increment_(auto_increment), it_(it) { }

  size_t execute() override {
    if (it_ != data_->end()) {
      it_->second = pending_row_;
      pending_row_.clear();
      return 1;
    } else {
      return 0;
    }
  }
    
  void set(int column_idx, const std::string & value, bool is_defined = true) override {
    while (column_idx >= static_cast<int>(pending_row_.size())) pending_row_.push_back("");
    pending_row_[column_idx] = is_defined ? value : "";
  }

  void set(int column_idx, int value, bool is_defined = true) override { set(column_idx, std::to_string(value), is_defined); }
  void set(int column_idx, long long value, bool is_defined = true) override { set(column_idx, std::to_string(value), is_defined); }
  void set(int column_idx, double value, bool is_defined = true) override { set(column_idx, std::to_string(value), is_defined); }
  void set(int column_idx, const void * data, size_t len, bool is_defined = true) { set(column_idx, std::string(reinterpret_cast<const char *>(data), len), is_defined); }

  bool next() override {
    if (it_ != data_->end()) {
      return ++it_ != data_->end();
    } else {
      return false;
    }
  }

  std::string getRowKey() const override {
    if (it_ != data_->end()) return it_->first;
    else return "";
  }

  std::string getText(int column_index, std::string default_value) override {
    if (column_index >= 0 && it_ != data_->end()) {
      auto idx = static_cast<size_t>(column_index);
      auto & row = it_->second;
      if (idx < row.size()) return row[idx];
    }
    return std::move(default_value);    
  }
    
  std::vector<uint8_t> getBlob(int column_index) override {
    std::vector<uint8_t> r;
    if (column_index >= 0 && it_ != data_->end()) {
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
    
  int getNumFields() const override { return static_cast<int>(header_row_->size()); }

  ColumnType getColumnType(int column_index) const override {
    auto idx = static_cast<size_t>(column_index);
    return idx < header_row_->size() ? std::get<0>((*header_row_)[idx]) : ColumnType::UNDEF;
  }
    
  std::string getColumnName(int column_index) const override {
    auto idx = static_cast<size_t>(column_index);
    return idx < header_row_->size() ? std::get<1>((*header_row_)[idx]) : "";
  }
    
  bool isNull(int column_index) const override {
    if (column_index >= 0 && it_ != data_->end()) {
      auto idx = static_cast<size_t>(column_index);
      auto & row = it_->second;
      if (idx < row.size()) return row[idx].empty();
    }
    return true;
  }

  long long getLastInsertId() const override { return *auto_increment_; }
    
private:
  std::unordered_map<std::string, std::vector<std::string> > * data_;
  std::vector<std::tuple<ColumnType, std::string, bool> > * header_row_;
  long long * auto_increment_;
  std::unordered_map<std::string, std::vector<std::string> >::iterator it_;
    
  std::vector<std::string> pending_row_;
};

std::unique_ptr<Cursor>
MemoryTable::addRow(const std::string & key) {  
  assert(!key.empty());
  auto it = data_.find(key);
  if (it == data_.end()) {
    data_[key] = std::vector<std::string>();
    it = data_.find(key);
  }
  cerr << "addRow: " << key << " (size = " << data_.size() << ")\n";

  assert(it != data_.end());
  return std::make_unique<MemoryTableCursor>(&data_, &header_row_, &auto_increment_, move(it));
}

std::unique_ptr<Cursor>
MemoryTable::seekBegin() {
  auto it = data_.begin();
  if (it != data_.end()) {
    return std::make_unique<MemoryTableCursor>(&data_, &header_row_, &auto_increment_, move(it));
  } else {
    return std::unique_ptr<Cursor>(nullptr);
  }
}

std::unique_ptr<Cursor>
MemoryTable::seek(const std::string & key) {
  auto it = data_.find(key);
  if (it != data_.end()) {
    return std::make_unique<MemoryTableCursor>(&data_, &header_row_, &auto_increment_, move(it));
  } else {
    return std::unique_ptr<Cursor>(nullptr);
  }
}

void
MemoryTable::append(Table & other) {
  // FIXME
  header_row_.clear();
  for (int i = 0; i < other.getNumFields(); i++) {
    header_row_.push_back(std::tuple(other.getColumnType(i), other.getColumnName(i), other.isColumnUnique(i)));
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
