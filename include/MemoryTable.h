#ifndef _SQLDB_MEMORYTABLE_H_
#define _SQLDB_MEMORYTABLE_H_

#include "Cursor.h"

#include <vector>
#include <unordered_map>

namespace sqldb {
  class MemoryTableCursor : public Cursor {
  public:
    MemoryTableCursor(std::unordered_map<std::string, std::vector<std::string> > * data,
		      std::vector<std::pair<ColumnType, std::string> > * header_row,
		      std::unordered_map<std::string, std::vector<std::string> >::iterator it)
      : data_(data), header_row_(header_row), it_(it) { }
    
    void set(int column_idx, std::string value) override {
      if (it_ != data_->end()) {
	auto & row = it_->second;
	while (column_idx >= static_cast<int>(row.size())) row.push_back("");
	row[column_idx] = std::move(value);
      }
    }
    
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
    
    ustring getBlob(int column_index) override {
      return ustring();
    }
    
    int getNumFields() const override { return static_cast<int>(header_row_->size()); }

    ColumnType getColumnType(int column_index) const override {
      auto idx = static_cast<size_t>(column_index);
      return idx < header_row_->size() ? (*header_row_)[idx].first : ColumnType::UNDEF;
    }

    std::string getColumnName(int column_index) const {
      auto idx = static_cast<size_t>(column_index);
      return idx < header_row_->size() ? (*header_row_)[idx].second : "";
    }
    
    bool isNull(int column_index) const override {
      if (column_index >= 0 && it_ != data_->end()) {
	auto idx = static_cast<size_t>(column_index);
	auto & row = it_->second;
	if (idx < row.size()) return row[idx].empty();
      }
      return true;
    }

  private:
    std::unordered_map<std::string, std::vector<std::string> > * data_;
    std::vector<std::pair<ColumnType, std::string> > * header_row_;
    std::unordered_map<std::string, std::vector<std::string> >::iterator it_;
  };
  
  class MemoryTable : public Table {
  public:
    MemoryTable() { }
    
    std::unique_ptr<Table> copy() const override { return std::make_unique<MemoryTable>(*this); }

    void addColumn(std::string name, sqldb::ColumnType type) override {
      header_row_.push_back(std::pair(type, std::move(name)));
    }
    
    std::unique_ptr<Cursor> addRow(const std::string & key) {
      auto it = data_.find(key);
      if (it == data_.end()) {
	data_[key] = std::vector<std::string>();
	it = data_.find(key);
      }
      return std::make_unique<MemoryTableCursor>(&data_, &header_row_, it);
    }
            
    std::unique_ptr<Cursor> seekBegin() override {
      return std::make_unique<MemoryTableCursor>(&data_, &header_row_, data_.begin());
    }
    
    std::unique_ptr<Cursor> seek(const std::string & key) override {
      return std::make_unique<MemoryTableCursor>(&data_, &header_row_, data_.find(key));
    }

    int getNumFields() const override { return static_cast<int>(header_row_.size()); }
    int getNumRows() const { return static_cast<int>(data_.size()); }
    ColumnType getColumnType(int column_index) const override {
      auto idx = static_cast<size_t>(column_index);
      return idx < header_row_.size() ? header_row_[idx].first : ColumnType::UNDEF;
    }

    std::string getColumnName(int column_index) const {
      auto idx = static_cast<size_t>(column_index);
      return idx < header_row_.size() ? header_row_[idx].second : "";
    }

    void append(Table & other) override {
      // FIXME
      header_row_.clear();
      for (int i = 0; i < other.getNumFields(); i++) {
	header_row_.push_back(std::pair(other.getColumnType(i), other.getColumnName(i)));
      }

      if (auto cursor = other.seekBegin()) {
	do {
	  auto my_cursor = addRow(cursor->getRowKey());	  
	  for (int i = 0; i < cursor->getNumFields(); i++) {
	    my_cursor->set(i, cursor->getText(i));
	  }
	} while (cursor->next());
      }
    }
          
  private:
    std::unordered_map<std::string, std::vector<std::string> > data_;
    std::vector<std::pair<ColumnType, std::string> > header_row_;
  };
};

#endif
